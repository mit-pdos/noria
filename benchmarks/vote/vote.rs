#[macro_use]
extern crate clap;

extern crate rand;

extern crate distributary;

extern crate hdrsample;
extern crate zipf;

mod exercise;
mod graph;

#[macro_use]
mod common;

use common::{Writer, Reader, ArticleResult, Period, MigrationHandle, RuntimeConfig, Distribution};
use distributary::{Mutator, DataType};

use std::sync::mpsc;
use std::thread;
use std::sync;
use std::time;

fn main() {
    use clap::{Arg, App};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(Arg::with_name("avg")
            .long("avg")
            .takes_value(false)
            .help("compute average throughput at the end of benchmark"))
        .arg(Arg::with_name("cdf")
            .short("c")
            .long("cdf")
            .takes_value(false)
            .help("produce a CDF of recorded latencies for each client at the end"))
        .arg(Arg::with_name("stage")
            .short("s")
            .long("stage")
            .takes_value(false)
            .help("stage execution such that all writes are performed before all reads"))
        .arg(Arg::with_name("distribution")
            .short("d")
            .takes_value(true)
            .default_value("uniform")
            .help("run benchmark with the given article id distribution [uniform|zipf:exponent]"))
        .arg(Arg::with_name("ngetters")
            .short("g")
            .long("getters")
            .value_name("N")
            .default_value("1")
            .help("Number of GET clients to start"))
        .arg(Arg::with_name("narticles")
            .short("a")
            .long("articles")
            .value_name("N")
            .default_value("100000")
            .help("Number of articles to prepopulate the database with"))
        .arg(Arg::with_name("runtime")
            .short("r")
            .long("runtime")
            .value_name("N")
            .default_value("60")
            .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("stupid")
             .long("stupid")
            .help("Make the migration stupid")
            .requires("migrate"))
        .arg(Arg::with_name("migrate")
            .short("m")
            .long("migrate")
            .value_name("N")
            .help("Perform a migration after this many seconds")
            .conflicts_with("stage"))
        .arg(Arg::with_name("transactions")
            .short("t")
            .long("transactions")
            .takes_value(false)
            .help("Use transactional reads and writes"))
        .arg(Arg::with_name("crossover")
            .short("x")
            .takes_value(true)
            .help("Period for transition to new views for readers and writers")
            .requires("migrate"))
        .arg(Arg::with_name("quiet")
            .short("q")
            .long("quiet")
            .help("No noisy output while running"))
        .arg(Arg::with_name("durability")
             .long("durability")
             .help("Enable durability for Base nodes"))
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let transactions = args.is_present("transactions");
    let dist = value_t_or_exit!(args, "distribution", Distribution);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let crossover = args.value_of("crossover")
        .map(|_| value_t_or_exit!(args, "crossover", u64))
        .map(time::Duration::from_secs);
    let ngetters = value_t_or_exit!(args, "ngetters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    assert!(ngetters > 0);

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = RuntimeConfig::new(narticles, common::Mix::Read(1), Some(runtime));
    config.set_verbose(!args.is_present("quiet"));
    config.produce_cdf(cdf);
    if let Some(migrate_after) = migrate_after {
        config.perform_migration_at(migrate_after);
    }
    config.use_distribution(dist);

    let durability = if args.is_present("durability") {
        Some((512, time::Duration::from_millis(10)))
    } else {
        None
    };

    // setup db
    let g = graph::make(!args.is_present("quiet"), transactions, durability);
    let putters = (g.graph.get_mutator(g.article), g.graph.get_mutator(g.vote));

    // prepare getters
    let getters: Vec<_> = (0..ngetters)
        .into_iter()
        .map(|_| Getter::new(g.graph.get_getter(g.end).unwrap(), crossover))
        .collect();

    let g = sync::Arc::new(sync::Mutex::new(g));
    let putter = Spoon {
        graph: g.clone(),
        getters: getters.iter().map(|g| unsafe { g.clone() }).collect(),
        article: putters.0,
        vote_pre: putters.1,
        vote_post: None,
        new_vote: None,
        stupid: args.is_present("stupid"),
        i: 0,
        x: Crossover::new(crossover),
        transactions: transactions,
    };

    let put_stats;
    let get_stats: Vec<_>;
    if stage {
        // put then get
        let mut pconfig = config.clone();
        pconfig.mix = common::Mix::Write(1);
        put_stats = exercise::launch(None::<exercise::NullClient>, Some(putter), pconfig, None);
        let getters: Vec<_> = getters
            .into_iter()
            .enumerate()
            .map(|(i, g)| {
                let config = config.clone();
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || {
                               exercise::launch(Some(g), None::<exercise::NullClient>, config, None)
                           })
                    .unwrap()
            })
            .collect();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
    } else {
        // put & get
        // TODO: how do we start getters after prepopulate?
        let (tx, prepop) = mpsc::sync_channel(0);
        let mut pconfig = config.clone();
        pconfig.mix = common::Mix::Write(1);
        let putter = thread::Builder::new()
            .name("PUT0".to_string())
            .spawn(move || {
                       exercise::launch(None::<exercise::NullClient>,
                                        Some(putter),
                                        pconfig,
                                        Some(tx))
                   })
            .unwrap();

        // wait for prepopulation to finish
        prepop.recv().is_err();

        let getters: Vec<_> = getters
            .into_iter()
            .enumerate()
            .map(|(i, g)| {
                let config = config.clone();
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || {
                               exercise::launch(Some(g), None::<exercise::NullClient>, config, None)
                           })
                    .unwrap()
            })
            .collect();

        put_stats = putter.join().unwrap();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
    }

    print_stats("PUT", false, &put_stats.pre, avg);
    for (i, s) in get_stats.iter().enumerate() {
        print_stats(format!("GET{}", i), true, &s.pre, avg);
    }
    if avg {
        let sum = get_stats
            .iter()
            .fold((0f64, 0usize), |(tot, count), stats| {
                // TODO: do we *really* want an average of averages?
                let (sum, num) = stats.pre.sum_len();
                (tot + sum, count + num)
            });
        println!("avg GET: {:.2}", sum.0 as f64 / sum.1 as f64);
    }

    if migrate_after.is_some() {
        print_stats("PUT+", false, &put_stats.post, avg);
        for (i, s) in get_stats.iter().enumerate() {
            print_stats(format!("GET{}+", i), true, &s.post, avg);
        }
        if avg {
            let sum = get_stats
                .iter()
                .fold((0f64, 0usize), |(tot, count), stats| {
                    // TODO: do we *really* want an average of averages?
                    let (sum, num) = stats.post.sum_len();
                    (tot + sum, count + num)
                });
            println!("avg GET+: {:.2}", sum.0 as f64 / sum.1 as f64);
        }
    }
}

fn print_stats<S: AsRef<str>>(desc: S, read: bool, stats: &exercise::BenchmarkResult, avg: bool) {
    if let Some((r_perc, w_perc)) = stats.cdf_percentiles() {
        let perc = if read { r_perc } else { w_perc };
        for iv in perc {
            println!("percentile {} {:.2} {:.2}",
                     desc.as_ref(),
                     iv.value(),
                     iv.percentile());
        }
    }
    if avg {
        println!("avg {}: {:.2}", desc.as_ref(), stats.avg_throughput());
    }
}

#[derive(Clone)]
struct Crossover {
    swapped: Option<time::Instant>,
    crossover: Option<u64>,
    done: bool,
    iteration: usize,
    post: usize,
}

impl Crossover {
    pub fn new(crossover: Option<time::Duration>) -> Self {
        Crossover {
            swapped: None,
            crossover: crossover.map(|d| dur_to_ns!(d)),
            done: false,
            iteration: 0,
            post: 0,
        }
    }

    pub fn swapped(&mut self) {
        assert!(self.swapped.is_none());
        self.swapped = Some(time::Instant::now());
        if self.crossover.is_none() {
            self.done = true;
        }
    }

    pub fn has_swapped(&self) -> bool {
        self.swapped.is_some()
    }

    pub fn use_post(&mut self) -> bool {
        if self.done {
            return true;
        }
        if self.swapped.is_none() {
            return false;
        }

        self.iteration += 1;
        if self.iteration == (1 << 12) {
            let elapsed = dur_to_ns!(self.swapped.as_ref().unwrap().elapsed());

            if elapsed > self.crossover.unwrap() {
                // we've fully crossed over
                self.done = true;
                return true;
            }

            self.post = ((elapsed as f64 / self.crossover.unwrap() as f64) * (1 << 12) as f64) as
                        usize;
            self.iteration = 0;
        }

        self.iteration < self.post
    }
}

type G = Box<Fn(&DataType, bool) -> Result<Vec<Vec<DataType>>, ()> + Send + 'static>;

// A more dangerous AtomicPtr that also derefs into the inner type
use std::sync::atomic::AtomicPtr;
struct Getter {
    real: sync::Arc<AtomicPtr<G>>,
    before: *mut G,
    after: *mut G,
    callable: bool,
    x: Crossover,
}

// *muts are Send
unsafe impl Send for Getter {}

impl Getter {
    pub fn new(inner: G, crossover: Option<time::Duration>) -> Getter {
        let m = Box::into_raw(Box::new(inner));
        Getter {
            real: sync::Arc::new(AtomicPtr::new(m)),
            before: m,
            after: m,
            callable: true,
            x: Crossover::new(crossover),
        }
    }

    // unsafe, because cannot use call() on clones
    pub unsafe fn clone(&self) -> Self {
        Getter {
            real: self.real.clone(),
            before: self.before.clone(),
            after: self.after.clone(),
            x: self.x.clone(),
            callable: false,
        }
    }

    // unsafe, because may only be called once
    pub unsafe fn replace(&mut self, new: G) {
        let m = Box::into_raw(Box::new(new));
        self.real.store(m, sync::atomic::Ordering::Release);
    }

    pub fn same(&self) -> bool {
        !self.x.has_swapped()
    }

    pub fn call(&mut self) -> &mut G {
        assert!(self.callable);

        use std::mem;
        if !self.x.has_swapped() {
            let real = self.real.load(sync::atomic::Ordering::Acquire);
            if self.after != real {
                // replace() happened
                self.after = real;
                self.x.swapped();
            }
        }

        if self.x.use_post() {
            unsafe { mem::transmute(self.after) }
        } else {
            unsafe { mem::transmute(self.before) }
        }
    }
}

impl Drop for Getter {
    fn drop(&mut self) {
        if self.callable {
            // free original get function
            drop(unsafe { Box::from_raw(self.before) });

            let real = self.real.load(sync::atomic::Ordering::Acquire);
            if self.x.has_swapped() {
                // we swapped, so also free after
                assert_eq!(self.after, real);
                drop(unsafe { Box::from_raw(self.after) });
            } else {
                assert_eq!(self.before, real);
            }
        }
    }
}

struct Spoon {
    graph: sync::Arc<sync::Mutex<graph::Graph>>,
    article: Mutator,
    getters: Vec<Getter>,
    vote_pre: Mutator,
    vote_post: Option<Mutator>,
    stupid: bool,
    i: usize,
    x: Crossover,
    new_vote: Option<mpsc::Receiver<Mutator>>,
    transactions: bool,
}

impl Writer for Spoon {
    type Migrator = Migrator;

    fn make_articles<I>(&mut self, articles: I)
        where I: ExactSizeIterator,
              I: Iterator<Item = (i64, String)>
    {
        for (article_id, title) in articles {
            self.article
                .put(vec![article_id.into(), title.into()])
                .unwrap();
        }
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        if self.new_vote.is_some() {
            self.i += 1;
            // don't try too eagerly
            if self.i & 16384 == 0 {
                // we may have been given a new putter
                if let Ok(nv) = self.new_vote.as_mut().unwrap().try_recv() {
                    // yay!
                    self.new_vote = None;
                    self.x.swapped();
                    self.vote_post = Some(nv);
                    self.i = usize::max_value();
                }
            }
        }

        if self.x.use_post() {
            for &(user_id, article_id) in ids {
                self.vote_post
                    .as_mut()
                    .unwrap()
                    .put(vec![user_id.into(), article_id.into(), 5.into()])
                    .unwrap();
            }
            Period::PostMigration
        } else {
            for &(user_id, article_id) in ids {
                self.vote_pre
                    .put(vec![user_id.into(), article_id.into()])
                    .unwrap();
            }
            // XXX: unclear if this should be Pre- or Post- if self.x.has_swapped()
            Period::PostMigration
        }
    }

    fn prepare_migration(&mut self) -> Self::Migrator {
        let (tx, rx) = mpsc::sync_channel(0);
        self.new_vote = Some(rx);
        self.i = 1;
        Migrator {
            graph: self.graph.clone(),
            mut_tx: tx,
            stupid: self.stupid,
            getters: self.getters.iter().map(|g| unsafe { g.clone() }).collect(),
            transactions: self.transactions,
        }
    }
}

struct Migrator {
    graph: sync::Arc<sync::Mutex<graph::Graph>>,
    mut_tx: mpsc::SyncSender<Mutator>,
    getters: Vec<Getter>,
    transactions: bool,
    stupid: bool,
}

impl MigrationHandle for Migrator {
    fn execute(&mut self) {
        let mut g = self.graph.lock().unwrap();
        let (rating, newend) = g.transition(self.stupid, self.transactions);
        let mutator = g.graph.get_mutator(rating);
        self.mut_tx.send(mutator).unwrap();
        for getter in &mut self.getters {
            unsafe { getter.replace(g.graph.get_getter(newend).unwrap()) };
        }
    }
}

impl Reader for Getter {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let res = ids.iter()
            .map(|&(_, article_id)| {
                let rows = try!((self.call())(&article_id.into(), true).map_err(|_| ()));
                debug_assert_eq!(rows.len(), 1);
                match rows.into_iter().last() {
                    Some(row) => {
                        // we only care about the first result
                        let mut row = row.into_iter();
                        let id: i64 = row.next().unwrap().into();
                        let title: String = row.next().unwrap().into();
                        let votes: i64 = match row.next().unwrap() {
                            DataType::None => 42,
                            d => d.into(),
                        };
                        Ok(ArticleResult::Article {
                               id: id,
                               title: title,
                               votes: votes,
                           })
                    }
                    None => Ok(ArticleResult::NoSuchArticle),
                }
            })
            .collect();

        if self.same() {
            (res, Period::PreMigration)
        } else {
            (res, Period::PostMigration)
        }
    }
}
