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

use common::{ArticleResult, Distribution, Period, Reader, RuntimeConfig, Writer};
use distributary::{DataType, DurabilityMode, Mutator, PersistenceParameters};
use std::sync::mpsc;

use std::thread;
use std::sync;
use std::time;

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("avg")
                .long("avg")
                .takes_value(false)
                .help("compute average throughput at the end of benchmark"),
        )
        .arg(
            Arg::with_name("cdf")
                .short("c")
                .long("cdf")
                .takes_value(false)
                .help("produce a CDF of recorded latencies for each client at the end"),
        )
        .arg(
            Arg::with_name("stage")
                .short("s")
                .long("stage")
                .takes_value(false)
                .help("stage execution such that all writes are performed before all reads"),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .takes_value(true)
                .default_value("uniform")
                .help(
                    "run benchmark with the given article id distribution [uniform|zipf:exponent]",
                ),
        )
        .arg(
            Arg::with_name("nmixers")
                .short("n")
                .long("mixers")
                .value_name("N")
                .help("Number of MIX clients to start"),
        )
        .arg(
            Arg::with_name("ngetters")
                .short("g")
                .long("getters")
                .value_name("N")
                .default_value("1")
                .help("Number of GET clients to start"),
        )
        .arg(
            Arg::with_name("nputters")
                .short("p")
                .long("putters")
                .value_name("N")
                .default_value("1")
                .help("Number of PUT clients to start"),
        )
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("60")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("2")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(
            Arg::with_name("stupid")
                .long("stupid")
                .help("Make the migration stupid")
                .requires("migrate"),
        )
        .arg(
            Arg::with_name("migrate")
                .short("m")
                .long("migrate")
                .value_name("N")
                .help("Perform a migration after this many seconds")
                .conflicts_with("stage"),
        )
        .arg(
            Arg::with_name("transactions")
                .short("t")
                .long("transactions")
                .takes_value(false)
                .help("Use transactional reads and writes"),
        )
        .arg(
            Arg::with_name("crossover")
                .short("x")
                .takes_value(true)
                .help("Period for transition to new views for readers and writers")
                .requires("migrate"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("No noisy output while running"),
        )
        .arg(
            Arg::with_name("durability")
                .long("durability")
                .takes_value(false)
                .help("Enable durability for Base nodes"),
        )
        .arg(
            Arg::with_name("retain-logs-on-exit")
                .long("retain-logs-on-exit")
                .takes_value(false)
                .requires("durability")
                .help("Do not delete the base node logs on exit."),
        )
        .arg(
            Arg::with_name("write-batch-size")
                .long("write-batch-size")
                .takes_value(true)
                .default_value("512")
                .help("Size of batches processed at base nodes."),
        )
        .arg(
            Arg::with_name("MODE")
                .index(1)
                .requires("nmixers")
                .conflicts_with_all(&["migrate", "stage"])
                .help("Mode to run clients in [read|write|mix:rw_ratio]"),
        )
        .arg(
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .takes_value(true)
                .default_value("2")
                .help("Number of workers to use"),
        )
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let dist = value_t_or_exit!(args, "distribution", Distribution);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let crossover = args.value_of("crossover")
        .map(|_| value_t_or_exit!(args, "crossover", u64))
        .map(time::Duration::from_secs);
    let mut ngetters = value_t_or_exit!(args, "ngetters", usize);
    let mut nputters = value_t_or_exit!(args, "nputters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    let queue_length = value_t_or_exit!(args, "write-batch-size", usize);
    let flush_timeout = time::Duration::from_millis(10);
    let nworkers = value_t_or_exit!(args, "workers", usize);
    let verbose = !args.is_present("quiet");

    let mix = match args.value_of("MODE") {
        Some("read") => Some(common::Mix::Read(128)),
        Some("write") => Some(common::Mix::Write(128)),
        Some(ref mode) if mode.starts_with("mix:") => {
            // ratio is number of reads per write
            let ratio = mode.split(':')
                .skip(1)
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap();
            Some(common::Mix::RW(ratio * 16, 16))
        }
        Some(_) => unreachable!(),
        None => None,
    };

    if mix.is_some() {
        assert!(args.occurrences_of("nputters") == 0);
        assert!(args.occurrences_of("ngetters") == 0);
        let nmixers = value_t_or_exit!(args, "nmixers", usize);
        assert!(nmixers > 0);

        // we can make the code below do what we want by playing a simple trick: use no readers,
        // and nmixers writers, and then force those writers to also grab a Getter and use the
        // proper mix.
        ngetters = 0;
        nputters = nmixers;
    } else {
        assert!(ngetters > 0);
        assert!(nputters > 0);
    }

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = RuntimeConfig::new(narticles, common::Mix::Read(128), Some(runtime));
    config.set_verbose(verbose);
    config.produce_cdf(cdf);
    config.use_distribution(dist);

    let mode = if args.is_present("durability") {
        if args.is_present("retain-logs-on-exit") {
            DurabilityMode::Permanent
        } else {
            DurabilityMode::DeleteOnExit
        }
    } else {
        DurabilityMode::MemoryOnly
    };

    let persistence_params = PersistenceParameters::new(
        mode,
        queue_length,
        flush_timeout,
        None,
        Some(String::from("vote")),
    );

    // setup db
    let mut s = graph::Setup::new(true, nworkers);
    s.logging = verbose;
    s.transactions = args.is_present("transactions");
    s.sharding = match value_t_or_exit!(args, "shards", usize) {
        0 => None,
        x => Some(x),
    };
    s.stupid = args.is_present("stupid");
    let g = graph::make(s, persistence_params);

    // prepare getters
    let getters: Vec<_> = {
        (0..ngetters)
            .into_iter()
            .map(|_| {
                Getter::new(g.graph.get_getter(g.end).unwrap(), crossover)
            })
            .collect()
    };

    let mut new_vote_senders = Vec::new();
    let mut new_vote_receivers = Vec::new();
    for _ in 0..nputters {
        let (tx, rx) = mpsc::channel();
        new_vote_receivers.push(rx);
        new_vote_senders.push(tx);
    }

    let new_votes = migrate_after.map(|t| (t, new_vote_senders));

    // prepare putters
    let putters: Vec<_> = {
        let mix_getters = (0..new_vote_receivers.len())
            .map(|_| mix.as_ref().map(|_| g.graph.get_getter(g.end).unwrap()));
        new_vote_receivers
            .into_iter()
            .zip(mix_getters)
            .map(|(new_vote, mix_getter)| {
                Spoon {
                    article: g.graph.get_mutator(g.article).unwrap(),
                    vote_pre: g.graph.get_mutator(g.vote).unwrap(),
                    vote_post: None,
                    new_vote: new_votes.as_ref().and(Some(new_vote)),
                    x: Crossover::new(crossover),
                    i: 0,
                    mix_getter,
                }
            })
            .collect()
    };

    let put_name = if mix.is_some() { "MIX" } else { "PUT" };
    let put_stats: Vec<_>;
    let get_stats: Vec<_>;
    if stage {
        let start = sync::Arc::new(sync::Barrier::new(putters.len()));
        let prepop = Some(sync::Arc::new(sync::Barrier::new(putters.len() + 1)));
        let mut pconfig = config.clone();
        pconfig.mix = common::Mix::Write(128);
        // put first
        let putters: Vec<_> = putters
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                let start = start.clone();
                let prepop = prepop.clone();
                let mut pconfig = pconfig.clone();
                if i != 0 {
                    // only one thread does prepopulation
                    pconfig.set_reuse(true);
                }
                thread::Builder::new()
                    .name(format!("PUT{}", i))
                    .spawn(move || {
                        exercise::launch(
                            None::<exercise::NullClient>,
                            Some(p),
                            pconfig,
                            prepop,
                            start,
                        )
                    })
                    .unwrap()
            })
            .collect();

        prepop.map(|b| b.wait());
        // prepop finished, now wait for them to finish
        put_stats = putters.into_iter().map(|jh| jh.join().unwrap()).collect();

        // then get
        let start = sync::Arc::new(sync::Barrier::new(getters.len()));
        let getters: Vec<_> = getters
            .into_iter()
            .enumerate()
            .map(|(i, g)| {
                let start = start.clone();
                let config = config.clone();
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || {
                        exercise::launch(Some(g), None::<exercise::NullClient>, config, None, start)
                    })
                    .unwrap()
            })
            .collect();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
    } else {
        // put & get
        let mut n = putters.len() + getters.len();
        if new_votes.is_some() {
            n += 1;
        }
        let start = sync::Arc::new(sync::Barrier::new(n));
        let barrier = Some(sync::Arc::new(sync::Barrier::new(putters.len() + 1)));
        let mut pconfig = config.clone();
        pconfig.mix = match mix {
            Some(ref mix) => mix.clone(),
            None => common::Mix::Write(128),
        };
        let putters: Vec<_> = putters
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                let start = start.clone();
                let barrier = barrier.clone();
                let mut pconfig = pconfig.clone();
                if i != 0 {
                    // only one thread does prepopulation
                    pconfig.set_reuse(true);
                }
                let is_mix = mix.is_some();
                thread::Builder::new()
                    .name(format!("{}{}", put_name, i))
                    .spawn(move || {
                        if is_mix {
                            exercise::launch_mix_wait(p, pconfig, barrier, start)
                        } else {
                            exercise::launch(
                                None::<exercise::NullClient>,
                                Some(p),
                                pconfig,
                                barrier,
                                start,
                            )
                        }
                    })
                    .unwrap()
            })
            .collect();

        // wait for prepopulation to finish
        barrier.map(|b| b.wait());

        // start migrator
        // we need a barrier to make sure the migrator doesn't drop the graph too early
        let drop = sync::Arc::new(sync::Barrier::new(2));
        let mig = if let Some((dur, bus)) = new_votes {
            let m = Migrator {
                graph: g,
                barrier: drop.clone(),
                getters: getters.iter().map(|g| unsafe { g.clone() }).collect(),
                bus: bus,
            };
            let start = start.clone();
            Some((
                drop,
                thread::Builder::new()
                    .name("migrator".to_string())
                    .spawn(move || {
                        start.wait();
                        m.run(dur)
                    })
                    .unwrap(),
            ))
        } else {
            None
        };

        let getters: Vec<_> = getters
            .into_iter()
            .enumerate()
            .map(|(i, g)| {
                let start = start.clone();
                let config = config.clone();
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || {
                        exercise::launch(Some(g), None::<exercise::NullClient>, config, None, start)
                    })
                    .unwrap()
            })
            .collect();

        put_stats = putters.into_iter().map(|jh| jh.join().unwrap()).collect();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
        if let Some((drop, mig)) = mig {
            drop.wait();
            mig.join().unwrap();
        }
    }

    for (i, s) in put_stats.iter().enumerate() {
        print_stats(format!("{}{}", put_name, i), true, &s.pre, avg);
    }
    for (i, s) in get_stats.iter().enumerate() {
        print_stats(format!("GET{}", i), true, &s.pre, avg);
    }
    if avg {
        if nputters != 0 {
            let sum = put_stats
                .iter()
                .fold((0f64, 0usize, 0f64), |(tot, count, totavg), stats| {
                    // TODO: do we *really* want an average of averages?
                    let (sum, num) = stats.pre.sum_len();
                    (tot + sum, count + num, totavg + (sum / num as f64))
                });
            println!("\navg {}: {:.2}", put_name, sum.0 as f64 / sum.1 as f64);
            println!("cumavg {}: {:.2}", put_name, sum.2 as f64);
        }
        if ngetters != 0 {
            let sum = get_stats
                .iter()
                .fold((0f64, 0usize, 0f64), |(tot, count, totavg), stats| {
                    // TODO: do we *really* want an average of averages?
                    let (sum, num) = stats.pre.sum_len();
                    (tot + sum, count + num, totavg + (sum / num as f64))
                });
            println!("\navg GET: {:.2}", sum.0 as f64 / sum.1 as f64);
            println!("cumavg GET: {:.2}", sum.2 as f64);
        }
    }

    if migrate_after.is_some() {
        for (i, s) in put_stats.iter().enumerate() {
            print_stats(format!("PUT{}+", i), true, &s.post, avg);
        }
        for (i, s) in get_stats.iter().enumerate() {
            print_stats(format!("GET{}+", i), true, &s.post, avg);
        }
        if avg {
            let sum = put_stats
                .iter()
                .fold((0f64, 0usize), |(tot, count), stats| {
                    // TODO: do we *really* want an average of averages?
                    let (sum, num) = stats.post.sum_len();
                    (tot + sum, count + num)
                });
            println!("avg PUT+: {:.2}", sum.0 as f64 / sum.1 as f64);
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
            println!(
                "percentile {} {:.2} {:.2}",
                desc.as_ref(),
                iv.value(),
                iv.percentile()
            );
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

            self.post =
                ((elapsed as f64 / self.crossover.unwrap() as f64) * (1 << 12) as f64) as usize;
            self.iteration = 0;
        }

        self.iteration < self.post
    }
}

type G = distributary::RemoteGetter;

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
    article: Mutator,
    vote_pre: Mutator,
    vote_post: Option<Mutator>,
    i: usize,
    x: Crossover,
    new_vote: Option<mpsc::Receiver<Mutator>>,
    mix_getter: Option<distributary::RemoteGetter>,
}

impl Writer for Spoon {
    fn make_articles<I>(&mut self, articles: I)
    where
        I: ExactSizeIterator,
        I: Iterator<Item = (i64, String)>,
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
            if self.i % 65536 == 0 {
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
            let data: Vec<Vec<DataType>> = ids.iter()
                .map(|&(user_id, article_id)| {
                    vec![user_id.into(), article_id.into(), 5.into()]
                })
                .collect();

            self.vote_post.as_mut().unwrap().multi_put(data).unwrap();
            Period::PostMigration
        } else {
            let data: Vec<Vec<DataType>> = ids.iter()
                .map(|&(user_id, article_id)| {
                    vec![user_id.into(), article_id.into()]
                })
                .collect();
            self.vote_pre.multi_put(data).unwrap();

            if !self.x.has_swapped() {
                Period::PreMigration
            } else {
                // XXX: unclear if this should be Pre- or Post- if self.x.has_swapped()
                Period::PostMigration
            }
        }
    }
}

struct Migrator {
    graph: graph::Graph,
    bus: Vec<mpsc::Sender<Mutator>>,
    getters: Vec<Getter>,
    barrier: sync::Arc<sync::Barrier>,
}

impl Migrator {
    pub fn run(mut self, migrate_after: time::Duration) {
        thread::sleep(migrate_after);
        println!("Starting migration");
        let mig_start = time::Instant::now();
        let (rating, newend) = self.graph.transition();
        for sender in self.bus {
            sender
                .send(self.graph.graph.get_mutator(rating).unwrap())
                .unwrap();
        }
        for mut getter in self.getters {
            unsafe { getter.replace(self.graph.graph.get_getter(newend).unwrap()) };
        }
        let mig_duration = dur_to_ns!(mig_start.elapsed()) as f64 / 1_000_000_000.0;
        println!("Migration completed in {:.4}s", mig_duration);
        self.barrier.wait();
    }
}

impl Reader for Getter {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let arg = ids.iter()
            .map(|&(_, article_id)| article_id.into())
            .collect();
        let res = (self.call())
            .multi_lookup(arg, true)
            .into_iter()
            .map(|res| {
                res.map(|rows| match rows.len() {
                    0 => ArticleResult::NoSuchArticle,
                    1 => {
                        let mut row = rows.into_iter().next().unwrap().into_iter();
                        let id: i64 = row.next().unwrap().into();
                        let title: String = row.next().unwrap().into();
                        let votes: i64 = match row.next().unwrap() {
                            DataType::None => 42,
                            d => d.into(),
                        };
                        ArticleResult::Article {
                            id: id,
                            title: title,
                            votes: votes,
                        }
                    }
                    _ => unreachable!(),
                })
            })
            .collect();

        if self.same() {
            (res, Period::PreMigration)
        } else {
            (res, Period::PostMigration)
        }
    }
}

// same for sneaky reader Spoon
impl Reader for Spoon {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let res = ids.iter()
            .map(|&(_, article_id)| {
                self.mix_getter
                    .as_mut()
                    .unwrap()
                    .lookup(&article_id.into(), true)
                    .map(|rows| match rows.len() {
                        0 => ArticleResult::NoSuchArticle,
                        1 => {
                            let row = &rows[0];
                            let id: i64 = row[0].clone().into();
                            let title: String = row[1].deep_clone().into();
                            let votes: i64 = match row[2] {
                                DataType::None => 42,
                                ref d => d.clone().into(),
                            };
                            ArticleResult::Article {
                                id: id,
                                title: title,
                                votes: votes,
                            }
                        }
                        _ => unreachable!(),
                    })
            })
            .collect();

        (res, Period::PreMigration)
    }
}
