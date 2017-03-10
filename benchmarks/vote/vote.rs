#[macro_use]
extern crate clap;

extern crate slog;
extern crate slog_term;

extern crate rand;

extern crate distributary;

extern crate hdrsample;

extern crate spmc;

mod exercise;
mod common;
mod graph;

use common::{Writer, Reader, ArticleResult, Period};
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
        .arg(Arg::with_name("migrate")
            .short("m")
            .long("migrate")
            .value_name("N")
            .help("Perform a migration after this many seconds")
            .conflicts_with("stage"))
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let ngetters = value_t_or_exit!(args, "ngetters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    assert!(ngetters > 0);

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = exercise::RuntimeConfig::new(narticles, runtime);
    config.produce_cdf(cdf);
    if let Some(migrate_after) = migrate_after {
        config.perform_migration_at(migrate_after);
    }

    // setup db
    let g = graph::make();
    let putter = (g.graph.get_mutator(g.article), g.graph.get_mutator(g.vote));
    let getter = sync::Arc::new(Getter(g.end));
    let getters = (0..ngetters).into_iter().map(move |_| getter.clone());

    let put_stats;
    let get_stats: Vec<_>;
    if stage {
        // put then get
        put_stats = exercise::launch_writer(putter, config, None);
        let getters: Vec<_> = getters.enumerate()
            .map(|(i, g)| {
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || exercise::launch_reader(g, config))
                    .unwrap()
            })
            .collect();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
    } else {
        // put & get
        // TODO: how do we start getters after prepopulate?
        let (tx, prepop) = mpsc::sync_channel(0);
        let putter = thread::Builder::new()
            .name("PUT0".to_string())
            .spawn(move || exercise::launch_writer(putter, config, Some(tx)))
            .unwrap();

        // wait for prepopulation to finish
        prepop.recv().unwrap();

        let getters: Vec<_> = getters.enumerate()
            .map(|(i, g)| {
                thread::Builder::new()
                    .name(format!("GET{}", i))
                    .spawn(move || exercise::launch_reader(g, config))
                    .unwrap()
            })
            .collect();

        put_stats = putter.join().unwrap();
        get_stats = getters.into_iter().map(|jh| jh.join().unwrap()).collect();
    }

    print_stats("PUT", &put_stats.pre, avg);
    for (i, s) in get_stats.iter().enumerate() {
        print_stats(format!("GET{}", i), &s.pre, avg);
    }
    if avg {
        let sum = get_stats.iter().fold((0f64, 0usize), |(tot, count), stats| {
            // TODO: do we *really* want an average of averages?
            let (sum, num) = stats.pre.sum_len();
            (tot + sum, count + num)
        });
        println!("avg GET: {:.2}", sum.0 as f64 / sum.1 as f64);
    }

    if migrate_after.is_some() {
        print_stats("PUT+", &put_stats.post, avg);
        for (i, s) in get_stats.iter().enumerate() {
            print_stats(format!("GET{}+", i), &s.post, avg);
        }
        if avg {
            let sum = get_stats.iter().fold((0f64, 0usize), |(tot, count), stats| {
                // TODO: do we *really* want an average of averages?
                let (sum, num) = stats.pre.sum_len();
                (tot + sum, count + num)
            });
            println!("avg GET+: {:.2}", sum.0 as f64 / sum.1 as f64);
        }
    }
}

fn print_stats<S: AsRef<str>>(desc: S, stats: &exercise::BenchmarkResult, avg: bool) {
    if let Some(perc) = stats.cdf_percentiles() {
        for (v, p, _, _) in perc {
            println!("percentile {} {:.2} {:.2}", desc.as_ref(), v, p);
        }
    }
    if avg {
        println!("avg {}: {:.2}", desc.as_ref(), stats.avg_throughput());
    }
}

struct Getter(Box<Fn(&DataType) -> Result<Vec<Vec<DataType>>, ()> + Send + Sync>);

//      if i % 16384 == 0 && start.elapsed() > migrate_after {
//         // we may have been given a new putter
//         if let Ok(np) = np_rx.try_recv() {
//             // we have a new putter!
//             //
//             // it's unfortunate that we have to use unsafe here...
//             // hopefully it can go if
//             // https://github.com/Kimundi/owning-ref-rs/issues/22
//             // gets resolved. fundamentally, the problem is that the compiler
//             // doesn't know that we won't overwrite new_putter in some
//             // subsequent iteration of the loop, which would leave new_vote
//             // with a dangling pointer to new_putter! *we* know that won't
//             // happen though, so this is ok
//             new_putter = Some(np);
//             let np = new_putter.as_mut().unwrap() as *mut _;
//             let np: &mut B::P = unsafe { &mut *np };
//             new_vote = Some(np.vote());
//         }
//     }
//     i += 1;

impl Writer for (Mutator, Mutator) {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        self.0.put(vec![article_id.into(), title.into()]);
    }
    fn vote(&mut self, user_id: i64, article_id: i64) {
        self.1.put(vec![user_id.into(), article_id.into()]);
        // post-migration
        // Box::new(move |user, id| { self.0(vec![user.into(), id.into(), 5.into()]); })
    }
    fn prepare_migration(&mut self) -> Self::Migrator {
        // TODO
        unimplemented!()
    }
}

impl Reader for sync::Arc<Getter> {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period) {
        let id = article_id.into();
        let res = match self.0(&id) {
            Ok(g) => {
                match g.into_iter().next() {
                    Some(row) => {
                        // we only care about the first result
                        let mut row = row.into_iter();
                        let id: i64 = row.next().unwrap().into();
                        let title: String = row.next().unwrap().into();
                        let count: i64 = row.next().unwrap().into();
                        ArticleResult::Article {
                            id: id,
                            title: title,
                            votes: count,
                        }
                    }
                    None => ArticleResult::NoSuchArticle,
                }
            }
            Err(_) => ArticleResult::Error,
        };
        (res, Period::PreMigration)
    }
}

/*
fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
    let (rating, newendq) = {
        // migrate
        let mut mig = self._g.start_migration();

        // add new "ratings" base table
        let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base::default());

        // add sum of ratings
        let rs = mig.add_ingredient("rsum",
                                    &["id", "total"],
                                    Aggregation::SUM.over(rating, 2, &[1]));

        // take a union of vote count and rsum
        let mut emits = HashMap::new();
        emits.insert(rs, vec![0, 1]);
        emits.insert(self.vci, vec![0, 1]);
        let u = Union::new(emits);
        let both = mig.add_ingredient("both", &["id", "value"], u);

        // sum them by article id
        let total = mig.add_ingredient("total",
                                       &["id", "total"],
                                       Aggregation::SUM.over(both, 1, &[0]));

        // finally, produce end result
        let j = JoinBuilder::new(vec![(self.articlei, 0), (self.articlei, 1), (total, 1)])
            .from(self.articlei, vec![1, 0])
            .join(total, vec![1, 0]);
        let newend = mig.add_ingredient("awr", &["id", "title", "score"], j);
        let newendq = mig.maintain(newend, 0);

        // we want ratings, rsum, and the union to be in the same domain,
        // because only rsum is really costly
        let domain = mig.add_domain();
        mig.assign_domain(rating, domain);
        mig.assign_domain(rs, domain);
        mig.assign_domain(both, domain);

        // and then we want the total sum and the join in the same domain,
        // to avoid duplicating the total state
        let domain = mig.add_domain();
        mig.assign_domain(total, domain);
        mig.assign_domain(newend, domain);

        // start processing
        mig.commit();
        (rating, newendq)
    };

    let mutator = self._g.get_mutator(rating);
    let put = Box::new(move |u: Vec<DataType>| { mutator.put(u); });


    let newendq = sync::Arc::new(newendq);
    ((put, None), (0..ngetters).into_iter().map(|_| newendq.clone()).collect())
}
*/
