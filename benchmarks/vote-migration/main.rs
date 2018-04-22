#[macro_use]
extern crate clap;
extern crate distributary;
extern crate rand;
extern crate zipf;

#[path = "../vote/clients/localsoup/graph.rs"]
mod graph;

use distributary::DataType;
use rand::Rng;
use std::sync::{Arc, Barrier};
use std::{thread, time};

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }};
}

struct Reporter {
    last: time::Instant,
    every: time::Duration,
    count: usize,
}

impl Reporter {
    pub fn report(&mut self, n: usize) -> Option<usize> {
        self.count += n;

        if self.last.elapsed() > self.every {
            let count = Some(self.count);
            self.last = time::Instant::now();
            self.count = 0;
            count
        } else {
            None
        }
    }

    pub fn new(every: time::Duration) -> Self {
        Reporter {
            last: time::Instant::now(),
            every: every,
            count: 0,
        }
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .takes_value(true)
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .required(true)
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("migrate")
                .short("m")
                .long("migrate")
                .required(true)
                .takes_value(true)
                .help("Perform a migration after this many seconds")
                .conflicts_with("stage"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("Enable verbose logging output"),
        )
        .arg(
            Arg::with_name("full")
                .long("full")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("stupid")
                .long("stupid")
                .help("Make the migration stupid"),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .help("Use N-way sharding."),
        )
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = time::Duration::from_secs(value_t_or_exit!(args, "migrate", u64));
    assert!(migrate_after < runtime);

    // reporting config
    let every = time::Duration::from_millis(200);

    // default persistence (memory only)
    let mut persistence_params = distributary::PersistenceParameters::default();
    persistence_params.queue_capacity = 1;
    persistence_params.mode = distributary::DurabilityMode::MemoryOnly;

    // set config options
    let mut s = graph::Setup::default();
    s.partial = !args.is_present("full");
    s.stupid = args.is_present("stupid");
    s.sharding = args.value_of("shards")
        .map(|_| value_t_or_exit!(args, "shards", usize));
    s.logging = args.is_present("verbose");
    s.nreaders = 4;
    s.nworkers = 4;

    // make the graph!
    eprintln!("Setting up soup");
    let mut g = graph::make(s, persistence_params);
    eprintln!("Getting accessors");
    let mut articles = g.graph.get_mutator("Article").unwrap().into_exclusive();
    let mut votes = g.graph.get_mutator("Vote").unwrap().into_exclusive();
    let mut read_old = g.graph
        .get_getter("ArticleWithVoteCount")
        .unwrap()
        .into_exclusive();

    // prepopulate
    eprintln!("Prepopulating with {} articles", narticles);
    for i in 0..(narticles as i64) {
        articles
            .put(vec![i.into(), format!("Article #{}", i).into()])
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));

    // start writer that just does a bunch of old writes
    eprintln!("Starting old writer");
    let w1 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut reporter = Reporter::new(every);
            barrier.wait();
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                let n = 500;
                votes
                    .batch_put((0..n).map(|i| vec![rng.gen_range(0, narticles).into(), i.into()]))
                    .unwrap();

                if let Some(count) = reporter.report(n) {
                    let count_per_ns = count as f64 / dur_to_ns!(every) as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    println!("{:?} OLD: {:.2}", dur_to_ns!(start.elapsed()), count_per_s);
                }
            }
        })
    };

    // start a read that just reads old forever
    eprintln!("Starting old reader");
    let r1 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            barrier.wait();
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                read_old
                    .lookup(&[DataType::from(rng.gen_range(0, narticles))], true)
                    .unwrap();
                thread::yield_now();
            }
        })
    };

    // wait for other threads to be ready
    barrier.wait();
    let start = time::Instant::now();

    // we now need to wait for migrate_after
    eprintln!("Waiting for migration time...");
    thread::sleep(migrate_after);

    // all right, migration time
    eprintln!("Starting migration");
    println!("{:?} MIG START", dur_to_ns!(start.elapsed()));
    g.transition();
    println!("{:?} MIG FINISHED", dur_to_ns!(start.elapsed()));
    let mut ratings = g.graph.get_mutator("Rating").unwrap().into_exclusive();
    let mut read_new = g.graph
        .get_getter("ArticleWithScore")
        .unwrap()
        .into_exclusive();

    // start writer that just does a bunch of new writes
    eprintln!("Starting new writer");
    let w2 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut reporter = Reporter::new(every);
            barrier.wait();
            while start.elapsed() < runtime {
                let n = 500;
                ratings
                    .batch_put(
                        (0..n)
                            .map(|i| vec![rng.gen_range(0, narticles).into(), i.into(), 5.into()]),
                    )
                    .unwrap();

                if let Some(count) = reporter.report(n) {
                    let count_per_ns = count as f64 / dur_to_ns!(every) as f64;
                    let count_per_s = count_per_ns * NANOS_PER_SEC as f64;
                    println!("{:?} NEW: {:.2}", dur_to_ns!(start.elapsed()), count_per_s);
                }
            }
        })
    };

    // start reader that keeps probing new read view
    eprintln!("Starting new read probe");
    let r2 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut hits = 0;
            let mut rng = rand::thread_rng();
            let mut reporter = Reporter::new(every);
            barrier.wait();
            while start.elapsed() < runtime {
                match read_new.lookup(&[DataType::from(rng.gen_range(0, narticles))], false) {
                    Ok(ref rs) if !rs.is_empty() => {
                        hits += 1;
                    }
                    _ => {
                        // miss, or view not yet ready
                    }
                }

                if let Some(count) = reporter.report(1) {
                    println!(
                        "{:?} HITF: {:.2}",
                        dur_to_ns!(start.elapsed()),
                        hits as f64 / count as f64
                    );
                    hits = 0;
                }
                thread::sleep(time::Duration::new(0, 10_000));
            }
        })
    };

    // fire them both off!
    barrier.wait();

    // everything finishes!
    eprintln!("Waiting for experiment to end...");
    w1.join().unwrap();
    w2.join().unwrap();
    r1.join().unwrap();
    r2.join().unwrap();

    println!("FIN");
}
