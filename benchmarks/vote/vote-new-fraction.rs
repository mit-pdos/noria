#[macro_use]
extern crate clap;

extern crate slog;
extern crate slog_term;

extern crate rand;

extern crate distributary;

extern crate hdrsample;
extern crate zipf;

extern crate spmc;

mod graph;

#[macro_use]
#[allow(dead_code)]
mod common;

const READ_RATE: u64 = 1_500_000;
const WRITE_RATE: u64 = 150_000;

use zipf::ZipfDistribution;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time;

fn randomness(distribution: common::Distribution, range: usize, n: u64) -> Vec<i64> {
    use rand::Rng;

    // random article ids with distribution. we pre-generate these to avoid overhead at
    // runtime. note that we don't use Iterator::cycle, since it buffers by cloning, which
    // means it might also do vector resizing.
    println!("Generating ~{}M random numbers; this'll take a few seconds...",
             n / 1_000_000);
    match distribution {
        common::Distribution::Uniform => {
            let mut u = rand::thread_rng();
            (0..n)
                .map(|_| u.gen_range(0, range as i64) as i64)
                .collect()
        }
        common::Distribution::Zipf(e) => {
            let mut z = ZipfDistribution::new(rand::thread_rng(), range, e).unwrap();
            (0..n)
                .map(|_| z.gen_range(0, range as i64) as i64)
                .collect()
        }
    }
}

struct Reporter {
    last: time::Instant,
    every: time::Duration,
    count: usize,
}

impl Reporter {
    pub fn report(&mut self) -> Option<usize> {
        self.count += 1;

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
    use clap::{Arg, App};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
        .arg(Arg::with_name("distribution")
            .short("d")
            .takes_value(true)
            .required(true)
            .default_value("uniform")
            .help("run benchmark with the given article id distribution [uniform|zipf:exponent]"))
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
            .help("Make the migration stupid"))
        .arg(Arg::with_name("migrate")
            .short("m")
            .long("migrate")
            .value_name("N")
            .required(true)
            .help("Perform a migration after this many seconds")
            .conflicts_with("stage"))
        .get_matches();

    let dist = value_t_or_exit!(args, "distribution", common::Distribution);
    let narticles = value_t_or_exit!(args, "narticles", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = time::Duration::from_secs(value_t_or_exit!(args, "migrate", u64));
    assert!(migrate_after < runtime);

    // reporting config
    let every = time::Duration::from_millis(200);

    // setup db
    let mut g = graph::make(false, false, None);

    // we need a putter and a getter
    let articles = g.graph.get_mutator(g.article);
    let votes = g.graph.get_mutator(g.vote);
    let read_old = g.graph.get_getter(g.end).unwrap();

    // prepopulate
    println!("Prepopulating with {} articles", narticles);
    for i in 0..(narticles as i64) {
        articles.put(vec![i.into(), format!("Article #{}", i).into()]);
    }

    // keep track of when randomness is ready
    let barrier = Arc::new(Barrier::new(3));

    println!("Starting old writer");

    // start writer that just does a bunch of old writes
    let w1 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut i = 0;
            let random = randomness(dist, narticles, WRITE_RATE * runtime.as_secs());
            barrier.wait();

            let start = time::Instant::now();
            let mut reporter = Reporter::new(every);
            while start.elapsed() < runtime {
                votes.put(vec![0.into(), random[i].into()]);
                i = (i + 1) % random.len();

                if let Some(count) = reporter.report() {
                    let count_per_ns = count as f64 / dur_to_ns!(every) as f64;
                    let count_per_s = count_per_ns * common::NANOS_PER_SEC as f64;
                    println!("{:?} OLD: {:.2}", dur_to_ns!(start.elapsed()), count_per_s);
                }
            }
        })
    };

    println!("Starting old reader");

    // start a read that just reads old forever
    let r1 = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let mut i = 0;
            let random = randomness(dist, narticles, READ_RATE * runtime.as_secs());
            barrier.wait();
            let start = time::Instant::now();
            while start.elapsed() < runtime {
                read_old(&random[i].into(), true).unwrap();
                i = (i + 1) % random.len();
                thread::yield_now();
            }
        })
    };

    // wait for other threads to be ready
    barrier.wait();
    let start = time::Instant::now();

    // we now need to wait for migrate_after
    // instead, we spend the interrim generating random numbers
    println!("Preparing migration while running");
    let w_random = randomness(dist, narticles, WRITE_RATE * runtime.as_secs());
    let r_random = randomness(dist, narticles, READ_RATE * runtime.as_secs());
    // then we wait
    if start.elapsed() > migrate_after {
        println!("Migration preparation overran by {}s -- starting immediately",
                 (start.elapsed() - migrate_after).as_secs());
    } else {
        println!("Preparation finished; waiting for another {}s",
                 (migrate_after - start.elapsed()).as_secs());
        thread::sleep(migrate_after - start.elapsed());
        println!("Starting migration");
    }

    // all right, migration time
    let (ratings, read_new) = g.transition(args.is_present("stupid"), false);
    let ratings = g.graph.get_mutator(ratings);
    let read_new = g.graph.get_getter(read_new).unwrap();

    println!("Starting new writer");

    // start writer that just does a bunch of new writes
    let w2 = thread::spawn(move || {
        let mut i = 0;
        let mut reporter = Reporter::new(every);
        while start.elapsed() < runtime {
            ratings.put(vec![0.into(), w_random[i].into(), 5.into()]);
            i = (i + 1) % w_random.len();

            if let Some(count) = reporter.report() {
                let count_per_ns = count as f64 / dur_to_ns!(every) as f64;
                let count_per_s = count_per_ns * common::NANOS_PER_SEC as f64;
                println!("{:?} NEW: {:.2}", dur_to_ns!(start.elapsed()), count_per_s);
            }
        }
    });

    println!("Starting new read probe");

    // start reader that keeps probing new read view
    let r2 = thread::spawn(move || {
        let mut i = 0;
        let mut hits = 0;
        let mut reporter = Reporter::new(every);
        while start.elapsed() < runtime {
            match read_new(&r_random[i].into(), false) {
                Ok(ref rs) if !rs.is_empty() => {
                    hits += 1;
                }
                _ => {
                    // miss, or view not yet ready
                }
            }
            i = (i + 1) % r_random.len();

            if let Some(count) = reporter.report() {
                println!("{:?} HITF: {:.2}",
                         dur_to_ns!(start.elapsed()),
                         hits as f64 / count as f64);
                hits = 0;
            }
            thread::yield_now();
        }
    });

    println!("Waiting for experiment to end...");

    // everything finishes!
    w1.join().unwrap();
    w2.join().unwrap();
    r1.join().unwrap();
    r2.join().unwrap();

    println!("FIN");
}
