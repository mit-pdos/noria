#[macro_use]
extern crate clap;
extern crate distributary;
extern crate rand;
extern crate zipf;

mod graph;

#[macro_use]
#[allow(dead_code)]
mod common;

use zipf::ZipfDistribution;
use std::thread;
use std::time;

fn randomness(distribution: common::Distribution, range: usize, n: usize) -> Vec<i64> {
    use rand::Rng;

    // random article ids with distribution. we pre-generate these to avoid overhead at
    // runtime. note that we don't use Iterator::cycle, since it buffers by cloning, which
    // means it might also do vector resizing.
    println!(
        "Generating ~{}M random numbers; this'll take a few seconds...",
        n / 1_000_000
    );
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

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about(
            "Benchmarks user-curated news aggregator throughput for in-memory Soup",
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .takes_value(true)
                .required(true)
                .default_value("uniform")
                .help(
                    "run benchmark with the given article id distribution [uniform|zipf:exponent]",
                ),
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
            Arg::with_name("nvotes")
                .short("v")
                .long("votes")
                .default_value("1000000")
                .help("Number of votes to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("reads")
                .short("r")
                .long("reads")
                .value_name("N")
                .default_value("20000")
                .help("Number of reads to perform"),
        )
        .arg(
            Arg::with_name("stupid")
                .long("stupid")
                .help("Make the migration stupid"),
        )
        .arg(
            Arg::with_name("unsharded")
                .long("unsharded")
                .help("Run without sharding"),
        )
        .arg(
            Arg::with_name("full")
                .long("full")
                .help("Disable partial materialization"),
        )
        .get_matches();

    let dist = value_t_or_exit!(args, "distribution", common::Distribution);
    let narticles = value_t_or_exit!(args, "narticles", usize);
    let nvotes = value_t_or_exit!(args, "nvotes", usize);
    let reads = value_t_or_exit!(args, "reads", usize);
    assert!(reads <= narticles);

    // default persistence (memory only)
    let mut persistence_params = distributary::PersistenceParameters::default();
    persistence_params.queue_capacity = 1;
    persistence_params.mode = distributary::DurabilityMode::MemoryOnly;

    // setup db
    let mut g = graph::make(false, false, persistence_params);

    if args.is_present("full") {
        // it's okay to change this here, since it only matters for migration
        g.graph.disable_partial();
    }

    if args.is_present("unsharded") {
        // it's okay to change this here, since it only matters for migration
        g.graph.disable_sharding();
    }

    // we need a putter and a getter
    let mut articles = g.graph.get_mutator(g.article);
    let mut votes = g.graph.get_mutator(g.vote);

    // prepopulate
    println!("Prepopulating with {} articles", narticles);
    for i in 0..(narticles as i64) {
        articles
            .put(vec![i.into(), format!("Article #{}", i).into()])
            .unwrap();
    }
    println!("Prepopulating with {} old votes", nvotes);
    let random = randomness(dist, narticles, nvotes);
    for i in 0..nvotes {
        votes.put(vec![0.into(), random[i].into()]).unwrap();
    }

    // migrate
    println!("Migrating...");
    let (ratings, read_new) = g.transition(args.is_present("stupid"), false);
    let mut ratings = g.graph.get_mutator(ratings);
    let read_new = g.graph.get_getter(read_new).unwrap();

    // prepopulate new ratings
    println!("Prepopulating with {} new votes", nvotes);
    let random = randomness(dist, narticles, nvotes);
    for i in 0..nvotes {
        ratings
            .put(vec![0.into(), random[i].into(), 5.into()])
            .unwrap();
    }

    // now we're going to do a fixed number of reads,
    // and wait until they've all completd
    println!("Doing {} reads", reads);
    let random = randomness(dist, narticles, reads);
    let mut start = None;
    for i in 0..reads {
        loop {
            match read_new.lookup(&random[i].into(), false) {
                Ok(_) => {
                    // we may be requesting duplicate keys!
                    // assert!(rs.is_empty());
                    if start.is_none() {
                        // if this is first read when view is ready, start timing!
                        start = Some(time::Instant::now());
                    }
                    break;
                }
                Err(_) => {
                    // view not yet ready, retry
                    thread::sleep(time::Duration::from_millis(1));
                }
            }
        }
    }
    // now we want to wait until all the replays have completed without triggering extra replays.
    // we can't quite do that perfectly, but we'll emulate it by doing blocking reads. by the time
    // one blocking read completes, we can expect that a bunch of other ones have also completed.
    for i in 0..reads {
        match read_new.lookup(&random[i].into(), true) {
            Ok(_) => {}
            Err(_) => unreachable!(),
        }
    }

    let ns = dur_to_ns!(start.unwrap().elapsed());
    println!("TOTAL: {} ns", ns);
    println!("OPSS: {:.2}", reads as f64 / (ns as f64 / 1_000_000_000f64));

    println!("FIN");
}
