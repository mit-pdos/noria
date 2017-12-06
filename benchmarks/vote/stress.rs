#[macro_use]
extern crate clap;
extern crate distributary;
extern crate rand;

mod graph;

#[macro_use]
#[allow(dead_code)]
mod common;

use std::thread;
use std::time;
use std::sync::{Arc, Mutex};

fn randomness(range: usize, n: usize) -> Vec<i64> {
    use rand::Rng;
    let mut u = rand::thread_rng();
    (0..n)
        .map(|_| u.gen_range(0, range as i64) as i64)
        .collect()
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
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .default_value("10")
                .help("Read until all keys have been read, or this time has elapsed"),
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
        .arg(Arg::with_name("quiet").long("quiet").short("q"))
        .arg(
            Arg::with_name("max_concurrent")
                .long("concurrent")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("replay_batch_size")
                .long("replay_size")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("replay_batch_timeout")
                .long("replay_timeout")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("full")
                .long("full")
                .help("Disable partial materialization"),
        )
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let nvotes = value_t_or_exit!(args, "nvotes", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));

    // config options
    let concurrent_replays = args.value_of("max_concurrent")
        .map(|_| value_t_or_exit!(args, "max_concurrent", usize));
    let replay_size = args.value_of("replay_batch_size")
        .map(|_| value_t_or_exit!(args, "replay_batch_size", usize));
    let replay_timeout = args.value_of("replay_batch_timeout")
        .map(|_| value_t_or_exit!(args, "replay_batch_timeout", u64))
        .map(time::Duration::from_millis);

    // default persistence (memory only)
    let mut persistence_params = distributary::PersistenceParameters::default();
    persistence_params.queue_capacity = 1;
    persistence_params.mode = distributary::DurabilityMode::MemoryOnly;

    // setup db
    let mut s = graph::Setup::new(true, 2);
    s.stupid = args.is_present("stupid");
    s.partial = !args.is_present("full");
    s.sharding = !args.is_present("unsharded");
    if let Some(n) = concurrent_replays {
        s = s.set_max_concurrent_replay(n);
    }
    if let Some(n) = replay_size {
        s = s.set_partial_replay_batch_size(n);
    }
    if let Some(t) = replay_timeout {
        s = s.set_partial_replay_batch_timeout(t);
    }

    let mut g = graph::make(s, persistence_params);

    let (mut articles, mut votes) = {
        // we need putters
        (g.graph.get_mutator(g.article).unwrap(), g.graph.get_mutator(g.vote).unwrap())
    };

    // prepopulate
    if !args.is_present("quiet") {
        println!("Prepopulating with {} articles", narticles);
    }
    for i in 0..(narticles as i64) {
        articles
            .put(vec![i.into(), format!("Article #{}", i).into()])
            .unwrap();
    }
    if !args.is_present("quiet") {
        println!("Prepopulating with {} old votes", nvotes);
    }
    let random = randomness(narticles, nvotes);
    for i in 0..nvotes {
        votes.put(vec![0.into(), random[i].into()]).unwrap();
    }

    // migrate
    if !args.is_present("quiet") {
        println!("Migrating...");
    }
    let (ratings, read_new) = g.transition();
    let (mut ratings, mut read_new) = {
        (g.graph.get_mutator(ratings), g.graph.get_getter(read_new).unwrap())
    };

    // prepopulate new ratings
    if !args.is_present("quiet") {
        println!("Prepopulating with {} new votes", nvotes);
    }
    let random = randomness(narticles, nvotes);
    for i in 0..nvotes {
        ratings
            .as_mut()
            .unwrap()
            .multi_put(vec![vec![0.into(), random[i].into(), 5.into()]])
            .unwrap();
    }

    if !args.is_present("quiet") {
        println!("Reading for {}s", runtime.as_secs());
    }

    // kick off all the replays
    // TODO: send these in smaller batches?
    let mut start = None;
    for i in 0..(narticles as i64) {
        loop {
            match read_new.lookup(&i.into(), false) {
                Ok(ref rs) => {
                    // we know we're not requesting duplicate keys, and we haven't requested
                    // this key before, so:
                    assert!(rs.is_empty());
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
    let start = start.unwrap();

    // we now want to wait until either all keys have been backfilled, or until the time runs out,
    // whichever comes first. we do that by looping on reading the *last* value we requested a
    // replay for (which should thus be populated last). if we read successfully, we break the
    // loop, otherwise we just wait until the time runs out.
    let mut reads = narticles;
    for i in 0..(narticles as i64) {
        match read_new.lookup(&i.into(), true) {
            Ok(_) => {}
            Err(_) => unreachable!(),
        }

        if start.elapsed() >= runtime {
            reads = i as usize + 1;
            break;
        }
    }

    let s = dur_to_ns!(start.elapsed()) as f64 / 1_000_000_000f64;
    println!("RATE: {:.2}", reads as f64 / s);
}
