#[macro_use]
extern crate clap;
extern crate distributary;
extern crate futures;
extern crate futures_state_stream;
extern crate hdrhistogram;
extern crate hwloc;
extern crate itertools;
extern crate libc;
extern crate memcached;
extern crate mysql;
extern crate rand;
extern crate rayon;
extern crate tiberius;
extern crate tokio_core;

mod clients;

use std::thread;
use std::time::{Duration, Instant};
use std::path::PathBuf;

use clap::{App, Arg};
use hdrhistogram::Histogram;
use itertools::Itertools;

use clients::localsoup::graph;
use distributary::{DataType, DurabilityMode, PersistenceParameters};

// If we .batch_put a huge amount of rows we'll end up with a deadlock when the base
// domains fill up their TCP buffers trying to send ACKs (which the batch putter
// isn't reading yet, since it's still busy sending).
const BATCH_SIZE: usize = 10000;

fn randomness(range: i64, n: i64) -> Vec<i64> {
    use rand::Rng;
    let mut u = rand::thread_rng();
    (0..n).map(|_| u.gen_range(0, range) as i64).collect()
}

fn populate(g: &mut graph::Graph, narticles: i64, nvotes: i64, verbose: bool) {
    let random = randomness(narticles, nvotes);
    let mut articles = g.graph.get_mutator("Article").unwrap();
    let mut votes = g.graph.get_mutator("Vote").unwrap();

    // prepopulate
    if verbose {
        eprintln!("Populating with {} articles", narticles);
    }

    (0..narticles)
        .map(|i| {
            let article: Vec<DataType> = vec![i.into(), format!("Article #{}", i).into()];
            article
        })
        .chunks(BATCH_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let rs: Vec<Vec<DataType>> = chunk.collect();
            articles.multi_put(rs).unwrap();
        });

    if verbose {
        eprintln!("Populating with {} votes", nvotes);
    }

    (0..nvotes)
        .map(|i| {
            let vote: Vec<DataType> = vec![random[i as usize].into(), i.into()];
            vote
        })
        .chunks(BATCH_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let rs: Vec<Vec<DataType>> = chunk.collect();
            votes.multi_put(rs).unwrap();
        });

    thread::sleep(Duration::from_secs(5));
}

fn perform_reads(g: &mut graph::Graph, narticles: i64, nvotes: i64) {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap();
    let mut getter = g.graph.get_getter("ArticleWithVoteCount").unwrap();
    let mut sum = 0;

    // Synchronously read each article once, which should trigger a full replay from the base.
    for i in 0..narticles {
        let start = Instant::now();
        let rs = getter.lookup(&i.into(), true).unwrap();
        let elapsed = start.elapsed();
        let us = elapsed.as_secs() * 1_000_000 + elapsed.subsec_nanos() as u64 / 1_000;
        assert_eq!(rs.len(), 1);
        assert_eq!(DataType::BigInt(i), rs[0][0]);
        sum += match rs[0][2] {
            DataType::Int(votes) => votes as i64,
            DataType::BigInt(votes) => votes,
            DataType::None => 0,
            _ => unreachable!(),
        };

        if hist.record(us).is_err() {
            let m = hist.high();
            hist.record(m).unwrap();
        }
    }

    assert_eq!(nvotes, sum);
    println!("# articles: {}, votes: {}", narticles, nvotes);
    println!("read\t50\t{:.2}\t(all µs)", hist.value_at_quantile(0.5));
    println!("read\t95\t{:.2}\t(all µs)", hist.value_at_quantile(0.95));
    println!("read\t99\t{:.2}\t(all µs)", hist.value_at_quantile(0.99));
    println!("read\t100\t{:.2}\t(all µs)", hist.max());
}

fn main() {
    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks the latency of full replays in a user-curated news aggregator")
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
            Arg::with_name("persist-bases")
                .long("persist-bases")
                .takes_value(false)
                .help("Persist base nodes to disk."),
        )
        .arg(
            Arg::with_name("log-dir")
                .long("log-dir")
                .takes_value(true)
                .help("Absolute path to the directory where the log files will be written."),
        )
        .arg(
            Arg::with_name("retain-logs-on-exit")
                .long("retain-logs-on-exit")
                .takes_value(false)
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
            Arg::with_name("flush-timeout")
                .long("flush-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to wait before processing a merged packet, in nanoseconds."),
        )
        .arg(
            Arg::with_name("persistence-threads")
                .long("persistence-threads")
                .takes_value(true)
                .default_value("1")
                .help("Number of background threads used by PersistentState."),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("0")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", i64);
    let nvotes = value_t_or_exit!(args, "nvotes", i64);
    let verbose = args.is_present("verbose");
    let flush_ns = value_t_or_exit!(args, "flush-timeout", u32);

    let mut persistence = PersistenceParameters::default();
    persistence.flush_timeout = Duration::new(0, flush_ns);
    persistence.persistence_threads = value_t_or_exit!(args, "persistence-threads", i32);
    persistence.queue_capacity = value_t_or_exit!(args, "write-batch-size", usize);
    persistence.persist_base_nodes = args.is_present("persist-bases");
    persistence.log_prefix = "vote-replay".to_string();
    persistence.log_dir = args.value_of("log-dir")
        .and_then(|p| Some(PathBuf::from(p)));
    persistence.mode = if args.is_present("retain-logs-on-exit") {
        DurabilityMode::Permanent
    } else {
        DurabilityMode::DeleteOnExit
    };

    let mut s = graph::Setup::default();
    s.logging = verbose;
    s.nworkers = 2;
    s.nreaders = 1;
    s.sharding = match value_t_or_exit!(args, "shards", usize) {
        0 => None,
        x => Some(x),
    };

    let mut g = graph::make(s, persistence);
    // Prepopulate with narticles and nvotes:
    populate(&mut g, narticles, nvotes, verbose);

    if verbose {
        eprintln!("Done populating state, now reading articles...");
    }

    perform_reads(&mut g, narticles, nvotes);
}
