#[macro_use]
extern crate clap;
extern crate distributary;
extern crate glob;
extern crate rand;

mod graph;

#[macro_use]
#[allow(dead_code)]
mod common;

use std::u64;
use std::thread;
use std::time;
use std::io::{self, Read};
use std::fs;

fn randomness(range: usize, n: usize) -> Vec<i64> {
    use rand::Rng;
    let mut u = rand::thread_rng();
    (0..n)
        .map(|_| u.gen_range(0, range as i64) as i64)
        .collect()
}

// Block until writes have finished.
// This performs a read and checks that the result corresponds to the total amount of votes.
// TODO(ekmartin): Would be nice if there's a way we can do this without having to poll
// Soup for results.
fn wait_for_writes(mut getter: distributary::RemoteGetter, narticles: usize, nvotes: usize) {
    loop {
        let keys = (0..narticles as i64).map(|i| i.into()).collect();
        let rows = getter.multi_lookup(keys, true);
        let sum: i64 = rows.into_iter()
            .map(|result| {
                let row = result.unwrap();
                if row.is_empty() {
                    return 0;
                }

                match row[0][2] {
                    distributary::DataType::None => 0,
                    distributary::DataType::BigInt(i) => i,
                    distributary::DataType::Int(i) => i as i64,
                    _ => unreachable!(),
                }
            })
            .sum();

        if sum == nvotes as i64 {
            return;
        }

        thread::sleep(time::Duration::from_millis(50));
    }
}

fn retrieve_snapshot_id() -> Option<u64> {
    let filename = "vote-recovery-snapshot_id";
    let mut file = match fs::File::open(&filename) {
        Ok(f) => f,
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            return None;
        }
        Err(e) => panic!("Could not open snapshot_id file {}: {}", filename, e),
    };

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)
        .expect("Failed reading snapshot_id");
    buffer.parse::<u64>().ok()
}

fn pre_recovery(
    s: graph::Setup,
    persistence_params: distributary::PersistenceParameters,
    random: Vec<i64>,
    narticles: usize,
    nvotes: usize,
    snapshot: bool,
    verbose: bool,
) {
    let mut g = graph::make(s, persistence_params.clone());
    let mut articles = g.graph.get_mutator(g.article).unwrap();
    let mut votes = g.graph.get_mutator(g.vote).unwrap();
    let getter = g.graph.get_getter(g.end).unwrap();

    // prepopulate
    if verbose {
        eprintln!("Populating with {} articles", narticles);
    }

    for i in 0..(narticles as i64) {
        articles
            .put(vec![i.into(), format!("Article #{}", i).into()])
            .unwrap();
    }

    if verbose {
        eprintln!("Populating with {} votes", nvotes);
    }

    for i in 0..nvotes {
        votes.put(vec![random[i].into(), i.into()]).unwrap();
    }

    thread::sleep(time::Duration::from_secs(1));
    wait_for_writes(getter, narticles, nvotes);
    if snapshot {
        g.graph.initialize_snapshot();

        // Wait for the domains to send ACKs to the controller:
        loop {
            let id = retrieve_snapshot_id();
            if id.is_some() {
                if verbose {
                    eprintln!("Completed snapshot with ID {}", id.unwrap());
                }

                break;
            }

            thread::sleep(time::Duration::from_millis(500));
        }
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks the recovery time of a user-curated news aggregator")
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
            Arg::with_name("keep_files")
                .long("keep-files")
                .help("Don't remove snapshots and log files."),
        )
        .arg(
            Arg::with_name("snapshot")
                .long("snapshot")
                .help("Snapshot only recovery, no replaying of logs."),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("2")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(Arg::with_name("quiet").long("quiet").short("q"))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let nvotes = value_t_or_exit!(args, "nvotes", usize);
    let verbose = !args.is_present("quiet");
    let snapshot = args.is_present("snapshot");
    let keep_files = args.is_present("keep_files");
    let durability_mode = if snapshot {
        distributary::DurabilityMode::MemoryOnly
    } else {
        distributary::DurabilityMode::Permanent
    };

    let persistence_params = distributary::PersistenceParameters::new(
        durability_mode,
        512,
        time::Duration::from_millis(10),
        // TODO(ekmartin): This will ensure that a snapshot listener is spawned, but it'll also
        // spawn a useless initialize_snapshots thread that sleeps forever. We might want to add a
        // `enable_snapshot_listener` override or something to builder instead, for this and the
        // tests.
        Some(time::Duration::from_secs(u64::MAX)),
        Some(String::from("vote-recovery")),
    );

    let mut s = graph::Setup::new(true, 2);
    s.logging = verbose;
    s.sharding = match value_t_or_exit!(args, "shards", usize) {
        0 => None,
        x => Some(x),
    };

    // Prepopulate with narticles and nvotes:
    let random = randomness(narticles, nvotes);
    pre_recovery(
        s.clone(),
        persistence_params.clone(),
        random.clone(),
        narticles,
        nvotes,
        snapshot,
        verbose,
    );

    if verbose {
        eprintln!("Done populating state, now recovering...");
    }

    let mut g = graph::make(s, persistence_params);
    let getter = g.graph.get_getter(g.end).unwrap();

    let start = time::Instant::now();
    g.graph.recover();
    let initial = dur_to_ns!(start.elapsed()) as f64 / 1_000_000f64;
    println!("Initial Recovery Time (ms): {}", initial);
    wait_for_writes(getter, narticles, nvotes);
    let total = dur_to_ns!(start.elapsed()) as f64 / 1_000_000f64;
    println!("Total Recovery Time (ms): {}", total);

    if !keep_files {
        // Clean up log files, snapshots and the snapshot_id counter:
        for path in glob::glob("./vote-recovery-*").unwrap() {
            fs::remove_file(path.unwrap()).unwrap();
        }
    }
}
