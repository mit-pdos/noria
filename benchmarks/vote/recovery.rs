#[macro_use]
extern crate clap;
extern crate distributary;
extern crate futures;
extern crate futures_state_stream;
extern crate glob;
extern crate hdrsample;
extern crate hwloc;
extern crate libc;
extern crate memcached;
extern crate mysql;
extern crate rand;
extern crate rayon;
extern crate tiberius;
extern crate tokio_core;

mod clients;

use std::u64;
use std::thread;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs;
use clients::localsoup::graph::RECIPE;
use distributary::{ControllerBuilder, ControllerHandle, NodeIndex, PersistenceParameters,
                   ZookeeperAuthority};

fn get_name() -> String {
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!(
        "vote-recovery-{}-{}",
        current_time.as_secs(),
        current_time.subsec_nanos()
    )
}

fn randomness(range: usize, n: usize) -> Vec<i64> {
    use rand::Rng;
    let mut u = rand::thread_rng();
    (0..n)
        .map(|_| u.gen_range(0, range as i64) as i64)
        .collect()
}

macro_rules! dur_to_millis {
    ($d:expr) => {{
        $d.as_secs() * 1_000 + $d.subsec_nanos() as u64 / 1_000_000
    }}
}

#[derive(Clone)]
struct Setup {
    pub sharding: Option<usize>,
    pub logging: bool,
    pub persistence_params: PersistenceParameters,
}

impl Setup {
    pub fn new(persistence_params: PersistenceParameters) -> Self {
        Setup {
            sharding: None,
            logging: false,
            persistence_params,
        }
    }
}

struct Graph {
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: ControllerHandle<ZookeeperAuthority>,
}

fn make(s: Setup, authority: ZookeeperAuthority) -> Graph {
    let mut g = ControllerBuilder::default();
    g.set_sharding(s.sharding);
    g.set_persistence(s.persistence_params.clone());
    g.set_local_workers(2);
    g.set_local_read_threads(1);
    if s.logging {
        g.log_with(distributary::logger_pls());
    }

    let mut graph = g.build(authority);
    graph.install_recipe(RECIPE.to_owned()).unwrap();
    let inputs = graph.inputs();
    let outputs = graph.outputs();

    if s.logging {
        println!("inputs {:?}", inputs);
        println!("outputs {:?}", outputs);
    }

    Graph {
        vote: inputs["Vote"],
        article: inputs["Article"],
        end: outputs["ArticleWithVoteCount"],
        graph,
    }
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

        thread::sleep(Duration::from_millis(50));
    }
}

fn pre_recovery(
    s: Setup,
    random: Vec<i64>,
    narticles: usize,
    nvotes: usize,
    snapshot: bool,
    verbose: bool,
) {
    let authority = ZookeeperAuthority::new(&format!(
        "127.0.0.1:2181/{}",
        s.persistence_params.log_prefix
    ));
    let mut g = make(s, authority);
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

    thread::sleep(Duration::from_secs(1));
    wait_for_writes(getter, narticles, nvotes);
    if snapshot {
        g.graph.initialize_snapshot();
        // Wait for the domains to send ACKs to the controller:
        loop {
            if g.graph.get_snapshot_id() > 0 {
                break
            }

            thread::sleep(Duration::from_millis(100));
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
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let nvotes = value_t_or_exit!(args, "nvotes", usize);
    let verbose = args.is_present("verbose");
    let snapshot = args.is_present("snapshot");
    let keep_files = args.is_present("keep_files");
    let durability_mode = if snapshot {
        distributary::DurabilityMode::MemoryOnly
    } else {
        distributary::DurabilityMode::Permanent
    };

    let name = get_name();
    let persistence_params = distributary::PersistenceParameters::new(
        durability_mode,
        512,
        Duration::from_millis(10),
        // TODO(ekmartin): This will ensure that a snapshot listener is spawned, but it'll also
        // spawn a useless initialize_snapshots thread that sleeps forever. We might want to add a
        // `enable_snapshot_listener` override or something to builder instead, for this and the
        // tests.
        Some(Duration::from_secs(u64::MAX)),
        Some(name.clone()),
    );

    let mut s = Setup::new(persistence_params);
    s.logging = verbose;
    s.sharding = match value_t_or_exit!(args, "shards", usize) {
        0 => None,
        x => Some(x),
    };

    // Prepopulate with narticles and nvotes:
    let random = randomness(narticles, nvotes);
    pre_recovery(s.clone(), random, narticles, nvotes, snapshot, verbose);

    if verbose {
        eprintln!("Done populating state, now recovering...");
    }

    let authority = ZookeeperAuthority::new(&format!("127.0.0.1:2181/{}", name));
    let mut g = make(s, authority);
    let getter = g.graph.get_getter(g.end).unwrap();

    let start = Instant::now();
    g.graph.recover();
    let initial = dur_to_millis!(start.elapsed());
    println!("Initial Recovery Time (ms): {}", initial);
    wait_for_writes(getter, narticles, nvotes);
    let total = dur_to_millis!(start.elapsed());
    println!("Total Recovery Time (ms): {}", total);

    if !keep_files {
        // Clean up log files and snapshots:
        for path in glob::glob(&format!("./{}-*", name)).unwrap() {
            fs::remove_file(path.unwrap()).unwrap();
        }
    }
}
