#[macro_use]
extern crate clap;

extern crate rand;
extern crate randomkit;

#[cfg(feature="b_postgresql")]
extern crate postgres;
#[cfg(feature="b_postgresql")]
extern crate r2d2;
#[cfg(feature="b_postgresql")]
extern crate r2d2_postgres;

extern crate distributary;

#[cfg(feature="b_netsoup")]
extern crate tarpc;

#[cfg(feature="b_memcached")]
extern crate memcache;

extern crate hdrsample;

extern crate spmc;

mod targets;
mod exercise;

use std::time;

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "\
EXAMPLES:
  vote soup://
  vote netsoup://127.0.0.1:7777
  vote memcached://127.0.0.1:11211
  vote postgresql://user@127.0.0.1/database";

fn main() {
    use clap::{Arg, App};
    let mut backends = vec!["soup"];
    if cfg!(feature = "b_postgresql") {
        backends.push("postgresql");
    }
    if cfg!(feature = "b_memcached") {
        backends.push("memcached");
    }
    if cfg!(feature = "b_netsoup") {
        backends.push("netsoup");
    }
    let backends = format!("Which database backend to use [{}]://<params>",
                           backends.join(", "));

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for different storage \
                backends.")
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
        .arg(Arg::with_name("distribution")
            .short("d")
            .long("distribution")
            .value_name("D")
            .possible_values(&["zipf", "uniform"])
            .default_value("zipf")
            .help("Vote to article distribution for reads and writes"))
        .arg(Arg::with_name("BACKEND")
            .index(1)
            .help(&backends)
            .required(true))
        .after_help(BENCH_USAGE)
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let dbn = args.value_of("BACKEND").unwrap();
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(|s| time::Duration::from_secs(s));
    let ngetters = value_t_or_exit!(args, "ngetters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    let distribution = args.value_of("distribution").unwrap();
    assert!(ngetters > 0);
    assert!(!dbn.is_empty());

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = exercise::RuntimeConfig::new(ngetters, narticles, runtime);
    config.produce_cdf(cdf);
    config.set_distribution(distribution.into());
    if stage {
        config.put_then_get();
    }
    if let Some(migrate_after) = migrate_after {
        config.perform_migration_at(migrate_after);
    }

    // setup db
    println!("Attempting to connect to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let (put_throughput, get_throughputs) = match dbn.next().unwrap() {
        // soup://
        "soup" => exercise::launch(targets::soup::make(dbn.next().unwrap(), ngetters), config),
        // postgresql://soup@127.0.0.1/bench_psql
        #[cfg(feature="b_postgresql")]
        "postgresql" => {
            exercise::launch(targets::postgres::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // memcached://127.0.0.1:11211
        #[cfg(feature="b_memcached")]
        "memcached" => {
            exercise::launch(targets::memcached::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // netsoup://127.0.0.1:7777
        #[cfg(feature="b_netsoup")]
        "netsoup" => {
            exercise::launch(targets::netsoup::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // garbage
        t => {
            panic!("backend not supported -- make sure you compiled with --features b_{}",
                   t)
        }
    };

    let sum: f64 = put_throughput.iter().sum();
    println!("avg PUT: {:.2}", sum / put_throughput.len() as f64);

    if avg {
        let mut thread_avgs = Vec::new();
        for (i, v) in get_throughputs.iter().enumerate() {
            let sum: f64 = v.iter().sum();
            let avg: f64 = sum / v.len() as f64;
            println!("avg GET{}: {:.2}", i, avg);
            thread_avgs.push(avg);
        }
        let sum: f64 = thread_avgs.iter().sum();
        println!("avg GET: {:.2}", sum / thread_avgs.len() as f64);
    }
}
