#![cfg_attr(feature="b_netsoup", feature(conservative_impl_trait, plugin))]
#![cfg_attr(feature="b_netsoup", plugin(tarpc_plugins))]

#[macro_use]
extern crate clap;

extern crate slog;
extern crate slog_term;

extern crate rand;

#[cfg(any(feature="b_mssql", feature="b_netsoup"))]
extern crate futures;
#[cfg(any(feature="b_mssql", feature="b_netsoup"))]
extern crate tokio_core;

#[cfg(feature="b_mssql")]
extern crate futures_state_stream;
#[cfg(feature="b_mssql")]
extern crate tiberius;

#[cfg(any(feature="b_mysql", feature="b_hybrid"))]
#[macro_use]
extern crate mysql;

#[cfg(feature="b_postgresql")]
extern crate postgres;

extern crate distributary;

#[cfg(feature="b_netsoup")]
extern crate tarpc;

#[cfg(any(feature="b_memcached", feature="b_hybrid"))]
extern crate memcached;

extern crate hdrsample;
extern crate zipf;

extern crate spmc;

mod clients;
mod common;
mod exercise;

use std::time;

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "\
EXAMPLES:
  vote-client [read|write] netsoup://127.0.0.1:7777
  vote-client [read|write] memcached://127.0.0.1:11211
  vote-client [read|write] mssql://server=tcp:127.0.0.1,1433;username=user;pwd=pwd;/database
  vote-client [read|write] mysql://user@127.0.0.1/database
  vote-client [read|write] postgresql://user@127.0.0.1/database
  vote-client [read|write] hybrid://mysql=user@127.0.0.1/database,memcached=127.0.0.1:11211";

fn main() {
    use clap::{Arg, App};
    let mut backends = vec![];
    if cfg!(feature = "b_mssql") {
        backends.push("mssql");
    }
    if cfg!(feature = "b_mysql") {
        backends.push("mysql");
    }
    if cfg!(feature = "b_postgresql") {
        backends.push("postgresql");
    }
    if cfg!(feature = "b_memcached") {
        backends.push("memcached");
    }
    if cfg!(feature = "b_netsoup") {
        backends.push("netsoup");
    }
    if cfg!(feature = "b_hybrid") {
        backends.push("hybrid");
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
        .arg(Arg::with_name("narticles")
            .short("a")
            .long("articles")
            .value_name("N")
            .default_value("100000")
            .help("Number of articles to prepopulate the database with"))
        .arg(Arg::with_name("distribution")
            .short("d")
            .takes_value(true)
            .default_value("uniform")
            .help("run benchmark with the given article id distribution [uniform|zipf:exponent]"))
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
        .arg(Arg::with_name("MODE")
            .index(1)
            .possible_values(&["read", "write"])
            .help("Mode to run this client in")
            .required(true))
        .arg(Arg::with_name("BACKEND")
            .index(2)
            .help(&backends)
            .required(true))
        .after_help(BENCH_USAGE)
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let dist = value_t_or_exit!(args, "distribution", exercise::Distribution);
    let mode = args.value_of("MODE").unwrap();
    let dbn = args.value_of("BACKEND").unwrap();
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    assert!(!dbn.is_empty());

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = exercise::RuntimeConfig::new(narticles, runtime);
    config.produce_cdf(cdf);
    if let Some(migrate_after) = migrate_after {
        config.perform_migration_at(migrate_after);
    }
    config.use_distribution(dist);

    // setup db
    println!("Attempting to connect to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let client = dbn.next().unwrap();
    let addr = dbn.next().unwrap();
    match mode {
        "read" => {
            let stats = match client {
                // mssql://server=tcp:127.0.0.1,1433;user=user;pwd=password/bench_mssql
                #[cfg(feature="b_mssql")]
                "mssql" => exercise::launch_reader(clients::mssql::make_reader(addr), config),
                // mysql://soup@127.0.0.1/bench_mysql
                #[cfg(feature="b_mysql")]
                "mysql" => {
                    let c = clients::mysql::setup(addr, false);
                    exercise::launch_reader(clients::mysql::make_reader(&c), config)
                }
                // hybrid://mysql=soup@127.0.0.1/bench_mysql,memcached=127.0.0.1:11211
                #[cfg(feature="b_hybrid")]
                "hybrid" => {
                    let mut split_dbn = addr.splitn(2, ",");
                    let mysql_dbn = &split_dbn.next().unwrap()[6..];
                    let memcached_dbn = &split_dbn.next().unwrap()[10..];
                    let mut c = clients::hybrid::setup(mysql_dbn, memcached_dbn, false);
                    exercise::launch_reader(clients::hybrid::make_reader(&mut c), config)
                }
                // postgresql://soup@127.0.0.1/bench_psql
                #[cfg(feature="b_postgresql")]
                "postgresql" => {
                    let c = clients::postgres::setup(addr, false);
                    let res = exercise::launch_reader(clients::postgres::make_reader(&c), config);
                    drop(c);
                    res
                }
                // memcached://127.0.0.1:11211
                #[cfg(feature="b_memcached")]
                "memcached" => {
                    exercise::launch_reader(clients::memcached::make_reader(addr), config)
                }
                // netsoup://127.0.0.1:7777
                #[cfg(feature="b_netsoup")]
                "netsoup" => exercise::launch_reader(clients::netsoup::make_reader(addr), config),
                // garbage
                t => {
                    panic!("backend not supported -- make sure you compiled with --features b_{}",
                           t)
                }
            };
            print_stats("GET", &stats.pre, avg);
            print_stats("GET+", &stats.post, avg);
        }
        "write" => {
            let stats = match client {
                // mssql://server=tcp:127.0.0.1,1433;user=user;pwd=password/bench_mssql
                #[cfg(feature="b_mssql")]
                "mssql" => exercise::launch_writer(clients::mssql::make_writer(addr), config, None),
                // mysql://soup@127.0.0.1/bench_mysql
                #[cfg(feature="b_mysql")]
                "mysql" => {
                    let c = clients::mysql::setup(addr, true);
                    exercise::launch_writer(clients::mysql::make_writer(&c), config, None)
                }
                // hybrid://mysql=soup@127.0.0.1/bench_mysql,memcached=127.0.0.1:11211
                #[cfg(feature="b_hybrid")]
                "hybrid" => {
                    let mut split_dbn = addr.splitn(2, ",");
                    let mysql_dbn = &split_dbn.next().unwrap()[6..];
                    let memcached_dbn = &split_dbn.next().unwrap()[10..];
                    let mut c = clients::hybrid::setup(mysql_dbn, memcached_dbn, true);
                    exercise::launch_writer(clients::hybrid::make_writer(&mut c), config, None)
                }
                // postgresql://soup@127.0.0.1/bench_psql
                #[cfg(feature="b_postgresql")]
                "postgresql" => {
                    let c = clients::postgres::setup(addr, true);
                    let res =
                        exercise::launch_writer(clients::postgres::make_writer(&c), config, None);
                    drop(c);
                    res
                }
                // memcached://127.0.0.1:11211
                #[cfg(feature="b_memcached")]
                "memcached" => {
                    exercise::launch_writer(clients::memcached::make_writer(addr), config, None)
                }
                // netsoup://127.0.0.1:7777
                #[cfg(feature="b_netsoup")]
                "netsoup" => {
                    exercise::launch_writer(clients::netsoup::make_writer(addr), config, None)
                }
                // garbage
                t => {
                    panic!("backend not supported -- make sure you compiled with --features b_{}",
                           t)
                }
            };
            print_stats("PUT", &stats.pre, avg);
            print_stats("PUT+", &stats.post, avg);
        }
        _ => unreachable!(),
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
