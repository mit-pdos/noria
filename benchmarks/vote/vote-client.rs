#[macro_use]
extern crate clap;

extern crate rand;

#[cfg(feature="b_mssql")]
extern crate futures;
#[cfg(feature="b_mssql")]
extern crate tokio_core;

#[cfg(feature="b_mssql")]
extern crate futures_state_stream;
#[cfg(feature="b_mssql")]
extern crate tiberius;

#[cfg(any(feature="b_mysql", feature="b_hybrid"))]
extern crate mysql;

extern crate distributary;

#[cfg(feature="b_netsoup")]
extern crate bincode;
#[cfg(feature="b_netsoup")]
extern crate bufstream;
#[cfg(feature="b_netsoup")]
extern crate net2;

#[cfg(any(feature="b_memcached", feature="b_hybrid"))]
extern crate memcached;

extern crate hdrsample;
extern crate zipf;

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
        .arg(Arg::with_name("reuse")
            .long("reuse")
            .takes_value(false)
            .help("do not prepopulate"))
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
        .arg(Arg::with_name("bind")
            .short("B")
            .takes_value(true)
            .help("bind to the given local address when possible"))
        .arg(Arg::with_name("runtime")
            .short("r")
            .long("runtime")
            .value_name("N")
            .default_value("60")
            .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("batch")
            .short("b")
            .takes_value(true)
            .default_value("1")
            .help("Number of operations per batch (if supported)"))
        .arg(Arg::with_name("quiet")
            .short("q")
            .long("quiet")
            .help("No noisy output while running"))
        .arg(Arg::with_name("MODE")
            .index(1)
            .help("Mode to run this client in [read|write|mix:rw_ratio]")
            .required(true))
        .arg(Arg::with_name("BACKEND")
            .index(2)
            .help(&backends)
            .required(true))
        .after_help(BENCH_USAGE)
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let dist = value_t_or_exit!(args, "distribution", common::Distribution);
    let mode = args.value_of("MODE").unwrap();
    let dbn = args.value_of("BACKEND").unwrap();
    let runtime = value_t_or_exit!(args, "runtime", u64);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    assert!(!dbn.is_empty());

    let batch_size = value_t_or_exit!(args, "batch", usize);
    let mix = match mode {
        "read" => common::Mix::Read(batch_size),
        "write" => common::Mix::Write(batch_size),
        mode if mode.starts_with("mix:") => {
            // ratio is number of reads per write
            let ratio = mode.split(':')
                .skip(1)
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap();
            // we want batch size of reads to be batch_size
            // what should batch size for writes be to get this ratio?
            if batch_size % ratio != 0 {
                println!("mix ratio does not evenly divide batch size");
                return;
            }
            common::Mix::RW(batch_size, batch_size / ratio)
        }
        _ => unreachable!(),
    };

    let runtime = if runtime == 0 { None } else { Some(runtime) };
    let runtime = runtime.map(time::Duration::from_secs);
    let mut config = common::RuntimeConfig::new(narticles, mix, runtime);
    config.produce_cdf(cdf);
    config.set_reuse(args.is_present("reuse"));
    config.set_verbose(!args.is_present("quiet"));
    config.use_distribution(dist);

    // setup db
    println!("Attempting to connect to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let client = dbn.next().unwrap();
    let addr = dbn.next().unwrap();

    if let Some(addr) = args.value_of("bind") {
        if client != "netsoup" {
            unimplemented!();
        }
        config.prefer_addr(addr);
    }

    let cfg = config.clone();
    let stats = match client {
        // mssql://server=tcp:127.0.0.1,1433;user=user;pwd=password/bench_mssql
        #[cfg(feature="b_mssql")]
        "mssql" => {
            let c = clients::mssql::make(addr, &config);
            exercise::launch_mix(c, config)
        }
        // mysql://soup@127.0.0.1/bench_mysql
        #[cfg(feature="b_mysql")]
        "mysql" => {
            let c = clients::mysql::setup(addr, &config);
            let c = clients::mysql::make(&c, &config);
            exercise::launch_mix(c, config)
        }
        // hybrid://mysql=soup@127.0.0.1/bench_mysql,memcached=127.0.0.1:11211
        #[cfg(feature="b_hybrid")]
        "hybrid" => {
            let mut split_dbn = addr.splitn(2, ",");
            let mysql_dbn = &split_dbn.next().unwrap()[6..];
            let memcached_dbn = &split_dbn.next().unwrap()[10..];
            let mut c = clients::hybrid::setup(mysql_dbn, memcached_dbn, &config);
            let c = clients::hybrid::make(&mut c);
            exercise::launch_mix(c, config)
        }
        // memcached://127.0.0.1:11211
        #[cfg(feature="b_memcached")]
        "memcached" => {
            let c = clients::memcached::make(addr);
            exercise::launch_mix(c, config)
        }
        // netsoup://127.0.0.1:7777
        #[cfg(feature="b_netsoup")]
        "netsoup" => {
            let c = clients::netsoup::make(addr, &config);
            exercise::launch_mix(c, config)
        }
        // garbage
        t => {
            panic!("backend not supported -- make sure you compiled with --features b_{}",
                   t)
        }
    };
    print_stats(&cfg.mix, &stats, avg);
}

fn print_stats(mix: &common::Mix, stats: &exercise::BenchmarkResults, avg: bool) {
    let stats = &stats.pre;
    if let Some((r_perc, w_perc)) = stats.cdf_percentiles() {
        if mix.does_read() {
            for iv in r_perc {
                println!("percentile GET {:.2} {:.2}", iv.value(), iv.percentile());
            }
        }
        if mix.does_write() {
            for iv in w_perc {
                println!("percentile PUT {:.2} {:.2}", iv.value(), iv.percentile());
            }
        }
    }

    if avg {
        let desc = if mix.is_mixed() {
            "MIX"
        } else if mix.does_read() {
            "GET"
        } else {
            "PUT"
        };

        println!("avg {}: {:.2}", desc, stats.avg_throughput());
    }
}
