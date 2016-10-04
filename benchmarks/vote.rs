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

extern crate clocked_dispatch;
extern crate distributary;
extern crate shortcut;

#[cfg(feature="b_netsoup")]
extern crate tarpc;

#[cfg(feature="b_memcached")]
extern crate memcache;

mod targets;

use rand::Rng as StdRng;
use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use std::thread;
use std::time;

extern crate hdrsample;
use hdrsample::Histogram;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

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

    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let dbn = args.value_of("BACKEND").unwrap();
    let mut runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let ngetters = value_t_or_exit!(args, "ngetters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    let distribution = args.value_of("distribution").unwrap();
    assert!(ngetters > 0);
    assert!(!dbn.is_empty());

    // setup db
    println!("Attempting to connect to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let mut target: Box<targets::Backend> = match dbn.next().unwrap() {
        // soup://
        "soup" => targets::soup::make(dbn.next().unwrap(), ngetters),
        // postgresql://soup@127.0.0.1/bench_psql
        #[cfg(feature="b_postgresql")]
        "postgresql" => targets::postgres::make(dbn.next().unwrap(), ngetters),
        // memcached://127.0.0.1:11211
        #[cfg(feature="b_memcached")]
        "memcached" => targets::memcached::make(dbn.next().unwrap(), ngetters),
        // netsoup://127.0.0.1:7777
        #[cfg(feature="b_netsoup")]
        "netsoup" => targets::netsoup::make(dbn.next().unwrap(), ngetters),
        // garbage
        t => {
            panic!("backend not supported -- make sure you compiled with --features b_{}",
                   t)
        }
    };

    // prepopulate
    println!("Connected. Now retrieving putter for prepopulation.");
    let mut putter = target.putter();
    {
        let mut article = putter.article();

        // prepopulate
        println!("Prepopulating with {} articles", narticles);
        {
            // let t = putter.transaction().unwrap();
            for i in 0..narticles {
                article(i as i64, format!("Article #{}", i));
            }
            // t.commit().unwrap();
        }
        println!("Done with prepopulation");
    }

    // let system settle
    thread::sleep(time::Duration::new(1, 0));
    let start = time::Instant::now();

    // benchmark
    // start putting
    let mut putter = Some({
        let distribution = distribution.to_owned();
        let start = start.clone();
        thread::spawn(move || {
            let mut count = 0;
            let mut samples = Histogram::<u64>::new_with_bounds(1, 100000, 3).unwrap();
            let mut last_reported = start;

            let mut t_rng = rand::thread_rng();
            let mut v_rng = Rng::from_seed(42);
            let zipf_dist = Zipf::new(1.07).unwrap();

            {
                let mut vote = putter.vote();
                while start.elapsed() < runtime {
                    let uniform_dist = Uniform::new(1.0, narticles as f64).unwrap();
                    let vote_user = t_rng.gen::<i64>();
                    let vote_rnd_id = match &*distribution {
                        "uniform" => uniform_dist.sample(&mut v_rng) as isize,
                        "zipf" => zipf_dist.sample(&mut v_rng) as isize,
                        _ => panic!("unknown vote distribution {}!", distribution),
                    };
                    let vote_rnd_id = std::cmp::min(vote_rnd_id, narticles - 1) as i64;
                    assert!(vote_rnd_id > 0);

                    if cdf {
                        let t = time::Instant::now();
                        vote(vote_user, vote_rnd_id);
                        samples += (dur_to_ns!(t.elapsed()) / 1000) as i64;
                    } else {
                        vote(vote_user, vote_rnd_id);
                    }
                    count += 1;

                    // check if we should report
                    if last_reported.elapsed() > time::Duration::from_secs(1) {
                        let ts = last_reported.elapsed();
                        let throughput = count as f64 /
                                         (ts.as_secs() as f64 +
                                          ts.subsec_nanos() as f64 / 1_000_000_000f64);
                        println!("{:?} PUT: {:.2}", dur_to_ns!(start.elapsed()), throughput);

                        last_reported = time::Instant::now();
                        count = 0;
                    }
                }
            }

            if cdf {
                for (v, p, _, _) in samples.iter_percentiles(1) {
                    println!("percentile PUT {:.2} {:.2}", v, p);
                }
            }
        })
    });

    if stage {
        println!("Waiting for putter before starting getters");
        putter.take().unwrap().join().unwrap();
        // avoid getters stopping immediately
        runtime *= 2;
    }

    // start getters
    println!("Starting {} getters", ngetters);
    let getters = (0..ngetters)
        .into_iter()
        .map(|i| (i, target.getter()))
        .map(|(i, g)| {
            println!("Starting getter #{}", i);
            let distribution = distribution.to_owned();
            let start = start.clone();
            thread::spawn(move || {
                let mut count = 0 as u64;
                let mut samples = Histogram::<u64>::new_with_bounds(1, 100000, 3).unwrap();
                let mut last_reported = start;

                let mut v_rng = Rng::from_seed(42);

                let zipf_dist = Zipf::new(1.07).unwrap();
                let uniform_dist = Uniform::new(1.0, narticles as f64).unwrap();

                {
                    let mut get = g.get();
                    while start.elapsed() < runtime {
                        // what article to vote for?
                        let id = match &*distribution {
                            "uniform" => uniform_dist.sample(&mut v_rng) as isize,
                            "zipf" => zipf_dist.sample(&mut v_rng) as isize,
                            _ => panic!("unknown vote distribution {}!", distribution),
                        };
                        let id = std::cmp::min(id, narticles - 1) as i64;

                        if cdf {
                            let t = time::Instant::now();
                            get(id);
                            samples += (dur_to_ns!(t.elapsed()) / 1000) as i64;
                        } else {
                            get(id);
                        }
                        count += 1;

                        if last_reported.elapsed() > time::Duration::from_secs(1) {
                            let ts = last_reported.elapsed();
                            let throughput = count as f64 /
                                             (ts.as_secs() as f64 +
                                              ts.subsec_nanos() as f64 / 1_000_000_000f64);
                            println!("{:?} GET{}: {:.2}",
                                     dur_to_ns!(start.elapsed()),
                                     i,
                                     throughput);

                            last_reported = time::Instant::now();
                            count = 0;
                        }
                    }
                }

                if cdf {
                    for (v, p, _, _) in samples.iter_percentiles(1) {
                        println!("percentile GET{} {:.2} {:.2}", i, v, p);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    println!("Started {} getters", getters.len());

    // clean
    if let Some(putter) = putter {
        // is putter also running?
        putter.join().unwrap();
    }
    for g in getters.into_iter() {
        g.join().unwrap();
    }
}
