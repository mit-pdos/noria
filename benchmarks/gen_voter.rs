#![feature(plugin)]
extern crate docopt;
extern crate rand;
extern crate randomkit;

extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;

extern crate clocked_dispatch;
extern crate distributary;
extern crate shortcut;

extern crate bmemcached;

mod targets;

use docopt::Docopt;
use rand::Rng as StdRng;
use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use std::thread;
use std::time;

pub trait Backend {
    fn putter(&mut self) -> Box<Putter>;
    fn getter(&mut self) -> Box<Getter>;
}

pub trait Putter {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a>;
    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a>;
}

pub trait Getter: Send {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> (i64, String, i64) + 'a>;
}

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "
Benchmarks PostgreSQL materialization performance.

Usage:
  gen_voter [\
    --num-getters=<num> \
    --prepopulate-articles=<num> \
    --runtime=<seconds> \
    --vote-distribution=<dist>\
    <dbn>\
]

Options:
  --num-getters=<num>             Number of GET clients to start [default: 1]
  --prepopulate-articles=<num>    Number of articles to prepopulate (no runtime article PUTs) [default: 0]
  --runtime=<seconds>             Runtime for benchmark, in seconds [default: 60]
  --vote-distribution=<dist>      Vote distribution: \"zipf\" or \"uniform\" [default: zipf]";

fn main() {
    let args = Docopt::new(BENCH_USAGE)
        .and_then(|dopt| dopt.parse())
        .unwrap_or_else(|e| e.exit());

    let dbn = args.get_str("<dbn>");
    let runtime = time::Duration::from_secs(args.get_str("--runtime").parse::<u64>().unwrap());
    let num_getters = args.get_str("--num-getters").parse::<usize>().unwrap();
    let num_articles = args.get_str("--prepopulate-articles").parse::<isize>().unwrap();
    let distribution = args.get_str("--vote-distribution");
    assert!(num_getters > 0);

    // setup db
    println!("Connecting to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let mut target = match dbn.next().unwrap() {
        // postgresql://soup@127.0.0.1/bench_psql
        "postgresql" => targets::postgres::make(dbn.next().unwrap(), num_getters),
        // memcached://127.0.0.11211
        "memcached" => targets::memcached::make(dbn.next().unwrap(), num_getters),
        // soup://
        "soup" => targets::soup::make(dbn.next().unwrap(), num_getters),
        // garbage
        _ => unimplemented!(),
    };

    // prepopulate
    println!("Connected. Now retrieving putter for prepopulation.");
    let mut putter = target.putter();
    {
        let mut article = putter.article();

        // prepopulate
        println!("Prepopulating with {} articles", num_articles);
        {
            // let t = putter.transaction().unwrap();
            for i in 0..num_articles {
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
    // TODO: support staging?
    // start getters
    println!("Starting {} getters", num_getters);
    let getters = (0..num_getters)
        .into_iter()
        .map(|i| (i, target.getter()))
        .map(|(i, g)| {
            println!("Starting getter #{}", i);
            let distribution = distribution.to_owned();
            let start = start.clone();
            thread::spawn(move || {
                let mut count = 0 as u64;
                let mut last_reported = start;

                let mut v_rng = Rng::from_seed(42);

                let zipf_dist = Zipf::new(1.07).unwrap();
                let uniform_dist = Uniform::new(1.0, num_articles as f64).unwrap();

                {
                    let mut get = g.get();
                    while start.elapsed() < runtime {
                        // what article to vote for?
                        let id = match &*distribution {
                            "uniform" => uniform_dist.sample(&mut v_rng) as isize,
                            "zipf" => zipf_dist.sample(&mut v_rng) as isize,
                            _ => panic!("unknown vote distribution {}!", distribution),
                        };
                        let id = std::cmp::min(id as isize, num_articles - 1) as i64;

                        get(id);
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
            })
        })
        .collect::<Vec<_>>();
    println!("Started {} getters", getters.len());

    // start putting
    let mut count = 0;
    let mut last_reported = start;

    let mut t_rng = rand::thread_rng();
    let mut v_rng = Rng::from_seed(42);
    let zipf_dist = Zipf::new(1.07).unwrap();

    {
        let mut vote = putter.vote();
        while start.elapsed() < runtime {
            let uniform_dist = Uniform::new(1.0, num_articles as f64).unwrap();
            let vote_user = t_rng.gen::<i64>();
            let vote_rnd_id = match distribution {
                "uniform" => uniform_dist.sample(&mut v_rng) as isize,
                "zipf" => zipf_dist.sample(&mut v_rng) as isize,
                _ => panic!("unknown vote distribution {}!", distribution),
            };
            let vote_rnd_id = std::cmp::min(vote_rnd_id, num_articles - 1) as i64;
            assert!(vote_rnd_id > 0);

            vote(vote_user, vote_rnd_id);
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

    // clean
    drop(putter);
    for g in getters.into_iter() {
        g.join().unwrap();
    }
}
