#![feature(plugin)]

extern crate clocked_dispatch;
extern crate distributary;
extern crate criterion;
extern crate shortcut;
extern crate docopt;
extern crate rand;
extern crate randomkit;

use distributary::*;
use docopt::Docopt;
use rand::Rng as StdRng;
use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use std::collections::HashMap;
use std::sync;
use std::thread;
use std::time;

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "
Benchmarks distributary put-get performance using article votes.

Usage:
  votes [\
--num-getters=<num> \
--prepopulate-articles=<num> \
--prepopulate-votes=<num> \
--runtime=<seconds> \
--vote-distribution=<dist>\
]

Options:
  --num-getters=<num>             Number of GET clients to start [default: 1]
  --prepopulate-articles=<num>    \
Number of articles to prepopulate (no runtime article PUTs) [default: 0]
  --prepopulate-votes=<num>       Number of votes to prepopulate [default: 0]
  --runtime=<seconds>             Runtime for benchmark, in seconds [default: 60]
  --vote-distribution=<dist>      Vote distribution: \"zipf\" or \"uniform\" [default: zipf]";

type Getter =
    Box<Fn(Option<distributary::Query>) -> Vec<Vec<distributary::DataType>> + Send + Sync>;

fn getter_client(client_id: usize,
                 start: time::Instant,
                 getter: sync::Arc<Getter>,
                 distribution: &str,
                 timeout_secs: usize,
                 max_art_id: sync::Arc<sync::atomic::AtomicIsize>) {

    let mut get_count = 0 as u64;
    let mut last_reported = start;

    let mut v_rng = Rng::from_seed(42);

    let zipf_dist = Zipf::new(1.07).unwrap();
    let uniform_dist = Uniform::new(1.0, max_art_id.load(sync::atomic::Ordering::Relaxed) as f64)
        .unwrap();

    while start.elapsed() < time::Duration::from_secs(timeout_secs as u64) {
        // Get some vote counts
        let cur_art_id = max_art_id.load(sync::atomic::Ordering::Relaxed);
        if cur_art_id > 0 {
            for _ in 0..9 {
                let id = match distribution {
                    "uniform" => uniform_dist.sample(&mut v_rng) as i64,
                    "zipf" => {
                        std::cmp::min(zipf_dist.sample(&mut v_rng) as isize, cur_art_id) as i64
                    }
                    _ => panic!("unknown vote distribution {}!", distribution),
                };


                // TODO: what if getter isn't ready?
                let q = Query::new(&[true, true, true],
                                   vec![shortcut::Condition {
                                 column: 0,
                                 cmp:
                                     shortcut::Comparison::Equal(shortcut::Value::Const(id.into())),
                             }]);
                let res: Vec<_> = getter(Some(q));
                if res.len() >= 2 {
                    println!("Got {} records, but expected zero or one!", res.len());
                    for r in res.into_iter() {
                        println!(" Fact: {:?}", r);
                    }
                }
                get_count += 1;
            }

            if last_reported.elapsed() > time::Duration::from_secs(1) {
                let ts = last_reported.elapsed();
                let throughput = get_count as f64 /
                                 (ts.as_secs() as f64 +
                                  ts.subsec_nanos() as f64 / 1_000_000_000f64);
                println!("{:?} GET{}: {:.2}", start.elapsed(), client_id, throughput);

                last_reported = time::Instant::now();
                get_count = 0;
            }
        }
    }
}

fn main() {
    let args = Docopt::new(BENCH_USAGE)
        .and_then(|dopt| dopt.parse())
        .unwrap_or_else(|e| e.exit());

    let runtime = args.get_str("--runtime").parse::<usize>().unwrap();
    let num_getters = args.get_str("--num-getters").parse::<usize>().unwrap();
    let prepopulate_articles = args.get_str("--prepopulate-articles").parse::<usize>().unwrap();
    let prepopulate_votes = args.get_str("--prepopulate-votes").parse::<usize>().unwrap();
    let vote_distribution = args.get_str("--vote-distribution").to_owned();

    assert!(num_getters > 0);

    // keep track of how many articles we have
    let max_art_id = sync::Arc::new(sync::atomic::AtomicIsize::new(0));

    // set up graph
    let mut g = distributary::FlowGraph::new();

    // add article base node
    let article = g.incorporate(new(&["id", "title"], true, Base {}), vec![]);

    // add vote base table
    let vote = g.incorporate(new(&["user", "id"], true, Base {}), vec![]);

    // add vote count
    let q = Query::new(&[true, true], Vec::new());
    let vc = g.incorporate(new(&["id", "votes"], true, Aggregation::COUNT.new(vote, 0, 2)),
                           vec![(q, vote)]);

    // add final join
    let mut join = HashMap::new();
    // if article joins against vote count, query and join using article's first field
    join.insert(article, vec![(article, vec![0]), (vc, vec![0])]);
    // if vote count joins against article, also query and join on the first field
    join.insert(vc, vec![(vc, vec![0]), (article, vec![0])]);
    // emit first and second field from article (id + title)
    // and second field from right (votes)
    let emit = vec![(article, 0), (article, 1), (vc, 1)];
    let j = Joiner::new(emit, join);
    // query to article/vc should select all fields, and query on id
    let q = Query::new(&[true, true],
                       vec![shortcut::Condition {
                                column: 0,
                                cmp:
                                    shortcut::Comparison::Equal(shortcut::Value::Const(distributary::DataType::None)),
                            }]);
    let end = g.incorporate(new(&["id", "title", "votes"], true, j),
                            vec![(q.clone(), article), (q, vc)]);


    // start processing
    let (put, mut get) = g.run(10);

    if prepopulate_articles > 0 {
        println!("Prepopulating {} articles", prepopulate_articles);
        let start = time::Instant::now();
        for i in 1..prepopulate_articles {
            let i = i as i64;
            put[&article]
                .send(distributary::Update::Records(vec![distributary::Record::Positive(vec![
                       i.into(), format!("Article #{}", i).into()
                    ])]));
        }
        let took = start.elapsed();
        println!(" ... took {} µs",
                 took.as_secs() * 1_000_000u64 + took.subsec_nanos() as u64 / 1_000u64);

        // some number of article ids are now safe
        max_art_id.fetch_add(prepopulate_articles as isize,
                             sync::atomic::Ordering::Relaxed);
    }

    if prepopulate_votes > 0 {
        assert!(prepopulate_articles > 0);

        println!("Prepopulating {} votes", prepopulate_votes);
        let start = time::Instant::now();
        for i in 0..prepopulate_votes {
            let i = i as i64;
            put[&article]
                .send(distributary::Update::Records(vec![distributary::Record::Positive(vec![
                     0.into(), (i % prepopulate_articles as i64).into()
                  ])]));
        }
        let took = start.elapsed();
        println!(" ... took {} µs",
                 took.as_secs() * 1_000_000u64 + took.subsec_nanos() as u64 / 1_000u64);
    }

    // let system settle
    thread::sleep(time::Duration::new(1, 0));

    // start getters
    let start = time::Instant::now();
    let getter = sync::Arc::new(get.remove(&end).unwrap());
    let getters: Vec<_> = (0..num_getters)
        .into_iter()
        .map(|i| {
            let getter = getter.clone();
            let max_art_id = max_art_id.clone();
            let vote_distribution = vote_distribution.clone();
            thread::spawn(move || {
                getter_client(i, start, getter, &*vote_distribution, runtime, max_art_id)
            })
        })
        .collect();

    // run puts
    let mut put_count = 0u64;
    let mut last_reported = start;

    let mut t_rng = rand::thread_rng();
    let mut v_rng = Rng::from_seed(42);

    let zipf_dist = Zipf::new(1.07).unwrap();

    while start.elapsed() < time::Duration::from_secs(runtime as u64) {
        // create an article if we don't have article prepopulation
        if prepopulate_articles == 0 {
            let article_id = max_art_id.load(sync::atomic::Ordering::Relaxed) as i64;
            put[&article]
                .send(distributary::Update::Records(vec![distributary::Record::Positive(vec![
                         article_id.into(), format!("Article #{}", article_id).into()
                      ])]));

            put_count += 1;
            max_art_id.fetch_add(1, sync::atomic::Ordering::Relaxed);
        }

        // create a bunch of random votes
        let max_id = max_art_id.load(sync::atomic::Ordering::Relaxed);
        let uniform_dist = Uniform::new(1.0, max_id as f64).unwrap();
        for _ in 0..9 {
            let vote_user = t_rng.gen::<i64>();
            let vote_rnd_id = match &*vote_distribution {
                "uniform" => uniform_dist.sample(&mut v_rng) as i64,
                "zipf" => std::cmp::min(zipf_dist.sample(&mut v_rng) as isize, max_id) as i64,
                _ => panic!("unknown vote distribution {}!", vote_distribution),
            };
            assert!(vote_rnd_id > 0);

            put[&vote].send(distributary::Update::Records(vec![distributary::Record::Positive(vec![
                             vote_user.into(), vote_rnd_id.into()
                      ])]));
            put_count += 1;
        }

        // check if we should report
        if last_reported.elapsed() > time::Duration::from_secs(1) {
            let ts = last_reported.elapsed();
            let throughput = put_count as f64 /
                             (ts.as_secs() as f64 + ts.subsec_nanos() as f64 / 1_000_000_000f64);
            println!("{:?} PUT: {:.2}", start.elapsed(), throughput);

            last_reported = time::Instant::now();
            put_count = 0;
        }
    }

    println!("Done, waiting for getters to join\n");

    for g in getters.into_iter() {
        g.join().unwrap();
    }

    println!("Done!");

    drop(put);
    drop(get);
}
