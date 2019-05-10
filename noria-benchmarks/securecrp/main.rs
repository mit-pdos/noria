use clap::value_t_or_exit;
use hdrhistogram::Histogram;
use noria::{Builder, FrontierStrategy, ReuseConfigType};
use rand::seq::SliceRandom;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use noria::manual::Base;
use noria::manual::ops::join::JoinSource::*;
use noria::manual::ops::join::{Join, JoinType};
use noria::manual::ops::rewrite::Rewrite;
use noria::manual::ops::filter::{Filter, FilterCondition, Value};
use nom_sql::Operator;

const PAPERS_PER_REVIEWER: usize = 5;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
enum Operation {
    ReadPaperList,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Operation::ReadPaperList => write!(f, "plist"),
        }
    }
}

struct Paper {
    accepted: bool,
    title: String,
    authors: Vec<usize>,
}

struct Review {
    paper: usize,
    rating: usize,
    confidence: usize,
}

fn main() {
    use clap::{App, Arg};
    let args = App::new("securecrp")
        .version("0.1")
        .about("Benchmarks HotCRP-like application with security policies.")
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("no")
                .possible_values(&["no", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("materialization")
                .long("materialization")
                .short("m")
                .default_value("full")
                .possible_values(&["full", "partial", "shallow-readers", "shallow-all"])
                .help("Set materialization strategy for the benchmark"),
        )
        .arg(
            Arg::with_name("source")
                .long("source")
                .default_value("https://openreview.net/group?id=ICLR.cc/2018/Conference")
                .takes_value(true)
                .help("Source to pull paper data from"),
        )
        .arg(
            Arg::with_name("schema")
                .long("schema")
                .short("s")
                .required(true)
                .default_value("noria-benchmarks/securecrp/jeeves_schema.sql")
                .help("SQL schema file"),
        )
        .arg(
            Arg::with_name("queries")
                .long("queries")
                .short("q")
                .required(true)
                .default_value("noria-benchmarks/securecrp/jeeves_queries.sql")
                .help("SQL query file"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .short("p")
                .required(true)
                .default_value("noria-benchmarks/securecrp/jeeves_policies.json")
                .help("Security policies file"),
        )
        .arg(
            Arg::with_name("npapers")
                .short("n")
                .takes_value(true)
                .default_value("10000")
                .help("Only fetch first n papers"),
        )
        .arg(
            Arg::with_name("logged-in")
                .short("l")
                .default_value("1.0")
                .help("Fraction of users that are logged in."),
        )
        .arg(
            Arg::with_name("iter")
                .long("iter")
                .default_value("1")
                .help("Number of iterations to run"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .takes_value(true)
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Enable verbose output"),
        )
        .get_matches();
    let verbose = args.occurrences_of("verbose");
    let loggedf = value_t_or_exit!(args, "logged-in", f64);
    let source = value_t_or_exit!(args, "source", url::Url);

    assert!(loggedf >= 0.0);
    assert!(loggedf <= 1.0);

    let log = if verbose != 0 {
        noria::logger_pls()
    } else {
        Logger::root(slog::Discard, o!())
    };
    let mut rng = rand::thread_rng();

    let conf = source
        .query_pairs()
        .find(|(arg, _)| arg == "id")
        .expect("could not find conference id in url")
        .1;
    info!(log, "fetching source data"; "conf" => &*conf);
    // https://openreview.net/api/#/Notes/findNotes
    let url = format!(
        "https://openreview.net/notes?invitation={}%2F-%2FBlind_Submission&limit={}",
        url::percent_encoding::utf8_percent_encode(
            &*conf,
            url::percent_encoding::DEFAULT_ENCODE_SET
        ),
        value_t_or_exit!(args, "npapers", usize),
    );
    debug!(log, "sending request for paper list"; "url" => &url);
    let all = reqwest::get(&url)
        .expect("failed to fetch source")
        .json::<serde_json::Value>()
        .expect("invalid source json");
    let all = all
        .as_object()
        .expect("root of source data is not a json object as expected");
    let all = all
        .get("notes")
        .and_then(|a| a.as_array())
        .expect("source data has a weird structure");
    let url = format!(
        "https://openreview.net/notes?invitation={}%2F-%2FAcceptance_Decision&limit=10000",
        url::percent_encoding::utf8_percent_encode(
            &*conf,
            url::percent_encoding::DEFAULT_ENCODE_SET
        )
    );
    debug!(log, "fetching list of accepted papers"; "url" => &url);
    let accept = reqwest::get(&url)
        .expect("failed to fetch accepted list")
        .json::<serde_json::Value>()
        .expect("invalid acceptance list json");
    let accept = accept
        .as_object()
        .expect("root of acceptance list is not a json object as expected");
    let accept = accept
        .get("notes")
        .and_then(|a| a.as_array())
        .expect("acceptance list data has a weird structure");
    let mut accepted = HashSet::new();
    for decision in accept {
        let decision = decision.as_object().expect("acceptance info is weird");
        let id = decision["forum"]
            .as_str()
            .expect("listed acceptance forum is not a string");
        let was_accepted = decision["content"]
            .as_object()
            .expect("acceptance info content is weird")["decision"]
            .as_str()
            .expect("listed acceptance decision is not a string")
            .starts_with("Accept");
        if was_accepted {
            trace!(log, "noted acceptance decision"; "paper" => id);
            accepted.insert(id);
        }
    }
    let mut author_set = HashMap::new();
    let mut authors = Vec::new();
    let mut papers = Vec::new();
    let mut reviews = Vec::new();
    debug!(log, "processing paper list"; "n" => all.len());
    for paper in all {
        let paper = paper.as_object().expect("paper info isn't a json object");
        let id = paper["id"].as_str().expect("paper id is weird");
        let number = paper["number"].as_u64().expect("paper number is weird");
        let content = paper["content"]
            .as_object()
            .expect("paper info doesn't have content");
        let title = content["title"].as_str().unwrap().to_string();
        let authors: Vec<_> = content["authorids"]
            .as_array()
            .expect("author list is not an array")
            .iter()
            .map(|author| {
                let author = author.as_str().expect("author id is not a string");
                *author_set.entry(author.to_string()).or_insert_with(|| {
                    trace!(log, "adding author"; "name" => author, "uid" => author.len() + 1);
                    authors.push(author);
                    authors.len() - 1
                })
            })
            .collect();;

        let pid = papers.len() + 1;
        trace!(log, "adding paper"; "title" => &title, "id" => pid, "accepted" => accepted.contains(id));
        papers.push(Paper {
            title,
            accepted: accepted.contains(id),
            authors,
        });

        let url = format!(
            "https://openreview.net/notes?forum={}&invitation={}/-/Paper{}/Official_Review",
            url::percent_encoding::utf8_percent_encode(
                &*id,
                url::percent_encoding::DEFAULT_ENCODE_SET
            ),
            url::percent_encoding::utf8_percent_encode(
                &*conf,
                url::percent_encoding::DEFAULT_ENCODE_SET
            ),
            format!("{}", number),
        );
        trace!(log, "fetching paper reviews"; "url" => &url);
        let paper_reviews = reqwest::get(&url)
            .expect("failed to fetch paper reviews")
            .json::<serde_json::Value>()
            .expect("invalid paper review json");
        let paper_reviews = paper_reviews
            .as_object()
            .expect("paper reviews is not a json object as expected");
        let paper_reviews = paper_reviews
            .get("notes")
            .and_then(|rs| rs.as_array())
            .expect("paper reviews has a weird structure");
        for review in paper_reviews {
            let content = review.as_object().expect("review was not an object")["content"]
                .as_object()
                .expect("review did not have regular contents");
            let r = Review {
                paper: pid,
                rating: content["rating"]
                    .as_str()
                    .expect("rating wasn't a string")
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .trim_end_matches(':')
                    .parse()
                    .expect("rating did not start with a number"),
                confidence: content["confidence"]
                    .as_str()
                    .expect("confidence wasn't a string")
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .trim_end_matches(':')
                    .parse()
                    .expect("confidence did not start with a number"),
            };
            trace!(log, "adding review"; "rating" => r.rating, "confidence" => r.confidence);
            reviews.push(r);
        }
    }

    drop(author_set);
    let nusers = authors.len();
    let nlogged = (loggedf * nusers as f64) as usize;

    // let's compute the number of reviewers
    // we know the number of reviews
    // we have fixed the number of reviews per reviewer
    // and we assume every reviewer is an author
    let nreviewers = (reviews.len() + (PAPERS_PER_REVIEWER - 1)) / PAPERS_PER_REVIEWER;

    println!("# nauthors: {}", authors.len());
    println!("# nreviewers: {}", nreviewers);
    println!("# logged-in users: {}", nlogged);
    println!("# npapers: {}", papers.len());
    println!("# nreviews: {}", reviews.len());
    println!(
        "# materialization: {}",
        args.value_of("materialization").unwrap()
    );

    let mut cold_stats = HashMap::new();
    let mut warm_stats = HashMap::new();
    let iter = value_t_or_exit!(args, "iter", usize);
    for iter in 1..=iter {
        info!(log, "starting up noria"; "iteration" => iter);
        debug!(log, "configuring noria");
        let mut g = Builder::default();
        match args.value_of("reuse").unwrap() {
            "finkelstein" => g.set_reuse(ReuseConfigType::Finkelstein),
            "full" => g.set_reuse(ReuseConfigType::Full),
            "no" => g.set_reuse(ReuseConfigType::NoReuse),
            "relaxed" => g.set_reuse(ReuseConfigType::Relaxed),
            _ => unreachable!(),
        }

        match args.value_of("materialization").unwrap() {
            "full" => {
                g.disable_partial();
            }
            "partial" => {}
            "shallow-readers" => {
                g.set_frontier_strategy(FrontierStrategy::Readers);
            }
            "shallow-all" => {
                g.set_frontier_strategy(FrontierStrategy::AllPartial);
            }
            _ => unreachable!(),
        }
        g.set_sharding(None);
        if verbose > 1 {
            g.log_with(log.clone());
        }
        debug!(log, "spinning up");
        let mut g = g.start_simple().unwrap();
        debug!(log, "noria ready");

        let manual_graph = (args.value_of("schema").unwrap() == "noschema");
        
        let init = Instant::now();
        let nauthors = authors.len();
        // Manual Graph
        if manual_graph {
            std::thread::sleep(std::time::Duration::from_secs(1)); // Allow Handle to realize it is leader
            println!("noschema case: doing manually set-up migrations");
            // BASE TABLES
            let (review, review_assgn, paper, coauthor) = g.migrate(|mig| {
                // paper,reviewer col is a hacky way of doing multi-column joins
                println!("making user profile table");
                let user_profile = mig.add_base(
                    "UserProfile",
                    &["username", "email", "name", "affiliation", "acm_number", "level"],
                    Base::new(vec![]).with_key(vec![0]));
                println!("making review_assgn table");
                let review_assgn = mig.add_base(
                    "ReviewAssignment",
                    &["paper", "reviewer", "paper,reviewer"],
                    Base::new(vec![]).with_key(vec![2]));
                //                    mig.maintain_anonymous(review_assgn, &[1]); // for PC members to view
                let review = mig.add_base(
                    "Review",
                    &["paper", "reviewer", "contents", "paper,reviewer", "rating", "confidence"],
                    Base::new(vec![]).with_key(vec![3]));
                let paper = mig.add_base(
                    "Paper",
                    &["paper","author","accepted"],
                    Base::new(vec![]).with_key(vec![0]));
                let coauthor = mig.add_base(
                    "PaperCoauthor",
                    &["paper","author"],
                    Base::new(vec![]).with_key(vec![0, 1]));
                println!("returning created tables");
                (review, review_assgn, paper, coauthor)
            });
            
            println!("created base tables");
            // BASE TABLE DIRECT DERIVATIVES
            let (paper_rewrite, papers_for_authors, submitted_reviews) = g.migrate(move |mig| {
                let paper_rewrite = mig.add_ingredient(
                    "paper_rewrite",
                    &["paper", "author", "accepted"],
                    Rewrite::new(
                        paper,
                        paper,
                        1 as usize,
                        "anonymous".into(),
                        0 as usize));
                mig.maintain_anonymous(paper_rewrite, &[0]);
                
                let papers_for_authors = mig.add_ingredient(
                    "papers_for_authors",
                    &["author", "paper", "accepted"],
                    Join::new(paper, coauthor, JoinType::Inner, vec![R(1), B(0, 0), L(2)]));
                
                let submitted_reviews = mig.add_ingredient(
                    "submitted_reviews",
                    &["reviewer", "paper", "contents", "rating", "confidence", "paper,reviewer"],
                    Join::new(review,
                              review_assgn,
                              JoinType::Inner,
                              vec![L(1), L(0), L(2), L(4), L(5), B(3, 2)]));
                
                (paper_rewrite, papers_for_authors, submitted_reviews)
            });

            println!("finished adding base table derivatives");
            // NEXT LAYER
            let (reviews_by_ris, review_rewrite) = g.migrate(move |mig| {
                // One view per author, per reviewer
                let mut reviews_by_ris = Vec::new();
                for i in 0..nauthors {
                    let uid = (i + 1).to_string();
                    let papers_ai = mig.add_ingredient(
                        format!("{}{}", "PaperList_u", uid),
                        &["author", "paper", "accepted"],
                        Filter::new(papers_for_authors,
                                    &[Some(FilterCondition::Comparison(Operator::Equal,
                                                                       Value::Constant(uid.clone().into())))]));
                    mig.maintain_anonymous(papers_ai, &[0]);
                    
                    if i > nreviewers {
                        continue;
                    }
                    
                    let reviews_by_ri = mig.add_ingredient(
                        format!("{}{}", "reviews_by_", uid),
                        &["reviewer", "paper", "contents", "rating", "confidence"], // another way to specify col?
                        Filter::new(submitted_reviews,
                                    &[Some(FilterCondition::Comparison(Operator::Equal,
                                                                       Value::Constant(uid.clone().into())))]));
                    reviews_by_ris.push(reviews_by_ri);
                }
                
                // Note: anonymization doesn't happen if signal column comes from submitted_reviews
                // instead of directly from review table.
                let review_rewrite = mig.add_ingredient(
                    "review_rewrite",
                    &["reviewer", "paper", "contents","rating", "confidence"],
                    Rewrite::new(
                        submitted_reviews,
                        review,
                        0 as usize,
                        "anonymous".into(),
                        1 as usize));
                
                (reviews_by_ris, review_rewrite)
            });
            
            println!("added reviews_by_ris and rewrite for review");
            // REVIEWS FOR R1
            let _ = g.migrate(move |mig| {
                for i in 0..nreviewers {
                    let uid = (i + 1).to_string();
                    let reviews_by_ri = *reviews_by_ris.get(i).expect("reviewer ids are off, can't find");
                    let reviews_ri = mig.add_ingredient(
                        format!("{}{}", "reviews_r", uid),
                        &["reviewer", "paper", "contents", "rating", "confidence"],
                        Join::new(review_rewrite,
                                  reviews_by_ri,
                                  JoinType::Inner,
                                  vec![L(0), B(1, 1), L(2), L(3), L(4)]));
                    mig.maintain_anonymous(reviews_ri, &[0]);
                }
            });
            println!("done constructing graph");
        } else {
        // Recipe Installation
            info!(log, "setting up database schema");
            debug!(log, "setting up initial schema");
            g.install_recipe(
                std::fs::read_to_string(args.value_of("schema").unwrap())
                    .expect("failed to read schema file"),
            )
                .expect("failed to load initial schema");
            debug!(log, "adding security policies");
            g.on_worker(|w| {
                w.set_security_config(
                    std::fs::read_to_string(args.value_of("policies").unwrap())
                        .expect("failed to read policy file"),
                )
            })
                .unwrap();
            debug!(log, "adding queries");
            g.extend_recipe(
                std::fs::read_to_string(args.value_of("queries").unwrap())
                    .expect("failed to read queries file"),
            )
                .expect("failed to load initial schema");
            debug!(log, "database schema setup done");
        }
        let memstats = |g: &mut noria::SyncHandle<_>, at| {
            if let Ok(mem) = std::fs::read_to_string("/proc/self/statm") {
                debug!(log, "extracing process memory stats"; "at" => at);
                let vmrss = mem.split_whitespace().nth(2 - 1).unwrap();
                let data = mem.split_whitespace().nth(6 - 1).unwrap();
                println!("# VmRSS @ {}: {} ", at, vmrss);
                println!("# VmData @ {}: {} ", at, data);
            }

            debug!(log, "extracing materialization memory stats"; "at" => at);
            let mut reader_mem = 0;
            let mut base_mem = 0;
            let mut mem = 0;
            
            println!("getting stats");
            let stats = g.statistics().unwrap();
            println!("got stats");
            for (_, nstats) in stats.values() {
                println!("looping through values");
                for nstat in nstats.values() {
                    if nstat.desc == "B" {
                        base_mem += nstat.mem_size;
                    } else if nstat.desc == "reader node" {
                        reader_mem += nstat.mem_size;
                    } else {
                        mem += nstat.mem_size;
                    }
                }
            }
            println!("# base memory @ {}: {}", at, base_mem);
            println!("# reader memory @ {}: {}", at, reader_mem);
            println!("# materialization memory @ {}: {}", at, mem);
        };

        info!(log, "starting db population");
        debug!(log, "getting handles to tables");
        let mut user_profile = g.table("UserProfile").unwrap().into_sync();
        let mut paper = g.table("Paper").unwrap().into_sync();
        let mut coauthor = g.table("PaperCoauthor").unwrap().into_sync();
//        let mut version = g.table("PaperVersion").unwrap().into_sync();
        let mut review_assignment = g.table("ReviewAssignment").unwrap().into_sync();
        let mut review = g.table("Review").unwrap().into_sync();
        debug!(log, "creating users"; "n" => nusers);
        user_profile
            .perform_all(authors.iter().enumerate().map(|(i, &email)| {
                vec![
                    format!("{}", i + 1).into(),
                    email.into(),
                    email.into(),
                    "university".into(),
                    "0".into(),
                    if i == 0 {
                        "chair".into()
//                    } else if i < nreviewers {
//                        "pc".into()
                    } else {
                        "normal".into()
                    },
                ]
            }))
            .unwrap();
        // skip this if manual graph
        if !manual_graph {
            debug!(log, "logging in users"; "n" => nlogged);
            let mut printi = 0;
            let stripe = nlogged / 10;
            let mut login_times = Vec::with_capacity(nlogged);
            for (i, &uid) in authors.iter().take(nlogged).enumerate() {
                trace!(log, "logging in user"; "uid" => uid);
                let user_context: HashMap<_, _> =
                    std::iter::once(("id".to_string(), format!("{}", i + 1).into())).collect();
                let start = Instant::now();
                g.on_worker(|w| w.create_universe(user_context.clone()))
                    .unwrap();
                let took = start.elapsed();
                login_times.push(took);
                
                if i == printi {
                    println!("# login sample[{}]: {:?}", i, login_times[i]);
                    if i == 0 {
                        // we want to include both 0 and 1
                        printi += 1;
                    } else if i == 1 {
                        // and then go back to every stripe'th sample
                        printi = stripe;
                    } else {
                        printi += stripe;
                    }
                }
            }
        }
        
        debug!(log, "registering papers");
        let start = Instant::now();
        paper
            .perform_all(papers.iter().enumerate().map(|(i, p)| {
                vec![
                    (i + 1).into(),
                    format!("{}", p.authors[0] + 1).into(),
                    if p.accepted { 1 } else { 0 }.into(),
                ]
            }))
            .unwrap();
        println!(
            "# paper registration: {} in {:?}",
            papers.len(),
            start.elapsed()
        );
        // For manual graph benchmarking, don't use PaperVersion table
/*
        trace!(log, "also registering paper version");
        version
            .perform_all(papers.iter().enumerate().map(|(i, p)| {
                vec![
                    (i + 1).into(),
                    (&*p.title).into(),
                    "Text".into(),
                    "Abstract".into(),
                    "0".into(),
                ]
            }))
            .unwrap();
        println!(
            "# paper + version: {} in {:?}",
            papers.len(),
            start.elapsed()
        );
*/
        debug!(log, "registering paper authors");
        let start = Instant::now();
        let mut npauthors = 0;
        coauthor
            .perform_all(papers.iter().enumerate().flat_map(|(i, p)| {
                // XXX: should first author be repeated here?
                npauthors += p.authors.len();
                p.authors
                    .iter()
                    .map(move |&a| vec![(i + 1).into(), format!("{}", a + 1).into()])
            }))
            .unwrap();
        println!("# paper authors: {} in {:?}", npauthors, start.elapsed());
        debug!(log, "registering reviews");
        reviews.shuffle(&mut rng);
        // assume all reviews have been submitted
        trace!(log, "register assignments");
        let start = Instant::now();
        review_assignment
            .perform_all(
                reviews
                    .chunks(PAPERS_PER_REVIEWER)
                    .enumerate()
                    .flat_map(|(i, rs)| {
                        // TODO: don't review own paper
                        rs.iter().map(move |r| {
                            if manual_graph {
                                vec![r.paper.into(),
                                     format!("{}", i + 1).into(), format!("{},{}", r.paper, i+1).into()]
                            } else {
                                vec![r.paper.into(), format!("{}", i + 1).into()]
                            }
                        })
                    }),
            )
            .unwrap();
        println!(
            "# review assignments: {} in {:?}",
            reviews.len(),
            start.elapsed()
        );
        trace!(log, "register the actual reviews");
        let start = Instant::now();
        review
            .perform_all(
                reviews
                    .chunks(PAPERS_PER_REVIEWER)
                    .enumerate()
                    .flat_map(|(i, rs)| {
                        rs.iter().map(move |r| {
                            if manual_graph {
                                vec![
                                    //                                "0".into(),
                                    r.paper.into(),
                                    format!("{}", i + 1).into(),
                                    "review text".into(),
                                    format!("{},{}", r.paper, i + 1).into(),
                                    r.rating.into(),
                                    //                                r.rating.into(),
                                    //                                r.rating.into(),
                                    r.confidence.into(),
                                ]
                            }
                            else {
                                vec![
                                    //                                "0".into(),
                                    r.paper.into(),
                                    format!("{}", i + 1).into(),
                                    "review text".into(),
                                    r.rating.into(),
                                    //                                r.rating.into(),
                                    //                                r.rating.into(),
                                    r.confidence.into(),
                                ]
                            }
                        })
                    }),
            )
            .unwrap();
        println!("# reviews: {} in {:?}", reviews.len(), start.elapsed());
        debug!(log, "population completed");
        memstats(&mut g, "populated");

        if let Some(gloc) = args.value_of("graph") {
            debug!(log, "extracing query graph");
            let gv = g.graphviz().expect("failed to read graphviz");
            std::fs::write(gloc, gv).expect("failed to save graphviz output");
        }

        info!(log, "creating api handles");
        debug!(log, "creating view handles for paper list");
        let mut paper_list: HashMap<_, _> = (0..nlogged)
            .map(|uid| {
                trace!(log, "creating posts handle for user"; "uid" => authors[uid]);
                (
                    authors[uid],
                    g.view(format!("PaperList_u{}", uid + 1))
                        .unwrap()
                        .into_sync(),
                )
            })
            .collect();
        debug!(log, "all api handles created");

        println!("# setup time: {:?}", init.elapsed());

        // now time to measure the cost of different operations
        info!(log, "starting cold read benchmarks");
        debug!(log, "cold reads of paper list");
        let mut requests = Vec::new();
        'pl_outer: for uid in authors[0..nlogged].choose_multiple(&mut rng, nlogged) {
            trace!(log, "reading paper list"; "uid" => uid);
            requests.push((Operation::ReadPaperList, uid));
            let begin = Instant::now();
            if manual_graph {
                paper_list
                    .get_mut(uid)
                    .unwrap()
                    .lookup(&[uid.to_string().into()], true)
                    .unwrap();
            } else {
                paper_list
                    .get_mut(uid)
                    .unwrap()
                    .lookup(&[0.into(/* bogokey */)], true)
                    .unwrap();
            }
            let took = begin.elapsed();

            // NOTE: do we want a warm-up period/drop first sample per uid?
            // trace!(log, "dropping sample during warm-up"; "at" => ?start.elapsed(), "took" => ?took);

            trace!(log, "recording sample"; "took" => ?took);
            cold_stats
                .entry(Operation::ReadPaperList)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }

        info!(log, "starting warm read benchmarks");
        for (op, uid) in requests {
            match op {
                Operation::ReadPaperList => {
                    trace!(log, "reading paper list"; "uid" => uid);
                }
            }

            let begin = Instant::now();
            match op {
                Operation::ReadPaperList => {
                    // Different "bogokey" for manually created graph
                    if manual_graph {
                        paper_list
                            .get_mut(uid)
                            .unwrap()
                            .lookup(&[uid.to_string().into()], true)
                            .unwrap();
                    } else { 
                        paper_list
                            .get_mut(uid)
                            .unwrap()
                            .lookup(&[0.into(/* bogokey */)], true)
                            .unwrap();
                    }
                }
            }
            let took = begin.elapsed();

            // NOTE: no warm-up for "warm" reads

            trace!(log, "recording sample"; "took" => ?took);
            warm_stats
                .entry(op)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }

        info!(log, "measuring space overhead");
        // NOTE: we have already done all possible reads, so no need to do "filling" reads
        memstats(&mut g, "end");
    }

    println!("# op\tphase\tpct\ttime");
    for &q in &[50, 95, 99, 100] {
        for &heat in &["cold", "warm"] {
            let stats = match heat {
                "cold" => &cold_stats,
                "warm" => &warm_stats,
                _ => unreachable!(),
            };
            let mut keys: Vec<_> = stats.keys().collect();
            keys.sort();
            for op in keys {
                let stats = &stats[op];
                if q == 100 {
                    println!("{}\t{}\t100\t{:.2}\tµs", op, heat, stats.max());
                } else {
                    println!(
                        "{}\t{}\t{}\t{:.2}\tµs",
                        op,
                        heat,
                        q,
                        stats.value_at_quantile(q as f64 / 100.0)
                    );
                }
            }
        }
    }
}
