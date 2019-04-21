use clap::value_t_or_exit;
use hdrhistogram::Histogram;
use noria::{Builder, FrontierStrategy, ReuseConfigType};
use rand::seq::SliceRandom;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const PAPERS_PER_REVIEWER: usize = 5;
const WRITE_CHUNK_SIZE: usize = 100;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
enum Operation {
    Login,
    ReadPaperList,
    WritePost,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Operation::Login => write!(f, "login"),
            Operation::ReadPaperList => write!(f, "plist"),
            Operation::WritePost => write!(f, "post"),
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
        info!(log, "starting up noria");
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

        let init = Instant::now();
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

        info!(log, "starting db population");
        debug!(log, "getting handles to tables");
        let mut user_profile = g.table("UserProfile").unwrap().into_sync();
        let mut paper = g.table("Paper").unwrap().into_sync();
        let mut coauthor = g.table("PaperCoauthor").unwrap().into_sync();
        let mut version = g.table("PaperVersion").unwrap().into_sync();
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
                    } else if i < nreviewers {
                        "pc".into()
                    } else {
                        "normal".into()
                    },
                ]
            }))
            .unwrap();
        debug!(log, "registering papers");
        paper
            .perform_all(papers.iter().enumerate().map(|(i, p)| {
                vec![
                    (i + 1).into(),
                    format!("{}", p.authors[0] + 1).into(),
                    if p.accepted { 1 } else { 0 }.into(),
                ]
            }))
            .unwrap();
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
        debug!(log, "registering paper authors");
        coauthor
            .perform_all(papers.iter().enumerate().flat_map(|(i, p)| {
                p.authors
                    .iter()
                    .skip(1)
                    .map(move |&a| vec![(i + 1).into(), format!("{}", a + 1).into()])
            }))
            .unwrap();
        debug!(log, "registering reviews");
        reviews.shuffle(&mut rng);
        // assume all reviews have been submitted
        trace!(log, "register assignments");
        review_assignment
            .perform_all(
                reviews
                    .chunks(PAPERS_PER_REVIEWER)
                    .enumerate()
                    .flat_map(|(i, rs)| {
                        rs.iter().map(move |r| {
                            vec![r.paper.into(), format!("{}", i + 1).into(), "foo".into()]
                        })
                    }),
            )
            .unwrap();
        trace!(log, "register the actual reviews");
        review
            .perform_all(
                reviews
                    .chunks(PAPERS_PER_REVIEWER)
                    .enumerate()
                    .flat_map(|(i, rs)| {
                        rs.iter().map(move |r| {
                            vec![
                                "0".into(),
                                r.paper.into(),
                                format!("{}", i + 1).into(),
                                "review text".into(),
                                r.rating.into(),
                                r.rating.into(),
                                r.rating.into(),
                                r.confidence.into(),
                            ]
                        })
                    }),
            )
            .unwrap();
        debug!(log, "population completed");

        let memstats = |g: &mut noria::SyncHandle<_>, at| {
            if let Ok(mem) = std::fs::read_to_string("/proc/self/statm") {
                debug!(log, "extracing process memory stats"; "at" => at);
                let vmrss = mem.split_whitespace().nth(2 - 1).unwrap();
                let data = mem.split_whitespace().nth(6 - 1).unwrap();
                println!("# VmRSS @ {}: {} ", at, vmrss);
                println!("# VmData @ {}: {} ", at, data);
            }

            debug!(log, "extracing materialization memory stats"; "at" => at);
            let mut base_mem = 0;
            let mut mem = 0;
            let stats = g.statistics().unwrap();
            for (_, nstats) in stats.values() {
                for nstat in nstats.values() {
                    if nstat.desc == "B" {
                        base_mem += nstat.mem_size;
                    } else {
                        mem += nstat.mem_size;
                    }
                }
            }
            println!("# base memory @ {}: {}", at, base_mem);
            println!("# materialization memory @ {}: {}", at, mem);
        };

        info!(log, "logging in users"; "n" => nlogged);
        memstats(&mut g, "populated");
        let mut login_times = Vec::with_capacity(nlogged);
        let mut login_stats = Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap();
        for (i, &uid) in authors.iter().take(nlogged).enumerate() {
            trace!(log, "logging in user"; "uid" => uid);
            let user_context: HashMap<_, _> =
                std::iter::once(("id".to_string(), format!("{}", i + 1).into())).collect();
            let start = Instant::now();
            g.on_worker(|w| w.create_universe(user_context.clone()))
                .unwrap();
            login_times.push(start.elapsed());
            login_stats.saturating_record(login_times.last().unwrap().as_micros() as u64);

            // TODO: measure space use, which means doing reads on partial
            // TODO: or do we want to do that measurement separately?

            if i == 0 {
                memstats(&mut g, "firstuser");
            }

            if i == 1 && iter == 1 {
                if let Some(gloc) = args.value_of("graph") {
                    debug!(log, "extracing query graph with two users");
                    let gv = g.graphviz().expect("failed to read graphviz");
                    std::fs::write(gloc, gv).expect("failed to save graphviz output");
                }
            }
        }
        use std::collections::hash_map::Entry;
        match cold_stats.entry(Operation::Login) {
            Entry::Vacant(v) => {
                v.insert(login_stats);
            }
            Entry::Occupied(mut h) => {
                *h.get_mut() += login_stats;
            }
        }
        memstats(&mut g, "allusers");

        info!(log, "creating api handles");
        debug!(log, "creating view handles for paper list");
        let mut paper_list: HashMap<_, _> = authors[0..nlogged]
            .iter()
            .map(|uid| {
                trace!(log, "creating posts handle for user"; "uid" => uid);
                (
                    uid,
                    g.view(format!("PaperList_u{}", uid)).unwrap().into_sync(),
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
            paper_list
                .get_mut(uid)
                .unwrap()
                .lookup(&[0.into(/* bogokey */)], true)
                .unwrap();
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
                _ => unreachable!(),
            }

            let begin = Instant::now();
            match op {
                Operation::ReadPaperList => {
                    paper_list
                        .get_mut(uid)
                        .unwrap()
                        .lookup(&[0.into(/* bogokey */)], true)
                        .unwrap();
                }
                _ => unreachable!(),
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

        /*
        info!(log, "performing write measurements");
        debug!(log, "writes to papers");
        let mut pid = nposts + 1;
        let start = Instant::now();
        while start.elapsed() < runtime {
            trace!(log, "writing post");
            let mut writes = Vec::new();
            while writes.len() < WRITE_CHUNK_SIZE {
                let uid = 1 + rng.gen_range(0, nlogged);
                trace!(log, "trying user"; "uid" => uid);
                if let Some(cids) = enrolled.get(&uid) {
                    let cid = *cids.choose(&mut rng).unwrap();
                    let private = if rng.gen_bool(private) { 1 } else { 0 };
                    trace!(log, "making post"; "cid" => cid, "private" => ?private);
                    writes.push(vec![
                        pid.into(),
                        cid.into(),
                        uid.into(),
                        format!("post #{}", pid).into(),
                        private.into(),
                    ]);
                    pid += 1;
                }
            }

            let begin = Instant::now();
            posts.perform_all(writes).unwrap();
            let took = begin.elapsed();

            if start.elapsed() < runtime / 3 {
                // warm-up
                trace!(log, "dropping sample during warm-up"; "at" => ?start.elapsed(), "took" => ?took);
                continue;
            }

            trace!(log, "recording sample"; "took" => ?took);
            cold_stats
                .entry(Operation::WritePost)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record((took.as_micros() / WRITE_CHUNK_SIZE as u128) as u64);
        }
        */

        // TODO: do we also want to measure writes when we _haven't_ read all the keys?

        let mut i = 0;
        let stripe = login_times.len() / 10;
        while i < login_times.len() {
            println!("# login sample[{}]: {:?}", i, login_times[i]);
            if i == 0 {
                // we want to include both 0 and 1
                i += 1;
            } else if i == 1 {
                // and then go back to every stripe'th sample
                i = stripe;
            } else {
                i += stripe;
            }
        }
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
