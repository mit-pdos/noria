#![feature(duration_float)]

use clap::value_t_or_exit;
use hdrhistogram::Histogram;
use noria::{Builder, FrontierStrategy, ReuseConfigType};
use rand::seq::SliceRandom;
use rand::Rng;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const STUDENTS_PER_CLASS: usize = 150;
const TAS_PER_CLASS: usize = 5;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
enum Operation {
    Login,
    ReadPostCount,
    ReadPosts,
    WritePost,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Operation::Login => write!(f, "login"),
            Operation::ReadPostCount => write!(f, "pcount"),
            Operation::ReadPosts => write!(f, "posts"),
            Operation::WritePost => write!(f, "post"),
        }
    }
}

fn main() {
    use clap::{App, Arg};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with security policies.")
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["no", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("runtime")
                .long("runtime")
                .short("r")
                .default_value("10")
                .help("Seconds to run the benchmark for (first 1/3 of samples are dropped)"),
        )
        .arg(
            Arg::with_name("materialization")
                .long("materialization")
                .short("m")
                .default_value("partial")
                .possible_values(&["full", "partial", "shallow-readers", "shallow-all"])
                .help("Set materialization strategy for the benchmark"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("noria-benchmarks/piazza/basic-policies.json")
                .help("Security policies file for Piazza application"),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .default_value("5000")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("nlogged")
                .short("l")
                .default_value("5000")
                .help(
                    "Number of logged users. \
                     Must be less or equal than the number of users in the db",
                ),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .default_value("1000")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("1000000")
                .help("Number of posts in the db"),
        )
        .arg(
            Arg::with_name("private")
                .long("private")
                .default_value("0.05")
                .help("Percentage of private posts"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Enable verbose output"),
        )
        .arg(
            Arg::with_name("info")
                .short("i")
                .takes_value(true)
                .help("Directory to dump runtime process info (doesn't work on OSX)"),
        )
        .get_matches();
    let verbose = args.occurrences_of("verbose");
    let nusers = value_t_or_exit!(args, "nusers", usize);
    let nlogged = value_t_or_exit!(args, "nlogged", usize);
    let nclasses = value_t_or_exit!(args, "nclasses", usize);
    let nposts = value_t_or_exit!(args, "nposts", usize);
    let private = value_t_or_exit!(args, "private", f64);
    let runtime = Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let iloc = args.value_of("info").map(std::path::Path::new);

    assert!(nusers >= STUDENTS_PER_CLASS + TAS_PER_CLASS);
    assert!(nlogged <= nusers);
    assert!(private >= 0.0);
    assert!(private <= 1.0);

    if let Some(ref iloc) = iloc {
        if !iloc.exists() {
            std::fs::create_dir_all(iloc)
                .expect("failed to create directory for runtime process info");
        }
    }

    let log = if verbose != 0 {
        noria::logger_pls()
    } else {
        Logger::root(slog::Discard, o!())
    };

    info!(log, "starting up noria");
    debug!(log, "configuring noria");
    let mut g = Builder::default();
    match args.value_of("reuse").unwrap() {
        "finkelstein" => g.set_reuse(ReuseConfigType::Finkelstein),
        "full" => g.set_reuse(ReuseConfigType::Full),
        "noreuse" => g.set_reuse(ReuseConfigType::NoReuse),
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
    let mut rng = rand::thread_rng();
    debug!(log, "setting up initial schema");
    g.install_recipe(include_str!("schema.sql"))
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
    g.extend_recipe(include_str!("post-queries.sql"))
        .expect("failed to load queries");
    if let Some(gloc) = args.value_of("graph") {
        debug!(log, "extracing query graph");
        let gv = g.graphviz().expect("failed to read graphviz");
        std::fs::write(gloc, gv).expect("failed to save graphviz output");
    }
    debug!(log, "database schema setup done");

    info!(log, "starting db population");
    debug!(log, "creating users");
    let mut users = g.table("User").unwrap().into_sync();
    users
        .perform_all((1..=nusers).map(|uid| vec![uid.into()]))
        .unwrap();
    debug!(log, "creating classes");
    let mut classes = g.table("Class").unwrap().into_sync();
    classes
        .perform_all((1..=nclasses).map(|cid| vec![cid.into()]))
        .unwrap();
    debug!(log, "enrolling users in classes");
    let mut roles = g.table("Role").unwrap().into_sync();
    let mut uids: Vec<_> = (1..=nusers).collect();
    let mut enrolled = HashMap::new();
    let mut records = Vec::new();
    for cid in 1..=nclasses {
        // in each class, some number of users are tas, and the rest are students.
        uids.shuffle(&mut rng);
        records.extend(
            uids[0..(STUDENTS_PER_CLASS + TAS_PER_CLASS)]
                .iter()
                .enumerate()
                .map(|(i, &uid)| {
                    enrolled.entry(uid).or_insert_with(Vec::new).push(cid);
                    let role = if i < TAS_PER_CLASS { 1 } else { 0 };
                    vec![uid.into(), cid.into(), role.into()]
                }),
        );
    }
    roles.perform_all(records).unwrap();
    debug!(log, "writing posts");
    let mut posts = g.table("Post").unwrap().into_sync();
    posts
        .perform_all((1..=nposts).map(|pid| {
            let author = 1 + rng.gen_range(0, nusers);
            let cid = 1 + rng.gen_range(0, nclasses);
            let private = if rng.gen_bool(private) { 1 } else { 0 };
            let anon = 1;
            vec![
                pid.into(),
                cid.into(),
                author.into(),
                format!("post #{}", pid).into(),
                private.into(),
                anon.into(),
            ]
        }))
        .unwrap();
    debug!(log, "population completed");

    let mut stats = HashMap::new();

    info!(log, "logging in users");
    let mut login_times = Vec::with_capacity(nlogged);
    let mut login_stats = Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap();
    for uid in 1..=nlogged {
        trace!(log, "logging in user"; "uid" => uid);
        let user_context: HashMap<_, _> =
            std::iter::once(("id".to_string(), uid.to_string().into())).collect();
        let start = Instant::now();
        g.on_worker(|w| w.create_universe(user_context.clone()))
            .unwrap();
        login_times.push(start.elapsed());
        login_stats.saturating_record(login_times.last().unwrap().as_micros() as u64);

        // TODO: measure space use, which means doing reads on partial
        // TODO: or do we want to do that measurement separately?

        if let Some(ref iloc) = iloc {
            if uid % 100 == 0 {
                std::fs::copy(
                    "/proc/self/status",
                    iloc.join(format!("login-{}.status", uid)),
                )
                .expect("failed to extract process info");
            }
        }
    }
    stats.insert(Operation::Login, login_stats);

    info!(log, "creating api handles");
    debug!(log, "creating view handles for posts");
    let mut posts_view: Vec<_> = (1..=nusers)
        .map(|uid| {
            trace!(log, "creating posts handle for user"; "uid" => uid);
            g.view(format!("posts_u{}", uid)).unwrap().into_sync()
        })
        .collect();
    debug!(log, "creating view handles for post_count");
    let mut post_count_view: Vec<_> = (1..=nusers)
        .map(|uid| {
            trace!(log, "creating post count handle for user"; "uid" => uid);
            g.view(format!("post_count_u{}", uid)).unwrap().into_sync()
        })
        .collect();
    debug!(log, "all api handles created");
    let setup = init.elapsed();

    // now time to measure the cost of different operations
    info!(log, "starting benchmark");
    let mut n = 0;
    let mut pid = nposts + 1;
    let start = Instant::now();
    while start.elapsed() < runtime {
        let seed = rng.gen_range(0.0, 1.0);
        let operation = if seed < 0.95 {
            if seed < 0.95 / 2.0 {
                Operation::ReadPosts
            } else {
                Operation::ReadPostCount
            }
        } else {
            Operation::WritePost
        };

        let begin = match operation {
            Operation::ReadPosts => {
                trace!(log, "reading posts"; "seed" => seed);
                // TODO: maybe only select from eligible users?
                loop {
                    let uid = 1 + rng.gen_range(0, nusers);
                    trace!(log, "trying user"; "uid" => uid);
                    if let Some(cids) = enrolled.get(&uid) {
                        let cid = *cids.choose(&mut rng).unwrap();
                        trace!(log, "chose class"; "cid" => cid);
                        let start = Instant::now();
                        posts_view[uid - 1].lookup(&[cid.into()], true).unwrap();
                        break start;
                    }
                }
            }
            Operation::ReadPostCount => {
                trace!(log, "reading post count"; "seed" => seed);
                // TODO: maybe only select from eligible users?
                loop {
                    let uid = 1 + rng.gen_range(0, nusers);
                    trace!(log, "trying user"; "uid" => uid);
                    if let Some(cids) = enrolled.get(&uid) {
                        let cid = *cids.choose(&mut rng).unwrap();
                        trace!(log, "chose class"; "cid" => cid);
                        let start = Instant::now();
                        post_count_view[uid - 1]
                            .lookup(&[cid.into()], true)
                            .unwrap();
                        break start;
                    }
                }
            }
            Operation::WritePost => {
                trace!(log, "writing post"; "seed" => seed);
                loop {
                    let uid = 1 + rng.gen_range(0, nusers);
                    trace!(log, "trying user"; "uid" => uid);
                    if let Some(cids) = enrolled.get(&uid) {
                        let cid = *cids.choose(&mut rng).unwrap();
                        let private = if rng.gen_bool(private) { 1 } else { 0 };
                        trace!(log, "making post"; "cid" => cid, "private" => ?private);
                        let anon = 1;
                        let start = Instant::now();
                        posts
                            .insert(vec![
                                pid.into(),
                                cid.into(),
                                uid.into(),
                                format!("post #{}", pid).into(),
                                private.into(),
                                anon.into(),
                            ])
                            .unwrap();
                        pid += 1;
                        break start;
                    }
                }
            }
            Operation::Login => unreachable!(),
        };
        let took = begin.elapsed();
        n += 1;

        if start.elapsed() < runtime / 3 {
            // warm-up
            trace!(log, "dropping sample during warm-up"; "at" => ?start.elapsed(), "took" => ?took);
            continue;
        }

        trace!(log, "recording sample"; "took" => ?took);
        stats
            .entry(operation)
            .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
            .saturating_record(took.as_micros() as u64);

        if let Some(ref iloc) = iloc {
            if n % 1000 == 0 {
                std::fs::copy("/proc/self/status", iloc.join(format!("op-{}.status", n)))
                    .expect("failed to extract process info");
            }
        }
    }

    println!("# setup time: {:?}", setup);

    if let Ok(mem) = std::fs::read_to_string("/proc/self/statm") {
        let vmrss = mem.split_whitespace().nth(2 - 1).unwrap();
        let data = mem.split_whitespace().nth(6 - 1).unwrap();
        println!("# VmRSS: {} ", vmrss);
        println!("# VmData: {} ", data);
    }

    let mut i = 0;
    let stripe = login_times.len() / (runtime.as_secs() as usize);
    while i < login_times.len() {
        println!("# login sample[{}]: {:?}", i, login_times[i]);
        i += stripe;
    }
    println!("# number of operations: {}", n);
    println!("#\top\tpct\ttime");
    for &q in &[50, 95, 99, 100] {
        let mut keys: Vec<_> = stats.keys().collect();
        keys.sort();
        for op in keys {
            let stats = &stats[op];
            if q == 100 {
                println!("{}\t100\t{:.2}\tµs", op, stats.max());
            } else {
                println!(
                    "{}\t{}\t{:.2}\tµs",
                    op,
                    q,
                    stats.value_at_quantile(q as f64 / 100.0)
                );
            }
        }
    }
}
