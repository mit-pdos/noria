#![feature(duration_float)]

use clap::value_t_or_exit;
use futures::Stream;
use hdrhistogram::Histogram;
use noria::{Builder, FrontierStrategy, ReuseConfigType};
use rand::seq::SliceRandom;
use rand::Rng;
use slog::{crit, debug, error, info, o, trace, warn, Logger};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const STUDENTS_PER_CLASS: usize = 150;
const TAS_PER_CLASS: usize = 5;
const WRITE_CHUNK_SIZE: usize = 100;

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
                .help("Max number of seconds to run each stage of the benchmark for."),
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
                .default_value("1000")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("logged-in")
                .short("l")
                .default_value("1.0")
                .help("Fraction of users that are logged in."),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .default_value("100")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("100000")
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
        .get_matches();
    let verbose = args.occurrences_of("verbose");
    let nusers = value_t_or_exit!(args, "nusers", usize);
    let loggedf = value_t_or_exit!(args, "logged-in", f64);
    let nclasses = value_t_or_exit!(args, "nclasses", usize);
    let nposts = value_t_or_exit!(args, "nposts", usize);
    let private = value_t_or_exit!(args, "private", f64);
    let runtime = Duration::from_secs(value_t_or_exit!(args, "runtime", u64));

    assert!(nusers >= STUDENTS_PER_CLASS + TAS_PER_CLASS);
    assert!(loggedf >= 0.0);
    assert!(loggedf <= 1.0);
    let nlogged = (loggedf * nusers as f64) as usize;
    assert!(nlogged <= nusers);
    assert!(private >= 0.0);
    assert!(private <= 1.0);

    let log = if verbose != 0 {
        noria::logger_pls()
    } else {
        Logger::root(slog::Discard, o!())
    };

    println!("# nusers: {}", nusers);
    println!("# logged-in users: {}", nlogged);
    println!("# nclasses: {}", nclasses);
    println!("# nposts: {}", nposts);
    println!("# private: {:.1}%", private * 100.0);
    println!(
        "# materialization: {}",
        args.value_of("materialization").unwrap()
    );

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
    debug!(log, "database schema setup done");

    info!(log, "starting db population");
    debug!(log, "creating users"; "n" => nusers);
    let mut users = g.table("User").unwrap().into_sync();
    users
        .perform_all((1..=nusers).map(|uid| vec![uid.into()]))
        .unwrap();
    debug!(log, "creating classes"; "n" => nclasses);
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
    debug!(log, "writing posts"; "n" => nposts);
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

    let mut cold_stats = HashMap::new();
    let mut warm_stats = HashMap::new();

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

        if uid == 1 {
            memstats(&mut g, "firstuser");
        }

        if uid == 2 {
            if let Some(gloc) = args.value_of("graph") {
                debug!(log, "extracing query graph with two users");
                let gv = g.graphviz().expect("failed to read graphviz");
                std::fs::write(gloc, gv).expect("failed to save graphviz output");
            }
        }
    }
    cold_stats.insert(Operation::Login, login_stats);
    memstats(&mut g, "allusers");

    info!(log, "creating api handles");
    debug!(log, "creating view handles for posts");
    let mut posts_view: Vec<_> = (1..=nlogged)
        .map(|uid| {
            trace!(log, "creating posts handle for user"; "uid" => uid);
            g.view(format!("posts_u{}", uid)).unwrap().into_sync()
        })
        .collect();
    debug!(log, "creating view handles for post_count");
    let mut post_count_view: Vec<_> = (1..=nlogged)
        .map(|uid| {
            trace!(log, "creating post count handle for user"; "uid" => uid);
            g.view(format!("post_count_u{}", uid)).unwrap().into_sync()
        })
        .collect();
    debug!(log, "all api handles created");

    println!("# setup time: {:?}", init.elapsed());

    // now time to measure the cost of different operations
    info!(log, "starting cold read benchmarks");
    debug!(log, "cold reads of posts");
    let mut posts_reads = 0;
    let start = Instant::now();
    let mut requests = Vec::new();
    'ps_outer: for (&uid, cids) in &mut enrolled {
        cids.shuffle(&mut rng);
        for &cid in &*cids {
            if start.elapsed() >= runtime {
                debug!(log, "time limit reached"; "nreads" => posts_reads);
                break 'ps_outer;
            }

            trace!(log, "reading posts"; "uid" => uid, "cid" => cid);
            requests.push((Operation::ReadPosts, uid, cid));
            let begin = Instant::now();
            posts_view[uid - 1].lookup(&[cid.into()], true).unwrap();
            let took = begin.elapsed();
            posts_reads += 1;

            // NOTE: do we want a warm-up period/drop first sample per uid?
            // trace!(log, "dropping sample during warm-up"; "at" => ?start.elapsed(), "took" => ?took);

            trace!(log, "recording sample"; "took" => ?took);
            cold_stats
                .entry(Operation::ReadPosts)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }
    }

    debug!(log, "cold reads of post count");
    let mut post_count_reads = 0;
    let start = Instant::now();
    // re-randomize order of uids
    let mut enrolled: HashMap<_, _> = enrolled.into_iter().collect();
    'pc_outer: for (&uid, cids) in &mut enrolled {
        cids.shuffle(&mut rng);
        for &cid in &*cids {
            if start.elapsed() >= runtime {
                debug!(log, "time limit reached"; "nreads" => post_count_reads);
                break 'pc_outer;
            }

            trace!(log, "reading post count"; "uid" => uid, "cid" => cid);
            requests.push((Operation::ReadPostCount, uid, cid));
            let begin = Instant::now();
            post_count_view[uid - 1]
                .lookup(&[cid.into()], true)
                .unwrap();
            let took = begin.elapsed();
            post_count_reads += 1;

            // NOTE: do we want a warm-up period/drop first sample per uid?
            // trace!(log, "dropping sample during warm-up"; "at" => ?start.elapsed(), "took" => ?took);

            trace!(log, "recording sample"; "took" => ?took);
            cold_stats
                .entry(Operation::ReadPostCount)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }
    }

    info!(log, "starting warm read benchmarks");
    for (op, uid, cid) in requests {
        match op {
            Operation::ReadPosts => {
                trace!(log, "reading posts"; "uid" => uid, "cid" => cid);
            }
            Operation::ReadPostCount => {
                trace!(log, "reading post count"; "uid" => uid, "cid" => cid);
            }
            _ => unreachable!(),
        }

        let begin = Instant::now();
        match op {
            Operation::ReadPosts => {
                posts_view[uid - 1].lookup(&[cid.into()], true).unwrap();
            }
            Operation::ReadPostCount => {
                post_count_view[uid - 1]
                    .lookup(&[cid.into()], true)
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
    debug!(log, "performing reads to fill materializations");
    // have every user read every class so that we can measure total space overhead
    let start = Instant::now();
    let mut all = futures::stream::futures_unordered::FuturesUnordered::new();
    for (&uid, cids) in &enrolled {
        trace!(log, "reading all classes for user"; "uid" => uid, "classes" => ?cids);
        let cids: Vec<_> = cids.iter().map(|&cid| vec![cid.into()]).collect();
        all.push(
            posts_view[uid - 1]
                .clone()
                .into_async()
                .multi_lookup(cids.clone(), true),
        );
        all.push(
            post_count_view[uid - 1]
                .clone()
                .into_async()
                .multi_lookup(cids, true),
        );
    }
    let _ = all.wait().count();
    debug!(log, "filling done"; "took" => ?start.elapsed());
    memstats(&mut g, "end");

    info!(log, "performing write measurements");
    let mut pid = nposts + 1;
    let mut post_writes = 0;
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
                let anon = 1;
                writes.push(vec![
                    pid.into(),
                    cid.into(),
                    uid.into(),
                    format!("post #{}", pid).into(),
                    private.into(),
                    anon.into(),
                ]);
                pid += 1;
            }
        }

        let begin = Instant::now();
        posts.perform_all(writes).unwrap();
        let took = begin.elapsed();
        post_writes += 1;

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

    // TODO: do we also want to measure writes when we _haven't_ read all the keys?

    let mut i = 0;
    let stripe = login_times.len() / (runtime.as_secs() as usize);
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
    println!(
        "# number of cold posts reads (in {:?}): {}",
        runtime, posts_reads
    );
    println!(
        "# number of cold post count reads (in {:?}): {}",
        runtime, post_count_reads
    );
    println!(
        "# number of post writes (in {:?}): {}",
        runtime,
        post_writes * WRITE_CHUNK_SIZE
    );
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
