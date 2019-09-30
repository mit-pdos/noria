#![feature(type_ascription)]

#[macro_use]
extern crate clap;
extern crate noria;
#[macro_use]
extern crate mysql;
extern crate rand;

use mysql as my;
use noria::DataType;
use std::time;

#[macro_use]
mod populate;
use populate::Populate;

struct Backend {
    pool: mysql::Pool,
}

impl Backend {
    fn new(addr: &str) -> Backend {
        Backend {
            pool: my::Pool::new_manual(1, 1, addr).unwrap(),
        }
    }

    pub fn read(&self, uid: i32) {
        let qstring = format!(
            "SELECT p_author, COUNT(p_id) FROM Post WHERE p_author={} GROUP BY p_author",
            uid
        );
        self.pool.prep_exec(qstring, ()).unwrap();
    }

    pub fn secure_read(&self, uid: i32, logged_uid: i32) {
        let qstring = format!(
            "SELECT p_author, count(p_id) FROM Post \
                WHERE \
                p_author = {} AND \
                (
                    (Post.p_private = 1 AND Post.p_author = {}) OR \
                    (Post.p_private = 1 AND Post.p_cid in (SELECT r_cid FROM Role WHERE r_role = 1 AND Role.r_uid = {})) OR \
                    (Post.p_private = 0 AND Post.p_cid in (SELECT r_cid FROM Role WHERE r_role = 0 AND Role.r_uid = {})) \
                ) \
                GROUP BY p_author",
            uid,
            logged_uid,
            logged_uid,
            logged_uid
        );

        self.pool.prep_exec(qstring, ()).unwrap();
    }

const STUDENTS_PER_CLASS: usize = 150;
const TAS_PER_CLASS: usize = 5;
const WRITE_CHUNK_SIZE: usize = 100;
const CLASSES_PER_STUDENT: usize = 4;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
enum Operation {
    ReadPostCount,
    ReadPosts,
    WritePost,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Operation::ReadPostCount => write!(f, "pcount"),
            Operation::ReadPosts => write!(f, "posts"),
            Operation::WritePost => write!(f, "post"),
        }
    }
}

fn main() {
    use clap::{App, Arg, ArgGroup};
    let args = App::new("piazza-mysql")
        .version("0.1")
        .about("Benchmarks Piazza-like application with security policies.")
        .arg(
            Arg::with_name("runtime")
                .long("runtime")
                .short("r")
                .default_value("10")
                .help("Max number of seconds to run each stage of the benchmark for."),
        )
        .arg(
            Arg::with_name("mysql")
                .takes_value(true)
                .required(true)
                .help("MySQL address"),
        )
        .group(
            ArgGroup::with_name("n")
                .args(&["nusers", "nclasses"])
                .required(true),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .takes_value(true)
                .help("Number of users in the db (# clases is computed)"),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .takes_value(true)
                .help("Number of classes in the db (# users is computed)"),
        )
        .arg(
            Arg::with_name("logged-in")
                .short("l")
                .default_value("1.0")
                .help("Fraction of users that are logged in."),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("1000")
                .help("Number of posts per class in the db"),
        )
        .arg(
            Arg::with_name("private")
                .long("private")
                .default_value("0.05")
                .help("Percentage of private posts"),
        )
        .arg(
            Arg::with_name("iter")
                .long("iter")
                .default_value("1")
                .help("Number of iterations to run"),
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
    let nposts = value_t_or_exit!(args, "nposts", usize);
    let private = value_t_or_exit!(args, "private", f64);
    let runtime = Duration::from_secs(value_t_or_exit!(args, "runtime", u64));

    let (nusers, nclasses) = if args.is_present("nusers") {
        let nusers = value_t_or_exit!(args, "nusers", usize);
        // how many classes should there be?
        // well, there should be 4 classes per student,
        // and 150 students per class,
        // but the classes can overlap!
        // so we work with expected values
        //
        // the expected number of students per class
        //  = nusers * chance one students picks that class
        //
        // chance one student picks a class (assuming disjoint)
        //  = classes per student / nclasses
        //
        // rearranging gives us
        //
        //   spc = #u * cps / #c
        //    #c = #u * cps / spc
        let nclasses =
            ((nusers * CLASSES_PER_STUDENT) as f64 / STUDENTS_PER_CLASS as f64).ceil() as usize;
        (nusers, nclasses)
    } else if args.is_present("nclasses") {
        let nclasses = value_t_or_exit!(args, "nclasses", usize);
        // solving for #c instead of #u above gives us
        //
        //   spc = #u * cps / #c
        //    #u = spc * #c / cps
        let nusers =
            ((STUDENTS_PER_CLASS * nclasses) as f64 / CLASSES_PER_STUDENT as f64).ceil() as usize;
        (nusers, nclasses)
    } else {
        unreachable!();
    };
    let nposts = nposts * nclasses;
    let mysql = args.value_of("mysql").unwrap();

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
    println!("# materialization: mysql");

    let mut rng = rand::thread_rng();
    let mut cold_stats = HashMap::new();
    let mut warm_stats = HashMap::new();
    let iter = value_t_or_exit!(args, "iter", usize);
    for i in 1..=iter {
        let init = Instant::now();
        info!(log, "setting up database schema"; "iteration" => i);

        debug!(log, "connecting to MySQL");
        let mut opts = mysql::OptsBuilder::from_opts(mysql);
        // we use tcp, so must they
        opts.prefer_socket(false);
        // start with no db in case it hasn't been created yet
        opts.db_name(None::<String>);
        opts.init(vec![
            "SET max_heap_table_size = 4294967296;",
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
        ]);
        let mut m = mysql::Conn::new(opts).expect("failed to connect to MySQL");
        let db = &mysql[mysql.rfind('/').unwrap() + 1..];

        debug!(log, "cycling database"; "db" => db);
        if m.query(format!("USE {}", db)).is_ok() {
            trace!(log, "dropping old database");
            m.query(format!("DROP DATABASE {}", db))
                .expect("failed to drop database");
        }
        m.query(format!("CREATE DATABASE {}", db))
            .expect("failed to create database");
        m.query(format!("USE {}", db))
            .expect("failed to switch to new database");

        debug!(log, "setting up tables");
        // TODO: can we just import schema.sql here too?
        m.prep_exec(
            "CREATE TABLE Post ( \
             p_id int(11) NOT NULL, \
             p_cid int(11) NOT NULL, \
             p_author int(11) NOT NULL, \
             p_content varchar(258) NOT NULL, \
             p_private tinyint(1) NOT NULL default '0', \
             PRIMARY KEY (p_id), \
             KEY p_cid (p_cid), \
             KEY p_author (p_author) \
             ) ENGINE = MEMORY;",
            (),
        )
        .unwrap();
        m.prep_exec(
            "CREATE TABLE User ( \
             u_id int(11) NOT NULL, \
             PRIMARY KEY  (u_id) \
             ) ENGINE = MEMORY;",
            (),
        )
        .unwrap();
        m.prep_exec(
            "CREATE TABLE Class ( \
             c_id int(11) NOT NULL, \
             PRIMARY KEY  (c_id) \
             ) ENGINE = MEMORY;",
            (),
        )
        .unwrap();
        m.prep_exec(
            "CREATE TABLE Role ( \
             r_uid int(11) NOT NULL, \
             r_cid int(11) NOT NULL, \
             r_role tinyint(1) NOT NULL default '0', \
             PRIMARY KEY (r_uid, r_cid) \
             ) ENGINE = MEMORY;",
            (),
        )
        .unwrap();

        info!(log, "starting db population");
        debug!(log, "creating users"; "n" => nusers);
        let ps: Vec<_> = (1..=nusers).map(|_| "(?)").collect();
        let q = format!("INSERT INTO User (u_id) VALUES {}", ps.join(","));
        m.prep_exec(q, (1..=nusers).collect::<Vec<_>>())
            .expect("failed to populate users");
        debug!(log, "creating classes"; "n" => nclasses);
        let ps: Vec<_> = (1..=nclasses).map(|_| "(?)").collect();
        let q = format!("INSERT INTO Class (c_id) VALUES {}", ps.join(","));
        m.prep_exec(q, (1..=nclasses).collect::<Vec<_>>())
            .expect("failed to populate classes");
        debug!(log, "enrolling users in classes");
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
                    .flat_map(|(i, &uid)| {
                        enrolled.entry(uid).or_insert_with(Vec::new).push(cid);
                        let role = if i < TAS_PER_CLASS { 1 } else { 0 };
                        vec![uid, cid, role]
                    }),
            );
        }
        let ps = (0..(records.len() / 3))
            .map(|_| "(?, ?, ?)")
            .collect::<Vec<_>>()
            .join(",");
        m.prep_exec(
            format!("INSERT INTO Role (r_uid, r_cid, r_role) VALUES {}", ps),
            records,
        )
        .expect("failed to enroll students");
        debug!(log, "writing posts"; "n" => nposts);
        let records: Vec<_> = (1..=nposts)
            .map(|pid| {
                let author = 1 + rng.gen_range(0, nusers);
                let cid = 1 + rng.gen_range(0, nclasses);
                let private = if rng.gen_bool(private) { 1 } else { 0 };
                vec![
                    mysql::Value::from(pid),
                    mysql::Value::from(cid),
                    mysql::Value::from(author),
                    mysql::Value::from(format!("post #{}", pid)),
                    mysql::Value::from(private),
                ]
            })
            .collect();
        for vs in records.chunks(WRITE_CHUNK_SIZE) {
            let ps = (0..vs.len())
                .map(|_| "(?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(",");

            let vs: Vec<_> = vs.iter().flat_map(|vvs| vvs.iter()).collect();
            m.prep_exec(
                format!(
                    "INSERT INTO Post (p_id, p_cid, p_author, p_content, p_private) VALUES {}",
                    ps
                ),
                vs,
            )
            .expect("failed to post");
        }

        debug!(log, "population completed");
        println!("# setup time: {:?}", init.elapsed());

        // now time to measure the cost of different operations
        info!(log, "starting cold read benchmarks");
        debug!(log, "cold reads of posts");
        // TODO: should mysql also get to prepare per user queries?
        let posts_query = "SELECT * FROM Post WHERE p_cid = ? AND (\
                    (p_private = 1 AND p_author = ?) OR \
                    (p_private = 1 AND p_cid in (SELECT r_cid FROM Role WHERE r_role = 1 AND r_uid = ?)) OR \
                    (p_private = 0 AND p_cid in (SELECT r_cid FROM Role WHERE r_role = 0 AND r_uid = ?)))";
        let mut posts_view = m
            .prepare(posts_query)
            .expect("failed to prepare posts view query");
        let mut posts_reads = 0;
        let start = Instant::now();
        let mut requests = Vec::new();
        'ps_outer: for (&uid, cids) in &mut enrolled {
            if uid > nlogged {
                // since users are randomly assigned to classes,
                // this should still give us a random sampling.
                continue;
            }

            cids.shuffle(&mut rng);
            for &cid in &*cids {
                if start.elapsed() >= runtime {
                    debug!(log, "time limit reached"; "nreads" => posts_reads);
                    break 'ps_outer;
                }

                trace!(log, "reading posts"; "uid" => uid, "cid" => cid);
                requests.push((Operation::ReadPosts, uid, cid));
                let begin = Instant::now();
                posts_view
                    .execute(vec![
                        mysql::Value::from(cid),
                        mysql::Value::from(uid),
                        mysql::Value::from(uid),
                        mysql::Value::from(uid),
                    ])
                    .unwrap();
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
        drop(posts_view);

        debug!(log, "cold reads of post count");
        let post_count_query =
            "SELECT p_cid, count(p_id) FROM Post \
                WHERE p_cid = ? AND (\
                    (p_private = 1 AND p_author = ?) OR \
                    (p_private = 1 AND p_cid in (SELECT r_cid FROM Role WHERE r_role = 1 AND r_uid = ?)) OR \
                    (p_private = 0 AND p_cid in (SELECT r_cid FROM Role WHERE r_role = 0 AND r_uid = ?))) \
                GROUP BY p_cid";
        let mut post_count_view = m
            .prepare(post_count_query)
            .expect("failed to prepare post count query");
        let mut post_count_reads = 0;
        let start = Instant::now();
        // re-randomize order of uids
        let mut enrolled: HashMap<_, _> = enrolled.into_iter().collect();
        'pc_outer: for (&uid, cids) in &mut enrolled {
            if uid > nlogged {
                continue;
            }

            cids.shuffle(&mut rng);
            for &cid in &*cids {
                if start.elapsed() >= runtime {
                    debug!(log, "time limit reached"; "nreads" => post_count_reads);
                    break 'pc_outer;
                }

                trace!(log, "reading post count"; "uid" => uid, "cid" => cid);
                requests.push((Operation::ReadPostCount, uid, cid));
                let begin = Instant::now();
                post_count_view
                    .execute(vec![
                        mysql::Value::from(cid),
                        mysql::Value::from(uid),
                        mysql::Value::from(uid),
                        mysql::Value::from(uid),
                    ])
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
        drop(post_count_view);

        info!(log, "starting warm read benchmarks");
        let mut view = m.prepare(posts_query).unwrap();
        for &(op, uid, cid) in &requests {
            if let Operation::ReadPostCount = op {
                break;
            }

            trace!(log, "reading posts"; "uid" => uid, "cid" => cid);
            let begin = Instant::now();
            view.execute(vec![
                mysql::Value::from(cid),
                mysql::Value::from(uid),
                mysql::Value::from(uid),
                mysql::Value::from(uid),
            ])
            .unwrap();
            let took = begin.elapsed();

            // NOTE: no warm-up for "warm" reads

            trace!(log, "recording sample"; "took" => ?took);
            warm_stats
                .entry(op)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }
        drop(view);
        let mut view = m.prepare(post_count_query).unwrap();
        for &(op, uid, cid) in &requests {
            if let Operation::ReadPosts = op {
                continue;
            }

            trace!(log, "reading post count"; "uid" => uid, "cid" => cid);
            let begin = Instant::now();
            view.execute(vec![
                mysql::Value::from(cid),
                mysql::Value::from(uid),
                mysql::Value::from(uid),
                mysql::Value::from(uid),
            ])
            .unwrap();
            let took = begin.elapsed();

            // NOTE: no warm-up for "warm" reads

            trace!(log, "recording sample"; "took" => ?took);
            warm_stats
                .entry(op)
                .or_insert_with(|| Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap())
                .saturating_record(took.as_micros() as u64);
        }
        drop(view);

        info!(log, "performing write measurements");
        let vs = (0..WRITE_CHUNK_SIZE)
            .map(|_| "(?, ?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(",");
        let mut post = m
            .prepare(format!(
                "INSERT INTO Post (p_id, p_cid, p_author, p_content, p_private) VALUES {}",
                vs
            ))
            .expect("failed to prepare post statement");
        let mut pid = nposts + 1;
        let start = Instant::now();
        while start.elapsed() < runtime {
            trace!(log, "writing post");
            let mut writes = Vec::new();
            while writes.len() < 5 * WRITE_CHUNK_SIZE {
                let uid = 1 + rng.gen_range(0, nlogged);
                trace!(log, "trying user"; "uid" => uid);
                if let Some(cids) = enrolled.get(&uid) {
                    let cid = *cids.choose(&mut rng).unwrap();
                    let private = if rng.gen_bool(private) { 1 } else { 0 };
                    trace!(log, "making post"; "cid" => cid, "private" => ?private);
                    writes.extend(vec![
                        mysql::Value::from(pid),
                        mysql::Value::from(cid),
                        mysql::Value::from(uid),
                        mysql::Value::from(format!("post #{}", pid)),
                        mysql::Value::from(private),
                    ]);
                    pid += 1;
                }
            }

            let begin = Instant::now();
            post.execute(writes).unwrap();
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
