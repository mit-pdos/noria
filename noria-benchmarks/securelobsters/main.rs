#[macro_use]
extern crate clap;

use noria::{ControllerBuilder, DataType, LocalAuthority, LocalControllerHandle, ReuseConfigType};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }};
}

pub struct Backend {
    g: LocalControllerHandle<LocalAuthority>,
}

impl Backend {
    pub fn new(partial: bool, shard: bool) -> Backend {
        let mut cb = ControllerBuilder::default();
        let log = noria::logger_pls();
        let blender_log = log.clone();

        if !partial {
            cb.disable_partial();
        }
        cb.set_reuse(ReuseConfigType::Full);
        cb.log_with(blender_log);

        let g = cb.build_local().unwrap();

        Backend { g: g }
    }

    pub fn populate(&mut self, name: &'static str, mut records: Vec<Vec<DataType>>) -> usize {
        let mut mutator = self.g.table(name).unwrap();

        let start = time::Instant::now();

        let i = records.len();
        for r in records.drain(..) {
            mutator.insert(r).unwrap();
        }

        let dur = dur_to_fsec!(start.elapsed());
        println!(
            "Inserted {} {} in {:.2}s ({:.2} PUTs/sec)!",
            i,
            name,
            dur,
            i as f64 / dur
        );

        i
    }

    fn login(&mut self, user_context: HashMap<String, DataType>) -> Result<(), String> {
        self.g.create_universe(user_context.clone());

        Ok(())
    }

    fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.set_security_config(config);
    }

    fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
        use std::io::Read;

        // Read schema file
        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();
        sf.read_to_string(&mut s).unwrap();

        let mut rs = s.clone();
        s.clear();

        // Read query file
        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s);
            }
        }

        // Install recipe
        self.g.install_recipe(&rs).unwrap();

        Ok(())
    }
}

fn make_user(id: i32) -> HashMap<String, DataType> {
    let mut user = HashMap::new();
    user.insert(String::from("id"), id.into());

    user
}

fn main() {
    use clap::{App, Arg};
    let args = App::new("Secure Lobsters")
        .version("0.1")
        .about("Lobsters-like application with security policies.")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("lobsters-schema.txt")
                .help("Schema file for Lobsters application"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("lobsters-queries.txt")
                .help("Query file for Lobsters application"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("noria-benchmarks/securelobsters/policies.json")
                .help("Security policies file for Lobsters application"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("shard")
                .long("shard")
                .help("Enable sharding"),
        )
        .arg(
            Arg::with_name("partial")
                .long("partial")
                .help("Enable partial materialization"),
        )
        .arg(
            Arg::with_name("users")
                .short("u")
                .default_value("1000")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("sessions")
                .short("s")
                .default_value("1000")
                .help(
                "Number of logged users. Must be less or equal than the number of users in the db",
            ),
        )
        .get_matches();

    println!("Starting benchmark...");

    // Read arguments
    let rloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();
    let gloc = args.value_of("graph");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let nusers = value_t_or_exit!(args, "users", i32);
    let nsessions = value_t_or_exit!(args, "sessions", i32);

    assert!(
        nsessions <= nusers,
        "--users must be greater or equal to --sessions"
    );

    // Initiliaze backend application with some queries and policies
    println!("Initializing database schema...");
    let mut backend = Backend::new(partial, shard);
    backend.migrate(sloc, None).unwrap();

    backend.set_security_config(ploc);
    backend.migrate(qloc, Some(sloc)).unwrap();

    thread::sleep(time::Duration::from_millis(2000));

    //backend.populate("User", users);

    // TODO populate/read

    // Login a user
    println!("Starting user sessions...");
    for i in 0..nsessions {
        let start = time::Instant::now();
        backend.login(make_user(i)).is_ok();
        let dur = dur_to_fsec!(start.elapsed());
        println!("Migration {} took {:.2}s!", i, dur,);
    }

    println!("Done.");

    if gloc.is_some() {
        let graph_fname = gloc.unwrap();
        let mut gf = File::create(graph_fname).unwrap();
        assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
    }
}
