extern crate distributary;
extern crate rand;

extern crate clap;
extern crate slog;

use distributary::{Blender, DataType, ReuseConfigType, ControllerBuilder};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};

pub struct Backend {
    g: Blender,
}

impl Backend {
    pub fn new(partial: bool, shard: bool, reuse: &str) -> Backend {
        let mut cb = ControllerBuilder::default();
        let log = distributary::logger_pls();
        let blender_log = log.clone();

        if !partial {
            cb.disable_partial();
        }

        if !shard {
            cb.disable_sharding();
        }

        let mut g = cb.build();
        g.log_with(blender_log);

        match reuse.as_ref() {
            "finkelstein" => g.enable_reuse(ReuseConfigType::Finkelstein),
            "full" => g.enable_reuse(ReuseConfigType::Full),
            "noreuse" => g.enable_reuse(ReuseConfigType::NoReuse),
            "relaxed" => g.enable_reuse(ReuseConfigType::Relaxed),
            _ => panic!("reuse configuration not supported"),
        }

        Backend {
            g: g,
        }
    }

    fn login(&mut self, user_context: HashMap<String, DataType>) -> Result<(), String> {
        self.g.create_universe(user_context.clone());

        self.write_to_user_context(user_context);
        Ok(())
    }

    fn write_to_user_context(&self, uc: HashMap<String, DataType>) {
        let name = &format!("UserContext_{}", uc.get("id").unwrap());
        let r: Vec<DataType> = uc.values().cloned().collect();
        let ins = self.g.inputs();
        let mut mutator = self
            .g
            .get_mutator(ins[name])
            .unwrap();

        mutator.put(r).unwrap();
    }

    fn migrate(
        &mut self,
        schema_file: &str,
        query_file: Option<&str>,
        policy_file: Option<&str>,
    ) -> Result<(), String> {
        use std::fs::File;
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

        // Read policy file
        match policy_file {
            None => (),
            Some(pf) => {
                let mut p = String::new();
                let mut pf = File::open(pf).unwrap();
                pf.read_to_string(&mut p).unwrap();

                // Install recipe with policies
                self.g.install_recipe_with_policies(rs, p);

                return Ok(())
            }
        };

        // Install recipe
        self.g.install_recipe(rs);

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
    let args = App::new("SecureCRP")
        .version("0.1")
        .about("Benchmarks HotCRP-like application with security policies.")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("benchmarks/securecrp/schema.sql")
                .help("SQL schema file"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("benchmarks/securecrp/queries.sql")
                .help("SQL query file"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("benchmarks/securecrp/policies.json")
                .help("Security policies file"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("graph.gv")
                .help("File to dump graph"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
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
            Arg::with_name("populate")
                .long("populate")
                .help("Populate app with randomly generated data"),
        )
        .get_matches();


    println!("Starting SecureCRP...");

    // Read arguments
    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();
    let gloc = args.value_of("graph");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let reuse = args.value_of("reuse").unwrap();

    let mut backend = Backend::new(partial, shard, reuse);
    backend.migrate(sloc, Some(qloc), Some(ploc)).unwrap();

    backend.login(make_user(1)).is_ok();

    if gloc.is_some() {
        let graph_fname = gloc.unwrap();
        let mut gf = File::create(graph_fname).unwrap();
        assert!(write!(gf, "{}", backend.g.graphviz()).is_ok());
    }

    // sleep "forever"
    thread::sleep(time::Duration::from_millis(200000000));
}
