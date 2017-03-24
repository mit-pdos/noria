extern crate distributary;

mod populate;

extern crate clap;
extern crate slog;
extern crate slog_term;

use slog::DrainExt;

use distributary::{Blender, Recipe};

pub struct Backend {
    blacklist: Vec<String>,
    r: Option<Recipe>,
    g: Blender,
}

fn make(blacklist: &str) -> Box<Backend> {
    use std::io::Read;
    use std::fs::File;

    // load query blacklist
    let mut bf = File::open(blacklist).unwrap();
    let mut s = String::new();
    bf.read_to_string(&mut s).unwrap();
    let blacklisted_queries = s.lines()
        .filter(|l| !l.is_empty() && !l.starts_with("#"))
        .map(|l| String::from(l.split(":").next().unwrap()))
        .map(|l| String::from(l.split("_").nth(1).unwrap()))
        .collect();

    // set up graph
    let mut g = Blender::new();
    g.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let recipe = Recipe::blank();
    Box::new(Backend {
                 blacklist: blacklisted_queries,
                 r: Some(recipe),
                 g: g,
             })
}

impl Backend {
    fn migrate(&mut self, schema_file: &str, query_file: Option<&str>) -> Result<(), String> {
        use std::io::Read;
        use std::fs::File;

        let ref blacklist = self.blacklist;
        // migrate
        let mut mig = self.g.start_migration();

        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();

        // load schema
        sf.read_to_string(&mut s).unwrap();
        // HotCRP schema files have some DROP TABLE and DELETE queries, so skip those
        let mut rs = s.lines()
            .filter(|l| !l.starts_with("DROP") && !l.starts_with("delete"))
            .take_while(|l| !l.contains("insert"))
            .collect::<Vec<_>>()
            .join("\n");
        // load queries and concatenate them onto the table definitions from the schema
        s.clear();
        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s.lines()
                                 .filter(|ref l| {
                    // make sure to skip blacklisted queries
                    for ref q in blacklist {
                        if l.contains(*q) {
                            return false;
                        }
                    }
                    true
                })
                                 .collect::<Vec<_>>()
                                 .join("\n"))
            }
        }

        let new_recipe = Recipe::from_str(&rs)?;
        let cur_recipe = self.r.take().unwrap();
        let updated_recipe = match cur_recipe.replace(new_recipe) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig).unwrap();
                recipe
            }
            Err(e) => return Err(format!("failed to activate recipe: {}", e)),
        };

        mig.commit();
        self.r = Some(updated_recipe);
        Ok(())
    }
}

fn main() {
    use clap::{Arg, App};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;

    let matches = App::new("hotsoup")
        .version("0.1")
        .about("Soupy conference management system for your HotCRP needs.")
        .arg(Arg::with_name("graphs")
            .short("g")
            .value_name("DIR")
            .help("Directory to dump graphs for each schema version into (if set)."))
        .arg(Arg::with_name("populate_from")
            .short("p")
            .required(true)
            .default_value("benchmarks/hotsoup/testdata")
            .help("Location of the HotCRP test data for population."))
        .arg(Arg::with_name("schemas")
            .short("s")
            .required(true)
            .default_value("benchmarks/hotsoup/schemas")
            .help("Location of the HotCRP query recipes to move through."))
        .arg(Arg::with_name("queries")
            .short("q")
            .required(true)
            .default_value("benchmarks/hotsoup/queries")
            .help("Location of the HotCRP schema recipes to move through."))
        .arg(Arg::with_name("blacklist")
            .short("b")
            .default_value("benchmarks/hotsoup/query_blacklist.txt")
            .help("File with blacklisted queries to skip."))
        .arg(Arg::with_name("base_only")
            .long("base_only")
            .help("Only add base tables, not queries."))
        .arg(Arg::with_name("transactional")
            .short("t")
            .help("Use transactional writes."))
        .get_matches();

    let blloc = matches.value_of("blacklist").unwrap();
    let gloc = matches.value_of("graphs");
    let sloc = matches.value_of("schemas").unwrap();
    let qloc = matches.value_of("queries").unwrap();
    let dataloc = matches.value_of("populate_from").unwrap();
    let transactional = matches.is_present("transactional");
    let base_only = matches.is_present("base_only");

    let mut backend = make(blloc);

    let mut query_files = Vec::new();
    let mut schema_files = Vec::new();

    for entry in fs::read_dir(qloc).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            query_files.push(entry.path());
        }
    }

    for entry in fs::read_dir(sloc).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            schema_files.push(entry.path());
        }
    }

    // hotcrp_*.sql
    query_files.sort_by_key(|k| {
        let fname = k.file_name()
            .unwrap()
            .to_str()
            .unwrap();
        u64::from_str(&fname[7..fname.len() - 4]).unwrap()
    });
    schema_files.sort_by_key(|k| {
        let fname = k.file_name()
            .unwrap()
            .to_str()
            .unwrap();
        u64::from_str(&fname[7..fname.len() - 4]).unwrap()
    });

    let files: Vec<(PathBuf, PathBuf)> = schema_files.into_iter().zip(query_files).collect();

    let mut i = 0;
    for (sfname, qfname) in files {
        println!("Loading HotCRP schema from {:?}, queries from {:?}",
                 sfname,
                 qfname);

        let queries = if base_only {
            None
        } else {
            Some(qfname.to_str().unwrap())
        };
        match backend.migrate(&sfname.to_str().unwrap(), queries) {
            Err(e) => {
                let graph_fname = format!("{}/failed_hotcrp_{}.gv", gloc.unwrap(), i);
                let mut gf = File::create(graph_fname).unwrap();
                assert!(write!(gf, "{}", backend.g).is_ok());
                panic!(e)
            }
            _ => (),
        }

        if gloc.is_some() {
            let graph_fname = format!("{}/hotcrp_{}.gv", gloc.unwrap(), i);
            let mut gf = File::create(graph_fname).unwrap();
            assert!(write!(gf, "{}", backend.g).is_ok());
        }

        i += 1;
    }

    // Populate with test data at latest schema
    populate::populate(&backend, dataloc, transactional).unwrap();

    println!("{}", backend.g);
}
