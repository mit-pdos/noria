extern crate distributary;

mod populate;

extern crate clap;
extern crate slog;
extern crate slog_term;

use slog::DrainExt;

use distributary::{Blender, Recipe};

pub struct Backend {
    r: Option<Recipe>,
    g: Blender,
}

fn make() -> Box<Backend> {
    // set up graph
    let mut g = Blender::new();
    g.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let recipe = Recipe::blank();
    Box::new(Backend {
        r: Some(recipe),
        g: g,
    })
}

impl Backend {
    fn migrate(&mut self, recipe_file: &str) -> Result<(), String> {
        use std::io::Read;
        use std::fs::File;

        // migrate
        let mut mig = self.g.start_migration();

        let mut f = File::open(recipe_file).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        // HotCRP schema files have some DROP TABLE and DELETE queries, so skip those
        let s = s.lines()
            .filter(|l| !l.starts_with("DROP") && !l.starts_with("delete"))
            .take_while(|l| !l.contains("insert"))
            .collect::<Vec<_>>()
            .join("\n");
        let new_recipe = Recipe::from_str(&s)?;
        let cur_recipe = self.r.take().unwrap();
        let updated_recipe = match cur_recipe.replace(new_recipe) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig).unwrap();
                recipe
            }
            Err(e) => panic!(e),
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
    use std::str::FromStr;

    let matches = App::new("hotsoup")
        .version("0.1")
        .about("Soupy conference management system for your HotCRP needs.")
        .arg(Arg::with_name("graphs")
            .short("g")
            .help("Directory to dump graphs for each schema version into (if set)."))
        .arg(Arg::with_name("populate_from")
            .short("p")
            .required(true)
            .default_value("benchmarks/hotsoup/testdata")
            .help("Location of the HotCRP test data for population."))
        .arg(Arg::with_name("recipes")
            .short("r")
            .required(true)
            .default_value("benchmarks/hotsoup/schemas")
            .help("Location of the HotCRP schema recipes to move through."))
        .arg(Arg::with_name("transactional")
            .short("t")
            .help("Use transactional writes."))
        .get_matches();

    let rloc = matches.value_of("recipes").unwrap();
    let gloc = matches.value_of("graphs");
    let dataloc = matches.value_of("populate_from").unwrap();
    let transactional = matches.is_present("transactional");

    let mut backend = make();

    let mut files = Vec::new();
    for entry in fs::read_dir(rloc).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            files.push(entry.path());
        }
    }
    // hotcrp_*.sql
    files.sort_by_key(|k| {
        let fname = k.file_name().unwrap().to_str().unwrap();
        u64::from_str(&fname[7..fname.len() - 4]).unwrap()
    });

    let mut i = 0;
    for fname in files {
        println!("Loading HotCRP recipe from {:?}", fname);

        match backend.migrate(&fname.to_str().unwrap()) {
            Err(e) => panic!(e),
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
