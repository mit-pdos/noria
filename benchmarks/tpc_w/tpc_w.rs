extern crate distributary;

mod populate;

extern crate clap;
extern crate slog;
extern crate slog_term;

use std::{thread, time};
use slog::DrainExt;

use distributary::{Blender, Recipe};

pub struct Backend {
    r: Recipe,
    g: Blender,
}

fn make(recipe_location: &str) -> Box<Backend> {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = Blender::new();
    g.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let recipe;
    {
        // migrate
        let mut mig = g.start_migration();

        let mut f = File::open(recipe_location).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        recipe = match Recipe::from_str(&s) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig).unwrap();
                recipe
            }
            Err(e) => panic!(e),
        };

        mig.commit();
    }

    //println!("{}", g);
    Box::new(Backend {
        //data: Some(g.get_mutator(data)),
        r: recipe,
        g: g,
    })

}

fn main() {
    use clap::{Arg, App};
    use populate::*;

    let matches = App::new("tpc_w")
        .version("0.1")
        .about("Soup TPC-W driver.")
        .arg(Arg::with_name("recipe")
            .short("r")
            .required(true)
            .default_value("tests/tpc-w-queries.txt")
            .help("Location of the TPC-W recipe file."))
        .arg(Arg::with_name("populate_from")
            .short("p")
            .required(true)
            .default_value("benchmarks/tpc_w/data")
            .help("Location of the data files for TPC-W prepopulation."))
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let ploc = matches.value_of("populate_from").unwrap();

    println!("Loading TPC-W recipe from {}", rloc);
    let backend = make(&rloc);

    println!("Prepopulating from data files in {}", ploc);
    populate_addresses(&backend, &ploc);
    populate_authors(&backend, &ploc);
    populate_countries(&backend, &ploc);
    populate_customers(&backend, &ploc);
    //populate_items(&backend, &ploc);
    populate_orders(&backend, &ploc);

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
}
