extern crate chrono;
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

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }}
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
        .arg(Arg::with_name("transactional")
            .short("t")
            .help("Use transactional writes."))
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let ploc = matches.value_of("populate_from").unwrap();
    let transactional = matches.is_present("transactional");

    println!("Loading TPC-W recipe from {}", rloc);
    let backend = make(&rloc);

    println!("Prepopulating from data files in {}", ploc);
    populate_addresses(&backend, &ploc, transactional);
    populate_authors(&backend, &ploc, transactional);
    populate_countries(&backend, &ploc, transactional);
    populate_customers(&backend, &ploc, transactional);
    populate_items(&backend, &ploc, transactional);
    populate_orders(&backend, &ploc, transactional);
    populate_cc_xacts(&backend, &ploc, transactional);
    populate_order_line(&backend, &ploc, transactional);

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
    for nq in backend.r.aliases().iter() {
        println!("{}", nq);
        match backend.r.node_addr_for(nq) {
            Err(_) => println!("no node!"),
            Ok(nd) => {
                let g = backend.g.get_getter(nd).unwrap();
                let start = time::Instant::now();
                for _ in 0..1_000_000 {
                    drop(g(&0.into()));
                }
                let dur = dur_to_fsec!(start.elapsed());
                println!("Took {:.2}s ({:.2} GETs/sec)!",
                         dur,
                         f64::from(1_000_000) / dur);
            }
        }
    }
}
