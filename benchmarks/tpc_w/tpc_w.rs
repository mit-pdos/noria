extern crate distributary;

#[macro_use]
extern crate clap;
extern crate slog;
extern crate slog_term;

use std::{thread, time};
use slog::DrainExt;

use distributary::{Blender, Base, Aggregation, Mutator, Recipe, Token, JoinBuilder};

struct Backend {
    r: Recipe,
    g: Blender,
}

fn make(recipe_location: &str) -> Box<Backend> {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = Blender::new();
    g.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let mut recipe;
    {
        // migrate
        let mut mig = g.start_migration();

        let mut f = File::open(recipe_location).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        recipe = match Recipe::from_str(&s) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig);
                recipe
            }
            Err(e) => panic!(e),
        };

        mig.commit();
    }

    println!("{}", g);
    Box::new(Backend {
        //data: Some(g.get_mutator(data)),
        r: recipe,
        g: g,
    })

}

fn main() {
    use clap::{Arg, App};
    let matches = App::new("tpc_w")
        .version("0.1")
        .about("Soup TPC-W driver.")
        .arg(Arg::with_name("recipe")
            .short("r")
            .required(true)
            .default_value("tests/tpc-w-queries.txt")
            .help("Location of the TPC-W recipe file."))
        .get_matches();

    let rloc = value_t_or_exit!(matches, "recipe", String);

    println!("Loading TPC-W recipe from {}", rloc);

    let mut backend = make(&rloc);

    //let data_putter = backend.data.take().unwrap();

    println!("Writing...");
    //data_putter.transactional_put(vec![batch_size.into(), y.into()], Token::empty());

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
}
