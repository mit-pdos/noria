extern crate chrono;
extern crate distributary;

//mod populate;

extern crate clap;
extern crate slog;
extern crate slog_term;

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
        // the default HotCRP schema file has some DROP TABLE queries, so skip those
        let s = s.lines().filter(|l| !l.starts_with("DROP")).collect::<Vec<_>>().join("\n");
        recipe = match Recipe::from_str(&s) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig).unwrap();
                recipe
            }
            Err(e) => panic!(e),
        };

        mig.commit();
    }

    println!("{}", g);

    Box::new(Backend { r: recipe, g: g })

}

fn main() {
    use clap::{Arg, App};

    let matches = App::new("hotsoup")
        .version("0.1")
        .about("Soupy conference management system for your HotCRP needs.")
        .arg(Arg::with_name("recipe")
            .short("r")
            .required(true)
            .default_value("tests/hotcrp-schema.txt")
            .help("Location of the HotCRP schema recipe to start off with."))
        .arg(Arg::with_name("transactional")
            .short("t")
            .help("Use transactional writes."))
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let transactional = matches.is_present("transactional");

    println!("Loading HotCRP recipe from {}", rloc);
    let backend = make(&rloc);
}
