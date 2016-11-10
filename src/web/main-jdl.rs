extern crate clap;
extern crate distributary;
extern crate shortcut;

#[cfg(all(feature="web", feature="jdl"))]
fn main() {
    use distributary::*;
    use clap::{Arg, App};
    use std::fs::File;

    let args = App::new("jdl-web")
        .version("0.1")
        .about("Constructs a web frontend for a Soup graph described in JSON.")
        .arg(Arg::with_name("JSON-FILE")
            .index(1)
            .required(true))
        .get_matches();

    let filename = args.value_of("JSON-FILE").unwrap();
    let file = match File::open(&filename) {
        Err(msg) => panic!("{}", msg),
        Ok(f) => f,
    };

    let g = jdl::parse(file);
    println!("booting server...");
    web::run(g).unwrap();
}

#[cfg(not(all(feature="web", feature="jdl")))]
fn main() {
    unreachable!("compile with --features='web jdl' to build the web-jdl frontend");
}
