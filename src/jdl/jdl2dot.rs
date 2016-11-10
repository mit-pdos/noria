extern crate clap;
extern crate distributary;

use distributary::jdl;

use std::fs::File;
use std::io::{Read, Write, stdin, stdout};

fn main() {
    use clap::{Arg, App};

    let args = App::new("jdl2dot")
        .version("0.1")
        .about("Convert a JSON description file into a DOT graph.")
        .arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("OUT-FILE")
            .help("Set DOT graph output file")
            .takes_value(true))
        .arg(Arg::with_name("IN-FILE")
            .index(1))
        .get_matches();

    let input: Box<Read> = match args.value_of("IN-FILE") {
        None | Some("-") => Box::new(stdin()),
        Some(filename) => match File::open(&filename) {
            Err(msg) => panic!("{}", msg),
            Ok(f) => Box::new(f)
        }
    };

    let mut output: Box<Write> = match args.value_of("output") {
        None | Some("-") => Box::new(stdout()),
        Some(filename) => match File::create(&filename) {
            Err(msg) => panic!("{}", msg),
            Ok(f) => Box::new(f)
        }
    };

    let graph = jdl::parse(input);
    write!(output, "{}", graph).unwrap();
}
