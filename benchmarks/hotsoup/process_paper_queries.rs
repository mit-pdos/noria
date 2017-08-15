extern crate clap;
extern crate nom_sql;
extern crate regex;

#[macro_use]
extern crate slog;
extern crate distributary;

use std::path::{Path, PathBuf};

fn traverse(path: &Path) -> Vec<PathBuf> {
    use std::fs;

    let mut files = Vec::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            files.push(path.clone())
        }
    }
    files
}

fn process_file(fp: &Path) -> Vec<String> {
    use regex::Regex;
    use std::io::Read;
    use std::fs::File;

    let re = "\\s+ [0-9]+ Query\t(.*)+";
    let query_regex = Regex::new(re).unwrap();

    let mut f = File::open(fp).unwrap();
    let mut s = String::new();

    f.read_to_string(&mut s).unwrap();

    let mut queries = Vec::new();
    let mut start = false;
    for l in s.lines() {
        if l.contains("### CHAIR PAPER LIST") {
            start = true;
        }

        if !start {
            continue;
        }

        for cap in query_regex.captures_iter(&l) {
            let qstr = &cap[1];
            queries.push(String::from(qstr));
        }
    }
    queries
}

fn main() {
    use clap::{Arg, App};

    let log = distributary::logger_pls();

    let matches = App::new("process_paper_queries")
        .version("0.1")
        .about("Process extracted HotCRP paper queries.")
        .arg(
            Arg::with_name("source")
                .index(1)
                .help(
                    "Location of the HotCRP paper queries (in MySQL query log form).",
                )
                .required(true),
        )
        .get_matches();

    let path = matches.value_of("source").unwrap();

    let files = traverse(Path::new(path));

    for fname in files {
        info!(log, "Processing {:?}", fname);
        let queries = process_file(fname.as_path());

        for q in queries {
            match nom_sql::parse_query(&q) {
                Ok(_) => println!("OK -- {}", q),
                Err(e) => println!("FAIL -- {}: {:?}", q, e),
            }
        }
    }
}
