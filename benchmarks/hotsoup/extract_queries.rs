extern crate clap;
extern crate nom_sql;
extern crate regex;
#[macro_use]
extern crate slog;
extern crate slog_term;

use slog::DrainExt;
use std::path::{Path, PathBuf};

fn traverse(path: &Path) -> Vec<PathBuf> {
    use std::fs;

    let mut files = Vec::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            match path.extension() {
                Some(e) => {
                    if e.to_str().unwrap() == "php" || e.to_str().unwrap() == "inc" {
                        files.push(path.clone())
                    }
                }
                _ => (),
            }
        } else if path.is_dir() {
            files.extend(traverse(path.as_path()));
        }
    }
    files
}

fn process_file(fp: &Path) -> Vec<(String, String)> {
    use std::io::Read;
    use std::fs::File;

    let mut f = File::open(fp).unwrap();
    let mut s = String::new();

    f.read_to_string(&mut s).unwrap();
    let lines = s.lines()
        .enumerate()
        .map(|(i, l)| {
            let fname = fp.file_name().unwrap().to_str().unwrap();
            (format!("{}_{}", fname, i + 1), String::from(l))
        })
        .collect::<Vec<_>>();

    let mut buf = String::new();
    let mut in_query = false;
    let mut location = String::new();
    let mut queries = Vec::new();
    for (fl, line) in lines {
        if !in_query && line.contains("\"select ") {
            // start query, look for end
            location = fl;
            // is the end on the same line?
            let qstart = line.find("\"select ").unwrap() + 1;
            match line[qstart..].find("\"") {
                None => {
                    in_query = true;
                    buf.push_str(&line[qstart..]);
                }
                Some(pos) => {
                    queries.push((location.clone(), String::from(&line[qstart..qstart + pos])));
                }
            }
        } else if in_query {
            // query continues, look for end
            match line.rfind("\"") {
                None => {
                    buf.push_str(" "); // replace newline with space
                    buf.push_str(&line);
                }
                Some(pos) => {
                    let qstr = format!("{}{}", buf, &line[0..pos + 1]);
                    queries.push((location.clone(), qstr));
                    in_query = false;
                    buf.clear();
                }
            }
        }
    }
    if in_query {
        let qstr = format!("{}", buf);
        queries.push((location.clone(), qstr));
    }
    queries
}

fn reformat(queries: Vec<(String, String)>) -> Vec<(String, String)> {
    use regex::Regex;
    let php_vars = Regex::new("\\$[a-zA-Z0-9->_]+").unwrap();
    let incomplete = Regex::new("=$").unwrap();

    queries.into_iter()
        .map(|(qn, q)| (qn, php_vars.replace_all(&q, "?")))
        .map(|(qn, q)| (qn, incomplete.replace_all(&q, "=?")))
        .collect()
}

const SKIP_FILES: [&str; 1] = ["test02.php"];

fn main() {
    use clap::{Arg, App};
    use std::io::Write;
    use std::fs::File;

    let log = slog::Logger::root(slog_term::streamer().full().build().fuse(), None);

    let matches = App::new("extract_queries")
        .version("0.1")
        .about("Extracts queries from HotCRP code.")
        .arg(Arg::with_name("source")
            .index(1)
            .help("Location of the HotCRP code to work on.")
            .required(true))
        .arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("FILE")
            .help("Location to write output recipe to.")
            .required(true))
        .get_matches();

    let path = matches.value_of("source").unwrap();
    let output = matches.value_of("output").unwrap();

    let files = traverse(Path::new(path));

    let mut ok = Vec::new();
    let mut rejected = Vec::new();
    for fname in files {
        if SKIP_FILES.contains(&fname.file_name().unwrap().to_str().unwrap()) {
            continue;
        }

        let queries = process_file(fname.as_path());

        let formatted_queries = reformat(queries);

        for (ql, q) in formatted_queries {
            match nom_sql::parse_query(&q) {
                Ok(_) => ok.push((ql, q)),
                Err(_) => rejected.push((ql, q)),
            }
        }
    }

    let mut f = File::create(output).unwrap();

    info!(log, "Writing {} valid queries...", ok.len());
    for (ql, q) in ok {
        assert!(write!(f, "{}: {}\n", ql, q).is_ok());
    }

    info!(log, "Writing {} rejected queries...", rejected.len());
    for (ql, q) in rejected {
        trace!(log, "failed to parse"; "query" => q, "name" => ql);
        assert!(write!(f, "# FAIL {}:\n# {}\n", ql, q).is_ok());
    }
}
