use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::path::Path;
use std::time;

use distributary::{DataType, Mutator, Token};
use super::Backend;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }}
}

fn do_put<'a>(mutator: &'a Mutator, tx: bool) -> Box<Fn(Vec<DataType>) + 'a> {
    match tx {
        true => Box::new(move |v| assert!(mutator.transactional_put(v, Token::empty()).is_ok())),
        false => Box::new(move |v| mutator.put(v)),
    }
}

fn populate_table(backend: &Backend, data: &Path, use_txn: bool) -> usize {
    let table_name = data.file_stem()
        .unwrap()
        .to_str()
        .unwrap();
    let putter = backend.g.get_mutator(backend.r
                                           .as_ref()
                                           .unwrap()
                                           .node_addr_for(table_name)
                                           .unwrap());

    let f = File::open(data).unwrap();
    let mut reader = BufReader::new(f);

    let mut s = String::new();
    println!("Populating {}...", table_name);
    let start = time::Instant::now();
    let mut i = 0;
    while reader.read_line(&mut s).unwrap() > 0 {
        {
            let fields: Vec<&str> = s.split("\t").map(str::trim).collect();
            let rec: Vec<DataType> = fields.into_iter().map(|s| s.into()).collect();
            do_put(&putter, use_txn)(rec);
        }
        i += 1;
        s.clear();
    }
    let dur = dur_to_fsec!(start.elapsed());
    println!("Inserted {} {} records in {:.2}s ({:.2} PUTs/sec)!",
             i,
             table_name,
             dur,
             f64::from(i) / dur);
    i as usize
}

pub fn populate(backend: &Backend, data_location: &str, use_txn: bool) -> io::Result<()> {
    use std::fs;

    let dir = Path::new(data_location);
    if dir.is_dir() {
        for entry in try!(fs::read_dir(dir)) {
            let entry = try!(entry);
            let path = entry.path();
            if path.is_file() {
                populate_table(backend, &path, use_txn);
            }
        }
    }
    Ok(())
}
