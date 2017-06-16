extern crate chrono;
extern crate distributary;

mod populate;

extern crate clap;
extern crate rand;

#[macro_use]
extern crate slog;

use rand::Rng;
use std::{thread, time};
use std::collections::HashMap;

use std::sync::{Arc, Barrier};

use distributary::{Blender, DataType, Recipe};

pub struct Backend {
    r: Recipe,
    g: Blender,
    parallel_prepop: bool,
    prepop_counts: HashMap<String, usize>,
    barrier: Arc<Barrier>,
}

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_fsec {
    ($d:expr) => {{
        let d = $d;
        (d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64) as f64 / NANOS_PER_SEC as f64
    }}
}

fn make(recipe_location: &str, transactions: bool, parallel: bool) -> Box<Backend> {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = Blender::new();
    let main_log = distributary::logger_pls();
    let recipe_log = main_log.new(o!());
    g.log_with(main_log);

    let recipe;
    {
        // migrate
        let mut mig = g.start_migration();

        let mut f = File::open(recipe_location).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        recipe = match Recipe::from_str(&s, Some(recipe_log)) {
            Ok(mut recipe) => {
                recipe.activate(&mut mig, transactions).unwrap();
                recipe
            }
            Err(e) => panic!(e),
        };

        mig.commit();
    }

    //println!("{}", g);

    Box::new(Backend {
        r: recipe,
        g: g,
        parallel_prepop: parallel,
        prepop_counts: HashMap::new(),
        barrier: Arc::new(Barrier::new(9)), // N.B.: # base tables
    })
}


impl Backend {
    fn generate_parameter(&self, query_name: &str, rng: &mut rand::ThreadRng) -> DataType {
        match query_name {
            "getName" => {
                rng.gen_range(1, self.prepop_counts["customers"] as i32)
                    .into()
            }
            "getBook" => rng.gen_range(1, self.prepop_counts["items"] as i32).into(),
            "getCustomer" => "".into(), // XXX(malte): fix username string generation
            "doSubjectSearch" => "".into(), // XXX(malte): fix subject string generation
            "getNewProducts" => "".into(), // XXX(malte): fix subject string generation
            "getUserName" => {
                rng.gen_range(1, self.prepop_counts["customers"] as i32)
                    .into()
            }
            "getPassword" => "".into(), // XXX(malte): fix username string generation
            "getRelated1" => rng.gen_range(1, self.prepop_counts["items"] as i32).into(),
            "getMostRecentOrderId" => "".into(), // XXX(malte): fix username string generation
            "getMostRecentOrderLines" => {
                rng.gen_range(1, self.prepop_counts["orders"] as i32).into()
            }
            "createEmptyCart" => 0i32.into(),
            "addItem" => 0.into(), // XXX(malte): dual parameter query, need SCL ID range
            "addRandomItemToCartIfNecessary" => 0.into(), // XXX(malte): need SCL ID range
            "getCart" => 0.into(), // XXX(malte): need SCL ID range
            "createNewCustomerMaxId" => 0i32.into(),
            "getCDiscount" => {
                rng.gen_range(1, self.prepop_counts["customers"] as i32)
                    .into()
            }
            "getCAddrId" => {
                rng.gen_range(1, self.prepop_counts["customers"] as i32)
                    .into()
            }
            "getCAddr" => {
                rng.gen_range(1, self.prepop_counts["customers"] as i32)
                    .into()
            }
            "enterAddressId" => {
                rng.gen_range(1, self.prepop_counts["countries"] as i32)
                    .into()
            }
            "enterAddressMaxId" => 0i32.into(),
            "enterOrderMaxId" => 0i32.into(),
            "getStock" => rng.gen_range(1, self.prepop_counts["items"] as i32).into(),
            "verifyDBConsistencyCustId" => 0i32.into(),
            "verifyDBConsistencyItemId" => 0i32.into(),
            "verifyDBConsistencyAddrId" => 0i32.into(),
            _ => unimplemented!(),
        }
    }

    fn read(&self, query_name: &str, num: usize) {
        match self.r.node_addr_for(query_name) {
            Err(_) => panic!("no node for {}!", query_name),
            Ok(nd) => {
                println!("reading {}", query_name);
                let g = self.g.get_getter(nd).unwrap();
                let mut rng = rand::thread_rng();
                let start = time::Instant::now();
                let mut ok = 0;
                for _ in 0..num {
                    let param = self.generate_parameter(query_name, &mut rng);
                    match g(&param, true) {
                        Err(_) => panic!(),
                        Ok(datas) => {
                            if datas.len() > 0 {
                                ok += 1;
                            }
                        }
                    }
                }
                let dur = dur_to_fsec!(start.elapsed());
                println!(
                    "{}: ({:.2} GETs/sec) (ok: {})!",
                    query_name,
                    f64::from(1_000_000) / dur,
                    ok
                );
            }
        }
    }
}

fn main() {
    use clap::{Arg, App};
    use populate::*;

    let matches = App::new("tpc_w")
        .version("0.1")
        .about("Soup TPC-W driver.")
        .arg(
            Arg::with_name("recipe")
                .short("r")
                .required(true)
                .default_value("tests/tpc-w-queries.txt")
                .help("Location of the TPC-W recipe file."),
        )
        .arg(
            Arg::with_name("populate_from")
                .short("p")
                .required(true)
                .default_value("benchmarks/tpc_w/data")
                .help("Location of the data files for TPC-W prepopulation."),
        )
        .arg(
            Arg::with_name("parallel_prepopulation")
                .long("parallel-prepopulation")
                .help("Prepopulate using parallel threads."),
        )
        .arg(
            Arg::with_name("transactional")
                .short("t")
                .help("Use transactional writes."),
        )
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let ploc = matches.value_of("populate_from").unwrap();
    let transactions = matches.is_present("transactional");
    let parallel_prepop = matches.is_present("parallel_prepopulation");

    println!("Loading TPC-W recipe from {}", rloc);
    let mut backend = make(&rloc, transactions, parallel_prepop);

    println!("Prepopulating from data files in {}", ploc);
    let num_addr = populate_addresses(&backend, &ploc);
    backend.prepop_counts.insert("addresses".into(), num_addr);
    let num_authors = populate_authors(&backend, &ploc);
    backend.prepop_counts.insert("authors".into(), num_authors);
    let num_countries = populate_countries(&backend, &ploc);
    backend.prepop_counts.insert(
        "countries".into(),
        num_countries,
    );
    let num_customers = populate_customers(&backend, &ploc);
    backend.prepop_counts.insert(
        "customers".into(),
        num_customers,
    );
    let num_items = populate_items(&backend, &ploc);
    backend.prepop_counts.insert("items".into(), num_items);
    let num_orders = populate_orders(&backend, &ploc);
    backend.prepop_counts.insert("orders".into(), num_orders);
    let num_cc_xacts = populate_cc_xacts(&backend, &ploc);
    backend.prepop_counts.insert(
        "cc_xacts".into(),
        num_cc_xacts,
    );
    let num_order_line = populate_order_line(&backend, &ploc);
    backend.prepop_counts.insert(
        "order_line".into(),
        num_order_line,
    );

    if parallel_prepop {
        backend.barrier.wait();
        backend.barrier.wait();
    }

    //println!("{}", backend.g);

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Reading...");
    for nq in backend.r.aliases().iter() {
        backend.read(nq, 1_000_000);
    }
}
