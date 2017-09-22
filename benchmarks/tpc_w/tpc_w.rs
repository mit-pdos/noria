extern crate chrono;
extern crate distributary;
extern crate rand;

mod populate;
mod parameters;

#[macro_use]
extern crate clap;

#[macro_use]
extern crate slog;

use parameters::SampleKeys;
use std::{thread, time};
use std::collections::HashMap;
use rand::Rng;

use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;

use distributary::{Blender, Recipe, ReuseConfigType};

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

fn get_queries(recipe_location: &str, random: bool) -> Vec<String> {
    use std::io::Read;
    use std::fs::File;

    let mut f = File::open(recipe_location).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    let mut queries = s.lines()
        .filter(|l| {
            !l.is_empty() && !l.starts_with('#') && !l.starts_with("--") && !l.starts_with("CREATE")
        })
        .map(String::from)
        .collect::<Vec<_>>();

    if random {
        let mut rng = rand::thread_rng();
        rng.shuffle(queries.as_mut_slice());
    }

    queries
}

fn make(recipe_location: &str, transactions: bool, parallel: bool, single_query: bool, disable_partial: bool, reuse: &str) -> Backend {
    use std::io::Read;
    use std::fs::File;

    // set up graph
    let mut g = Blender::new();

    let main_log = distributary::logger_pls();
    let recipe_log = main_log.new(o!());
    g.log_with(main_log);
    g.disable_sharding();
    if disable_partial {
        g.disable_partial();
    }


    let recipe;
    {
        // migrate
        let mut mig = g.start_migration();

        let mut f = File::open(recipe_location).unwrap();
        let mut s = String::new();

        // load queries
        f.read_to_string(&mut s).unwrap();
        if single_query {
            s = s.lines()
                .take_while(|l| l.starts_with("CREATE"))
                .collect::<Vec<_>>()
                .join("\n");
        }
        recipe = match Recipe::from_str(&s, Some(recipe_log.clone())) {
            Ok(mut recipe) => {
                match reuse.as_ref() {
                    "finkelstein" => recipe.enable_reuse(ReuseConfigType::Finkelstein),
                    "full" => recipe.enable_reuse(ReuseConfigType::Full),
                    "noreuse" => recipe.enable_reuse(ReuseConfigType::NoReuse),
                    "relaxed" => recipe.enable_reuse(ReuseConfigType::Relaxed),
                    _ => panic!("reuse configuration not supported"),
                }
                recipe.activate(&mut mig, transactions).unwrap();
                recipe
            }
            Err(e) => panic!(e),
        };

        mig.commit();
    }

    // println!("{}", g);

    Backend {
        r: recipe,
        g: g,
        parallel_prepop: parallel,
        prepop_counts: HashMap::new(),
        barrier: Arc::new(Barrier::new(9)), // N.B.: # base tables
    }
}


impl Backend {
    fn extend(mut self, query: &str, transactions: bool) -> Backend {
        {
            let query_name = query.split(":").next().unwrap();
            let start = time::Instant::now();
            let mut mig = self.g.start_migration();
            let new_recipe = match self.r.extend(query) {
                Ok(mut recipe) => {
                    recipe.activate(&mut mig, transactions).unwrap();
                    recipe
                }
                Err(e) => panic!(e),
            };

            mig.commit();
            let dur = dur_to_fsec!(start.elapsed());
            println!("Migrate query {}: ({:.2} sec)", query_name, dur,);

            self.r = new_recipe;
        }

        self
    }

    fn read(&self, keys: &mut SampleKeys, query_name: &str, read_scale: f32, parallel: bool) -> Option<JoinHandle<()>>{
        match self.r.node_addr_for(query_name) {
            Err(_) => panic!("no node for {}!", query_name),
            Ok(nd) => {
                    println!("reading {}", query_name);
                    let g = self.g.get_getter(nd).unwrap();
                    let query_name = String::from(query_name);

                    let num = ((keys.keys_size(&query_name) as f32) * read_scale) as usize;
                    let params = keys.generate_parameter(&query_name, num);

                    let read_view = move || {
                        let mut ok = 0;

                        let start = time::Instant::now();
                        for i in 0..num {
                            match g.lookup(&params[i], true) {
                                Err(_) => continue,
                                Ok(datas) => if datas.len() > 0 {
                                    ok += 1;
                                },
                            }
                        }
                        let dur = dur_to_fsec!(start.elapsed());
                        println!(
                            "{}: ({:.2} GETs/sec) (ok: {})!",
                            query_name,
                            f64::from(num as i32) / dur,
                            ok
                        );
                    };

                    if parallel {
                        Some(thread::spawn(move || {
                            read_view();
                        }))
                    } else {
                        read_view();
                        None
                    }
            }
        }
    }
}

fn main() {
    use clap::{App, Arg};
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
        .arg(
            Arg::with_name("single_query_migration")
                .long("single-query-migration")
                .short("s")
                .help("Add queries one by one, instead of in a batch."),
        )
        .arg(
            Arg::with_name("gloc")
                .short("g")
                .value_name("DIR")
                .help("Directory to store graphs generated by benchmark"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("finkelstein")
                .help("Enable node reuse"),
        )
        .arg(
            Arg::with_name("disable_partial")
                .long("disable_partial")
                .help("Disable partial materialization"),
        )
        .arg(
            Arg::with_name("read")
                .long("read")
                .default_value("0.00")
                .help("Scale reads from the application")
        )
        .arg(
            Arg::with_name("item_write")
                .long("item_write")
                .default_value("1.00")
                .help("Scale writes")
        )
        .arg(
            Arg::with_name("customer_write")
                .long("customer_write")
                .default_value("1.00")
                .help("Scale writes")
        )
        .arg(
            Arg::with_name("random")
                .long("random")
                .help("Adds queries in random order (only makes sense with -s)")
        )
        .arg(
            Arg::with_name("parallel_read")
                .long("parallel_read")
                .help("Reads using parallel threads")
        )
        .get_matches();

    let rloc = matches.value_of("recipe").unwrap();
    let ploc = matches.value_of("populate_from").unwrap();
    let transactions = matches.is_present("transactional");
    let parallel_prepop = matches.is_present("parallel_prepopulation");
    let parallel_read = matches.is_present("parallel_read");
    let single_query = matches.is_present("single_query_migration");
    let gloc = matches.value_of("gloc");
    let disable_partial = matches.is_present("disable_partial");
    let read_scale = value_t_or_exit!(matches, "read", f32);
    let item_write = value_t_or_exit!(matches, "item_write", f32);
    let reuse = matches.value_of("reuse").unwrap();
    let random = matches.is_present("random");

    println!("Loading TPC-W recipe from {}", rloc);
    let mut backend = make(&rloc, transactions, parallel_prepop, single_query, disable_partial, reuse);

    println!("Prepopulating from data files in {}", ploc);
    let num_addr = populate_addresses(&backend, &ploc);
    backend.prepop_counts.insert("addresses".into(), num_addr);
    let num_authors = populate_authors(&backend, &ploc);
    backend.prepop_counts.insert("authors".into(), num_authors);
    let num_countries = populate_countries(&backend, &ploc);
    backend
        .prepop_counts
        .insert("countries".into(), num_countries);
    let num_customers = populate_customers(&backend, &ploc);
    backend
        .prepop_counts
        .insert("customers".into(), num_customers);
    let num_items = populate_items(&backend, &ploc, item_write, true);
    backend.prepop_counts.insert("items".into(), num_items);
    let num_orders = populate_orders(&backend, &ploc);
    backend.prepop_counts.insert("orders".into(), num_orders);
    let num_cc_xacts = populate_cc_xacts(&backend, &ploc);
    backend
        .prepop_counts
        .insert("cc_xacts".into(), num_cc_xacts);
    let num_order_line = populate_order_line(&backend, &ploc);
    backend
        .prepop_counts
        .insert("order_line".into(), num_order_line);

    if parallel_prepop {
        backend.barrier.wait();
        backend.barrier.wait();
    }

    //println!("{}", backend.g);

    println!("Finished writing! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    if single_query {
        use std::fs::File;
        use std::io::Write;

        println!("Migrating individual queries...");
        let queries = get_queries(&rloc, random);

        for (i, q) in queries.iter().enumerate() {
            backend = backend.extend(&q, transactions);

            if gloc.is_some() {
                let graph_fname = format!("{}/tpcw_{}.gv", gloc.unwrap(), i);
                let mut gf = File::create(graph_fname).unwrap();
                assert!(write!(gf, "{}", backend.g).is_ok());
            }
        }
    }

    if read_scale > 0.0 {
        println!("Reading...");
        let mut keys = SampleKeys::new(&ploc, item_write);
        let item_queries = [
                            "getBestSellers",
                            "getMostRecentOrderLines",
                            "getBook",
                            "doSubjectSearch",
                            "getNewProducts",
                            "getRelated1",
                            "getCart",
                            "verifyDBConsistencyItemId"
        ];
        let mut handles = Vec::new();
        for nq in item_queries.iter() {
            handles.push(backend.read(&mut keys, nq, read_scale, parallel_read));
        }

        for h in handles {
            match h {
                Some(jh) => jh.join().unwrap(),
                None => continue,
            }
        }
    }

    if item_write < 1.0 {
        println!("Do some more writes...");
        populate_items(&backend, &ploc, item_write, false);
    }
}
