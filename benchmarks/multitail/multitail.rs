extern crate distributary;

#[macro_use]
extern crate clap;

use std::{thread, time};

use distributary::{Blender, Base, Aggregation, Mutator, Token, Join, JoinType};

struct Backend {
    data: Option<Mutator>,
    number: Option<Mutator>,
    _g: Blender,
}

enum DomainConfiguration {
    SingleDomain,
    DomainPerNode,
    HorizontalSlice,
    VerticalSlice,
}

fn make(domains: DomainConfiguration, width: u16, height: u16) -> Box<Backend> {
    // set up graph
    let mut g = Blender::new();

    let data;
    let number;
    {
        // migrate
        let mut mig = g.start_migration();

        // add data base node
        data = mig.add_ingredient("data", &["number", "val"], Base::default());
        // add number base node
        number = mig.add_ingredient("number", &["number"], Base::default());

        // create a setup for `width` chains of `height` aggregations using `width` joins
        let mut all = Vec::with_capacity(width as usize);

        // first, create the base of each chain
        for col in 0..width {
            use distributary::JoinSource::*;
            let j = Join::new(data, number, JoinType::Inner, vec![B(0, 0), L(1)]);
            all.push(vec![mig.add_ingredient(format!("nodeJ{}", col), &["number", "val"], j)]);
        }

        // next, create the aggregations for each chain
        for (col, els) in all.iter_mut().enumerate() {
            for row in 1..height {
                let agg = Aggregation::SUM.over(*els.last().unwrap(), 0, &[1]);
                let name = format!("node{}{}", row, col);
                let n = mig.add_ingredient(name, &["number", "val"], agg);
                els.push(n);
            }
        }

        // finally, assign nodes to domains
        // base nodes are in one domain
        let base_domain = mig.add_domain();
        mig.assign_domain(data, base_domain);
        mig.assign_domain(number, base_domain);

        match domains {
            DomainConfiguration::VerticalSlice => {
                println!("Creating domain for each column!");
                for col in &all {
                    let domain = mig.add_domain();
                    for node in col {
                        mig.assign_domain(*node, domain);
                    }
                }
            }
            DomainConfiguration::HorizontalSlice => {
                println!("Creating domain for each row!");
                let row2domain: Vec<_> = (0..height).map(|_| mig.add_domain()).collect();
                for col in &all {
                    for (row, node) in col.iter().enumerate() {
                        mig.assign_domain(*node, row2domain[row]);
                    }
                }
            }
            DomainConfiguration::DomainPerNode => {
                println!("Creating domain for each node!");
                // this is the default, so we don't need to do any assignments
            }
            DomainConfiguration::SingleDomain => {
                println!("Using one domain for all nodes!");
                for col in &all {
                    for node in col {
                        mig.assign_domain(*node, base_domain);
                    }
                }
            }
        }
        mig.commit();
    }

    println!("{}", g);
    Box::new(Backend {
                 data: Some(g.get_mutator(data)),
                 number: Some(g.get_mutator(number)),
                 _g: g,
             })

}

fn main() {
    use clap::{Arg, App};
    let matches = App::new("multitail")
        .version("0.1")
        .about("Benchmarks different thread domain splits for a multi-tailed graphi.")
        .arg(Arg::with_name("cfg")
                 .short("c")
                 .possible_values(&["one_domain",
                                    "domain_per_node",
                                    "vert_slice",
                                    "horiz_slice"])
                 .takes_value(true)
                 .required(true)
                 .help("Domain split type"))
        .arg(Arg::with_name("batch")
                 .short("b")
                 .takes_value(true)
                 .required(true)
                 .help("Batch size"))
        .arg(Arg::with_name("width")
                 .short("w")
                 .takes_value(true)
                 .required(true)
                 .help("Number of tails"))
        .arg(Arg::with_name("height")
                 .short("h")
                 .takes_value(true)
                 .required(true)
                 .help("Depth of each tail"))
        .arg(Arg::with_name("csv")
                 .required(false)
                 .help("Print output in CSV format."))
        .arg(Arg::with_name("stats")
                 .short("s")
                 .required(false)
                 .help("Print statistics about time usage"))
        .get_matches();

    let cfg = matches.value_of("cfg").unwrap();
    let batch_size = value_t_or_exit!(matches, "batch", i64);
    let width = value_t_or_exit!(matches, "width", u16);
    let height = value_t_or_exit!(matches, "height", u16);
    let csv = matches.is_present("csv");
    let stats = matches.is_present("stats");
    assert!(!stats || cfg!(feature = "profiling"));

    println!("Using batch size of {}", batch_size);

    let mut backend = match cfg.as_ref() {
        "vert_slice" => make(DomainConfiguration::VerticalSlice, width, height),
        "horiz_slice" => make(DomainConfiguration::HorizontalSlice, width, height),
        "domain_per_node" => make(DomainConfiguration::DomainPerNode, width, height),
        _ => make(DomainConfiguration::SingleDomain, width, height),
    };
    let data_putter = backend.data.take().unwrap();
    let number_putter = backend.number.take().unwrap();
    println!("Seeding...");
    for y in 1..batch_size + 1 {
        data_putter
            .transactional_put(vec![batch_size.into(), y.into()], Token::empty())
            .unwrap();
    }
    println!("Finished seeding! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Starting benchmark!");
    let start = time::Instant::now();
    let mut elapsed_secs: f64;
    let mut num_puts: i64 = 0;
    let mut num_updates: i64 = 0;
    loop {
        num_puts += 1;
        num_updates += batch_size * width as i64;
        number_putter.put(vec![batch_size.into()]);
        let elapsed = time::Instant::now().duration_since(start);
        elapsed_secs = (elapsed.as_secs() as f64) +
                       (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        if elapsed_secs > 30.0 {
            break;
        }
    }

    if csv {
        println!("{:.2},{:.2}",
                 num_puts as f64 / elapsed_secs,
                 num_updates as f64 / elapsed_secs);
    } else {
        println!("{:.2} PUTs/sec, {:.2} updates/sec",
                 num_puts as f64 / elapsed_secs,
                 num_updates as f64 / elapsed_secs);
    }

    if stats {
        let s = backend._g.get_statistics();
        for (domain_index, (domain_stats, node_map)) in s.domains {
            let total_time = domain_stats.total_time as f64;
            let wait_time = domain_stats.wait_time as f64;

            let mut node_map: Vec<_> = node_map.iter().collect();
            node_map.sort_by_key(|&(_, ns)| ns.process_time);
            node_map.reverse();

            let total_process_time: u64 = node_map.iter().map(|&(_, ns)| ns.process_time).sum();
            println!("Domain {:?}: wait fraction = {:.3}, unaccounted = {:.3}",
                     domain_index,
                     wait_time / total_time,
                     1.0 - (total_process_time as f64 + wait_time) / total_time);

            for (node_address, node_stats) in node_map {
                let process_time = node_stats.process_time as f64;
                let process_ptime = node_stats.process_ptime as f64;
                println!("  {:?}: {:.3} @ {:.0}%",
                         node_address,
                         process_time / total_time,
                         100.0 * process_ptime / process_time);
            }
            println!("");
        }
    }
}
