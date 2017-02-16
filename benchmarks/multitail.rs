extern crate distributary;

#[macro_use]
extern crate clap;

use std::{thread, time};

use distributary::{Blender, Base, Aggregation, Mutator, Token, JoinBuilder, NodeAddress};

struct Backend {
    data: Option<Mutator>,
    number: Option<Mutator>,
    _g: Blender,
}

enum DomainConfiguration {
    SingleDomain,
    DomainPerNode,
    HorizSlice,
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

        let mut domain = mig.add_domain();
        mig.assign_domain(data, domain);
        mig.assign_domain(number, domain);
        let mut base;
        match domains {
            DomainConfiguration::VerticalSlice => {
                println!("Creating domain for each tail!");
                for col in 0..width {
                    domain = mig.add_domain();
                    let j = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
                    base = mig.add_ingredient(format!("nodeJ{}", col), &["number", "val"], j);
                    mig.assign_domain(base, domain);
                    for row in 1..height {
                        base = mig.add_ingredient(format!("node{}{}", row, col), &["number", "val"], Aggregation::SUM.over(base, 0, &[1]));
                        mig.assign_domain(base, domain);
                    }
                }
            },
            DomainConfiguration::HorizSlice => {
                println!("Creating domain for each row!");
                let mut rows: Vec<NodeAddress> = Vec::new();
                domain = mig.add_domain();
                for col in 0..width {
                    let j = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
                    rows.push(mig.add_ingredient(format!("nodeJ{}", col), &["number", "val"], j));
                    mig.assign_domain(rows[col as usize], domain);
                }
                for row in 1..height {
                    domain = mig.add_domain();
                    for col in 0..width {
                        rows[col as usize] = mig.add_ingredient(format!("node{}{}", row, col), &["number", "val"], Aggregation::SUM.over(rows[col as usize], 0, &[1]));
                        mig.assign_domain(rows[col as usize], domain);
                    }
                }
            },
            DomainConfiguration::DomainPerNode => {
                println!("Creating domain for each node!");
                for col in 0..width {
                    let j = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
                    base = mig.add_ingredient(format!("nodeJ{}", col), &["number", "val"], j);
                    domain = mig.add_domain();
                    mig.assign_domain(base, domain);
                    for row in 1..height {
                        base = mig.add_ingredient(format!("node{}{}", row, col), &["number", "val"], Aggregation::SUM.over(base, 0, &[1]));
                        domain = mig.add_domain();
                        mig.assign_domain(base, domain);
                    }
                }
            },
            DomainConfiguration::SingleDomain => {
                println!("Using one domain for all nodes!");
                for col in 0..width {
                    let j = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
                    base = mig.add_ingredient(format!("nodeJ{}", col), &["number", "val"], j);
                    mig.assign_domain(base, domain);
                    for row in 1..height {
                        base = mig.add_ingredient(format!("node{}{}", row, col), &["number", "val"], Aggregation::SUM.over(base, 0, &[1]));
                        mig.assign_domain(base, domain);
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
             .possible_values(&["one_domain", "domain_per_node", "vert_slice", "horiz_slice"])
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
        .get_matches();

    let cfg = matches.value_of("cfg").unwrap();
    let batch_size = value_t_or_exit!(matches, "batch", i64);
    let width = value_t_or_exit!(matches, "width", u16);
    let height = value_t_or_exit!(matches, "height", u16);

    println!("Using batch size of {}", batch_size);

    let mut backend;
    match cfg.as_ref() {
        "vert_slice" => backend = make(DomainConfiguration::VerticalSlice, width, height),
        "horiz_slice" => backend = make(DomainConfiguration::HorizSlice, width, height),
        "domain_per_node" => backend = make(DomainConfiguration::DomainPerNode, width, height),
        _ => backend = make(DomainConfiguration::SingleDomain, width, height),
    }
    let data_putter = backend.data.take().unwrap();
    let number_putter = backend.number.take().unwrap();
    println!("Seeding...");
    for y in 1..batch_size+1 {
        // 3 because 3 tails
        data_putter.transactional_put(vec![batch_size.into(), y.into()], Token::empty());
    }
    println!("Finished seeding! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Starting benchmark!");
    let start = time::Instant::now();
    let mut elapsed_millis: i64;
    let mut num_updates: i64 = 0;
    loop {
        num_updates += 3 * batch_size;
        number_putter.transactional_put(vec![batch_size.into()], Token::empty());
        let elapsed = time::Instant::now().duration_since(start);
        elapsed_millis = (elapsed.as_secs() as i64 * 1_000) + (elapsed.subsec_nanos() as i64 / 1_000_000);
        if elapsed_millis > 30_000 {
            break;
        }
    }

    println!("{}", num_updates / elapsed_millis);

}
