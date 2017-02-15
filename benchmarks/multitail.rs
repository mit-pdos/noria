extern crate distributary;

use std::{thread, time, env};

use distributary::{Blender, Base, Aggregation, Mutator, Token, JoinBuilder};

struct Backend {
    data: Option<Mutator>,
    number: Option<Mutator>,
    _g: Blender,
}


fn make(domains: u16) -> Box<Backend> {
    // set up graph
    let mut g = Blender::new();

    let data;
    let number;
    let node1a;
    let node2a;
    let node3a;
    let node1b;
    let node2b;
    let node3b;
    let node1c;
    let node2c;
    let node3c;
    let node1d;
    let node2d;
    let node3d;
    {
        // migrate
        let mut mig = g.start_migration();

        // add data base node
        data = mig.add_ingredient("data", &["number", "val"], Base::default());
        // add number base node
        number = mig.add_ingredient("number", &["number"], Base::default());
        // create joiner
        let j1 = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
        let j2 = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);
        let j3 = JoinBuilder::new(vec![(data, 0), (data, 1)]).from(data, vec![1, 0]).join(number, vec![1]);

        node1a = mig.add_ingredient("node1a",
                                    &["number", "val"],
                                    j1);
        node2a = mig.add_ingredient("node2a",
                                    &["number", "val"],
                                    j2);
        node3a = mig.add_ingredient("node3a",
                                    &["number", "val"],
                                    j3);

        node1b = mig.add_ingredient("node1b",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node1a, 0, &[1]));
        node2b = mig.add_ingredient("node2b",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node2a, 0, &[1]));
        node3b = mig.add_ingredient("node3b",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node3a, 0, &[1]));

        node1c = mig.add_ingredient("node1c",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node1b, 0, &[1]));
        node2c = mig.add_ingredient("node2c",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node2b, 0, &[1]));
        node3c = mig.add_ingredient("node3c",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node3b, 0, &[1]));

        node1d = mig.add_ingredient("node1d",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node1c, 1, &[0]));
        node2d = mig.add_ingredient("node2d",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node2c, 1, &[0]));
        node3d = mig.add_ingredient("node3d",
                                    &["number", "val"],
                                    Aggregation::SUM.over(node3c, 1, &[0]));

        let d1 = mig.add_domain();
        let (d4, d5, d6);
        if domains == 1 {
            println!("Creating domain for each tail!");
            d4 = mig.add_domain();
            d5 = mig.add_domain();
            mig.assign_domain(data, d1);
            mig.assign_domain(number, d1);

            mig.assign_domain(node1a, d1);
            mig.assign_domain(node2a, d4);
            mig.assign_domain(node3a, d5);

            mig.assign_domain(node1b, d1);
            mig.assign_domain(node2b, d4);
            mig.assign_domain(node3b, d5);

            mig.assign_domain(node1c, d1);
            mig.assign_domain(node2c, d4);
            mig.assign_domain(node3c, d5);

            mig.assign_domain(node1d, d1);
            mig.assign_domain(node2d, d4);
            mig.assign_domain(node3d, d5);
        } else {
            if domains == 2 {
                println!("Creating domain for each horizontal slice!");
                // let d2 = mig.add_domain();
                // let d3 = mig.add_domain();
                d4 = mig.add_domain();
                d5 = mig.add_domain();
                d6 = mig.add_domain();
            } else {
                println!("Only using 1 domain for all tails!");
                d4 = d1;
                d5 = d1;
                d6 = d1;
            }
            mig.assign_domain(data, d1);
            mig.assign_domain(number, d1);

            mig.assign_domain(node1a, d1);
            mig.assign_domain(node2a, d1);
            mig.assign_domain(node3a, d1);

            mig.assign_domain(node1b, d4);
            mig.assign_domain(node2b, d4);
            mig.assign_domain(node3b, d4);

            mig.assign_domain(node1c, d5);
            mig.assign_domain(node2c, d5);
            mig.assign_domain(node3c, d5);

            mig.assign_domain(node1d, d6);
            mig.assign_domain(node2d, d6);
            mig.assign_domain(node3d, d6);
        }
        mig.commit();
    }

    // println!("{}", g);
    Box::new(Backend {
        data: Some(g.get_mutator(data)),
        number: Some(g.get_mutator(number)),
        _g: g,
    })

}

fn main() {
    let args: Vec<_> = env::args().collect();
    if args.len() <= 1 || args[1] != "domains" && args[1] != "no_domains" && args[1] != "horiz" {
        println!("Usage:\n./multitail domains\n./multitail no_domains\n./multitail horiz\nOptionally put batch size after test type");
        return
    }
    let batch_size;
    if args.len() > 2 {
        let input_batch = args[2].parse();
        match input_batch {
            Ok(i) => {
                batch_size = i;
            },
            Err(_) => {
                println!("Invalid batch size!");
                return
            }
        }
    } else {
        batch_size = 1;
    }
    println!("Using batch size of {}", batch_size);

    let mut backend;
    match args[1].as_ref() {
        "domains" => backend = make(1),
        "horiz" => backend = make(2),
        _ => backend = make(0),
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
