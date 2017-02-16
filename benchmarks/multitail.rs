extern crate distributary;

use std::{thread, time, env};

use distributary::{Blender, Base, Aggregation, Mutator, Token, JoinBuilder, NodeAddress};

struct Backend {
    data: Option<Mutator>,
    number: Option<Mutator>,
    _g: Blender,
}


fn make(domains: u16, width: u16, height: u16) -> Box<Backend> {
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
            1 => {
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
            2 => {
                println!("Creating domain for each row!");
                let mut rows: Vec<NodeAddress> = Vec::new();
                for col in 0..width {
                    domain = mig.add_domain();
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
            _ => {
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
    let args: Vec<_> = env::args().collect();
    if args.len() <= 1 || args[1] != "domains" && args[1] != "no_domains" && args[1] != "horiz" {
        println!("Usage:\n./multitail domains\n./multitail no_domains\n./multitail horiz\nOptionally put batch size after test type\nOptionally put '<width> <height>' after batch size");
        return
    }
    let batch_size;
    let width: u16;
    let height: u16;
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
    if args.len() > 4 {
        let w = args[3].parse();
        let h = args[4].parse();
        match w {
            Ok(i) => {
                width = i;
            },
            Err(_) => {
                println!("Invalid width!");
                return
            }
        }
        match h {
            Ok(i) => {
                height = i;
            },
            Err(_) => {
                println!("Invalid height!");
                return
            }
        }
    } else {
        width = 3;
        height = 3;
    }

    println!("Using batch size of {}", batch_size);

    let mut backend;
    match args[1].as_ref() {
        "domains" => backend = make(1, width, height),
        "horiz" => backend = make(2, width, height),
        _ => backend = make(0, width, height),
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
