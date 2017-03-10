extern crate distributary;

#[macro_use]
extern crate clap;

use std::{thread, time};
use std::fs::{OpenOptions, File};
use std::io::Write;

use distributary::{DataType, JoinBuilder, Blender, Base, NodeAddress, Filter, Mutator, Index};

pub struct Piazza {
    pub soup: Blender,
    user: NodeAddress,
    post: NodeAddress,
    class: NodeAddress,
    taking: NodeAddress,
    domain: Index,
}

impl Piazza {
    // Create the base nodes for our Piazza application
    pub fn new() -> Self {
        let mut g = Blender::new();

        let (user, post, class, taking);

        let base_domain;

        {
            let mut mig = g.start_migration();

            base_domain = mig.add_domain();

            // add a user account base table
            user = mig.add_ingredient("user", &["uid", "username", "hash"], Base::default());

            // add a post base table
            post = mig.add_ingredient("post", &["pid", "cid", "author", "content"], Base::new(vec![1]));

            // add a class base table
            class = mig.add_ingredient("class", &["cid", "classname"], Base::default());

            // associations between users and classes
            taking = mig.add_ingredient("taking", &["cid", "uid"], Base::default());

            // assign everything to the same domain to save up memory
            mig.assign_domain(user, base_domain);

            mig.assign_domain(post, base_domain);

            mig.assign_domain(class, base_domain);

            mig.assign_domain(taking, base_domain);

            // commit migration
            mig.commit();
        }

        Piazza {
            soup: g,
            user: user,
            post: post,
            class: class,
            taking: taking,
            domain: base_domain,
        }
    }

    pub fn log_user(&mut self, uid: DataType) {

        let visible_posts;

        let mut mig = self.soup.start_migration();

        // classes user is taking
        let class_filter = Filter::new(self.taking, &[None, Some(uid.into())]);

        let user_classes = mig.add_ingredient("class_filter", &["cid", "uid"], class_filter);
        // add visible posts to user
        // only posts from classes the user is taking should be visible
        let j = JoinBuilder::new(vec![(self.post, 0), (self.post, 1), (self.post, 2), (self.post, 3)])
                .from(self.post, vec![0, 1, 0, 0])
                .join(user_classes, vec![1, 0]);

        visible_posts = mig.add_ingredient("visible_posts", &["pid", "cid", "author", "content"], j);

        // assign everything to the same domain to save up memory
        mig.assign_domain(user_classes, self.domain);

        mig.assign_domain(visible_posts, self.domain);

        // maintain visible_posts
        mig.maintain(visible_posts, 0);

        // commit migration
        mig.commit();

    }
}

fn populate_users(nusers: i64, users_putter: Mutator) {
    for i in 0..nusers {
        users_putter.put(vec![i.into(), "user".into(), "pass".into()]);
    }
}

fn populate_classes(nclasses: i64, class_putter: Mutator) {
    for i in 0..nclasses {
        class_putter.put(vec![i.into(), i.into()]);
    }
}

fn populate_taking(nclasses: i64, nusers: i64, taking_putter: Mutator) {
    for i in 0..nclasses {
        for j in 0..nusers {
            taking_putter.put(vec![i.into(), j.into()]);
        }
    }
}

fn main() {
    use clap::{Arg, App};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with some security policies.")
        .arg(Arg::with_name("nclasses")
            .short("c")
            .long("classes")
            .value_name("N")
            .default_value("100")
            .help("Number of classes to prepopulate the database with"))
        .arg(Arg::with_name("nusers")
            .short("u")
            .long("users")
            .value_name("N")
            .default_value("100")
            .help("Number of users to prepopulate the database with"))
        .arg(Arg::with_name("nposts")
            .short("p")
            .long("posts")
            .value_name("N")
            .default_value("10000")
            .help("Number of posts to prepopulate the database with"))
        .arg(Arg::with_name("csv")
            .long("csv")
            .required(false)
            .help("Print output in CSV format."))
        .arg(Arg::with_name("cfg")
            .possible_values(&["write", "migration"])
            .takes_value(true)
            .required(true)
            .help("Benchmark configuration"))
        .get_matches();


    println!("Creating app...");
    let mut app = Piazza::new();
    println!("Done with app creation.");

    let nusers = value_t_or_exit!(args, "nusers", i64);
    let nclasses = value_t_or_exit!(args, "nclasses", i64);
    let nposts = value_t_or_exit!(args, "nposts", i64);
    let cfg = args.value_of("cfg").unwrap();
    let csv = args.is_present("csv");

    let class_putter = app.soup.get_mutator(app.class);
    let user_putter = app.soup.get_mutator(app.user);
    let taking_putter = app.soup.get_mutator(app.taking);
    let post_putter = app.soup.get_mutator(app.post);

    println!("Seeding...", );
    populate_users(nusers, user_putter);
    populate_classes(nclasses, class_putter);
    populate_taking(nclasses, nusers, taking_putter);

    match cfg.as_ref() {
        "migration" => {
            for pid in 0..nposts {
                post_putter.put(vec![
                    pid.into(),
                    (pid / nclasses).into(),
                    (pid / nusers).into(),
                    "post".into()
                    ]);
            }
        },
        "write" => {
            // login nusers
            for i in 0..nusers {
                app.log_user(i.into());
            }
        }
        _ => {
            println!("wrong benchmark!");
            return
        }
    }

    println!("Finished seeding! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));


    let mut times = Vec::new();
    if csv {
       File::create("out.csv").unwrap();
    }

    println!("Starting benchmark...");
    for uid in 0..(nusers) {
        let start;
        let end;

        match cfg.as_ref() {
            "migration" => {
                start = time::Instant::now();
                app.log_user(uid.into());
                end = time::Instant::now().duration_since(start);
            },
            "write" => {
                start = time::Instant::now();
                for cid in 0..nclasses {
                    post_putter.put(vec![0.into(), cid.into(), uid.into(), "post".into()]);
                }
                end = time::Instant::now().duration_since(start);

                thread::sleep(time::Duration::from_millis(1000));
            },
            _ => {
                println!("wrong benchmark!");
                return
            }
        };

        let time = (end.as_secs() as f64) +
                       (end.subsec_nanos() as f64 / 1_000_000_000.0);

        times.push(time);

        if csv {
            let mut f = OpenOptions::new().write(true).append(true).open("out.csv").unwrap();
            writeln!(f, "{:?},{:?}", uid, time).unwrap();
        } else {
            println!("{:?}: {:?}", uid, time);
        }
    }

    println!("{:?} results ", cfg);
    println!("avg: {:?}", avg(&times) );
    println!("max: {:?}", max_duration(&times) );
    println!("min: {:?}", min_duration(&times) );

    println!("Done with benchmark.");

}

fn max_duration(stats: &Vec<f64>) -> f64 {
    let max = stats.iter().fold(0f64, |acc, el| {
        f64::max(acc, *el)
    });

    max
}

fn min_duration(stats: &Vec<f64>) -> f64 {
    let min_start = stats[0];
    let min = stats.iter().fold(min_start, |acc, el| {
        f64::min(acc, *el)
    });

    min
}


fn avg(stats: &Vec<f64>) -> f64 {
    let sum = stats.iter().fold(0f64, |acc, el| {
        acc + (el / stats.len() as f64)

    });

    sum
}