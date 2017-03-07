extern crate distributary;

#[macro_use]
extern crate clap;

use std::{thread, time};

use distributary::{DataType, JoinBuilder, Blender, Base, NodeAddress, Filter, Mutator};

pub struct Piazza {
    pub soup: Blender,
    user: NodeAddress,
    post: NodeAddress,
    class: NodeAddress,
    taking: NodeAddress,
}

impl Piazza {
    // Create the base nodes for our Piazza application
    pub fn new() -> Self {
        let mut g = Blender::new();

        let (user, post, class, taking);


        {
            let mut mig = g.start_migration();

            // add a user account base table
            user = mig.add_ingredient("user", &["uid", "username", "hash"], Base::default());

            // add a post base table
            post = mig.add_ingredient("post", &["pid", "cid", "author", "content"], Base::default());

            // add a class base table
            class = mig.add_ingredient("class", &["cid", "classname"], Base::default());

            // associations between users and classes
            taking = mig.add_ingredient("taking", &["uid", "cid"], Base::default());

            // commit migration
            mig.commit();
        }

        Piazza {
            soup: g,
            user: user,
            post: post,
            class: class,
            taking: taking,
        }
    }

    pub fn log_user(&mut self, uid: DataType) {

        let visible_posts;

        let mut mig = self.soup.start_migration();

        // classes user is taking
        let class_filter = Filter::new(self.taking, &[Some(uid.into()), None]);

        let user_classes = mig.add_ingredient("class_filter", &["uid", "cid"], class_filter);
        // add visible posts to user
        // only posts from classes the user is taking should be visible
        let j = JoinBuilder::new(vec![(self.post, 0), (self.post, 1), (self.post, 2), (self.post, 3)])
                .from(self.post, vec![0, 1, 0, 0])
                .join(user_classes, vec![0, 1]);

        visible_posts = mig.add_ingredient("visible_posts", &["pid", "cid", "author", "content"], j);

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
            taking_putter.put(vec![j.into(), i.into()]);
        }
    }
}

// writes a post for each class
fn write_posts(nclasses: i64, uid: i64, post_putter: &Mutator) {
    for cid in 0..nclasses {
        post_putter.put(vec![0.into(), cid.into(), uid.into(), "post".into()]);
    }
}

fn main() {
    use clap::{Arg, App};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with some security policies.")
        .arg(Arg::with_name("runtime")
            .short("r")
            .long("runtime")
            .value_name("N")
            .default_value("60")
            .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("nclasses")
            .short("c")
            .long("classes")
            .value_name("N")
            .default_value("1000")
            .help("Number of classes to prepopulate the database with"))
        .arg(Arg::with_name("nusers")
            .short("u")
            .long("users")
            .value_name("N")
            .default_value("100")
            .help("Number of users to prepopulate the database with"))
        .arg(Arg::with_name("csv")
            .required(false)
            .help("Print output in CSV format."))
        .get_matches();


    println!("Creating app...");
    let mut app = Piazza::new();
    println!("Done with app creation.");

    let nusers = value_t_or_exit!(args, "nusers", i64);
    let nclasses = value_t_or_exit!(args, "nclasses", i64);

    let class_putter = app.soup.get_mutator(app.class);
    let user_putter = app.soup.get_mutator(app.user);
    let taking_putter = app.soup.get_mutator(app.taking);
    let post_putter = app.soup.get_mutator(app.post);

    println!("Populating db...", );
    populate_users(nusers, user_putter);
    populate_classes(nclasses, class_putter);
    populate_taking(nclasses, nusers, taking_putter);
    println!("Finished seeding! Sleeping for 1 second...");
    thread::sleep(time::Duration::from_millis(1000));

    println!("Starting benchmark!");
    println!("Logging in users...");
    let start = time::Instant::now();
    let mut elapsed_secs: f64;
    for i in 0..nusers {
        app.log_user(i.into());
        write_posts(nclasses, i, &post_putter);
        let elapsed = time::Instant::now().duration_since(start);
        elapsed_secs = (elapsed.as_secs() as f64) +
                       (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        if elapsed_secs > 30.0 {
            break;
        }

        let avg : f64 = ((nusers as f64)*(nclasses as f64)*(i as f64) as f64) / elapsed_secs;
        // println!("PUTS {:?} posts in elapsed_secs {:?}", nusers*nclasses, elapsed_secs);
        println!("avg {:?}",  avg);
    }

    println!("Done with benchmark.");
}