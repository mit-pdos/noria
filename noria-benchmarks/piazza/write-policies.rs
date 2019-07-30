#![feature(duration_float)]

#[macro_use]
extern crate clap;

use noria::{ControllerHandle, ZookeeperAuthority, DataType, Builder, LocalAuthority, ReuseConfigType, SyncHandle, Handle};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::{thread, time};
use std::net::IpAddr;
use futures::future::Future;
use std::sync::Arc;
use zookeeper::ZooKeeper;
use noria::SyncControllerHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tokio::prelude::*;
use tokio::executor::Executor;

#[macro_use]
mod populate;

use crate::populate::Populate;

pub struct Backend {
    g: Handle<ZookeeperAuthority>,
    rt: tokio::runtime::Runtime,
}

#[derive(PartialEq)]
enum PopulateType {
    Before,
    After,
    NoPopulate,
}

pub enum DataflowType {
    Write,
    Read,
}


impl Backend {
    pub fn new(partial: bool, _shard: bool, reuse: &str, dftype: DataflowType) -> Backend {
        match dftype {
            DataflowType::Read => {
                println!("in backend new. read");
                let zk_address = "127.0.0.1:2181/read";
                let mut rt = tokio::runtime::Runtime::new().unwrap();
                let authority = Arc::new(ZookeeperAuthority::new(zk_address).unwrap());
                let mut cb = Builder::default();
                let log = noria::logger_pls();
                let blender_log = log.clone();
                cb.set_reuse(ReuseConfigType::NoReuse);
                let g = rt.block_on(cb.start(authority)).unwrap();
                cb.log_with(blender_log);
                Backend { g, rt }
            },
            DataflowType::Write => {
                println!("in backend new. write");
                let zk_address = "127.0.0.1:2181/write";
                let mut rt = tokio::runtime::Runtime::new().unwrap();

                let authority = Arc::new(ZookeeperAuthority::new(zk_address).unwrap());
                let mut cb = Builder::default();
                let log = noria::logger_pls();
                let blender_log = log.clone();

                cb.set_reuse(ReuseConfigType::NoReuse);

                let g = rt.block_on(cb.start(authority)).unwrap();

                cb.log_with(blender_log);
                Backend { g, rt }
            }
        }
    }

    pub fn populate<T>(&mut self, name: &'static str, mut records: Vec<Vec<DataType>>, db: &mut SyncControllerHandle<ZookeeperAuthority, T>)
    where T : tokio::executor::Executor
    {
        let get_table = move |b: &mut SyncControllerHandle<_, _>, n| loop {
            match b.table(n) {
                Ok(v) => return v.into_sync(),
                Err(x) => {
                    println!("{}", x);
                    panic!("tried to get table via controller handle and failed. should not happen!");
                    // thread::sleep(Duration::from_millis(50));
                    // *b = SyncControllerHandle::from_zk("127.0.0.1:2181/write", executor.clone())
                    //     .unwrap();
                }
            }
        };

        // Get mutators and getter.
        let mut table = get_table(db, name);
        for r in records.drain(..) {
            table
                .insert(r)
                .unwrap();
        }
    }

    fn set_security_config(&mut self, config_file: &str) {
        use std::io::Read;
        let mut config = String::new();
        let mut cf = File::open(config_file).unwrap();
        cf.read_to_string(&mut config).unwrap();

        // Install recipe with policies
        self.g.set_security_config(config);
    }

    fn migrate<T>(&mut self, schema_file: &str, query_file: Option<&str>, db: &mut SyncControllerHandle<ZookeeperAuthority, T>) -> Result<(), String>
    where T : tokio::executor::Executor
    {
        use std::io::Read;

        // Read schema file
        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();
        sf.read_to_string(&mut s).unwrap();

        let mut rs = s.clone();
        s.clear();

        // Read query file
        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s);
            }
        }

        db.extend_recipe(&rs).is_ok(); 
        Ok(()) 
    }

    fn login(&mut self, user_context: HashMap<String, DataType>) -> Result<(), String> {
        self.g.create_universe(user_context.clone()); 
        Ok(())
    }
}

fn make_user(id: i32) -> HashMap<String, DataType> {
    let mut user = HashMap::new();
    user.insert(String::from("id"), id.to_string().into());

    user
}

#[allow(clippy::cognitive_complexity)]
fn main() {
    use clap::{App, Arg};
    let args = App::new("piazza")
        .version("0.1")
        .about("Benchmarks Piazza-like application with security policies.")
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("noria-benchmarks/piazza/schema.sql")
                .help("Schema file for Piazza application"),
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("noria-benchmarks/piazza/post-queries.sql")
                .help("Query file for Piazza application"),
        )
        .arg(
            Arg::with_name("policies")
                .long("policies")
                .required(true)
                .default_value("noria-benchmarks/piazza/basic-policies.json")
                .help("Security policies file for Piazza application"),
        )
        .arg(
            Arg::with_name("graph")
                .short("g")
                .default_value("pgraph.gv")
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("info")
                .short("i")
                .takes_value(true)
                .help("Directory to dump runtime process info (doesn't work on OSX)"),
        )
        .arg(
            Arg::with_name("reuse")
                .long("reuse")
                .default_value("full")
                .possible_values(&["noreuse", "finkelstein", "relaxed", "full"])
                .help("Query reuse algorithm"),
        )
        .arg(
            Arg::with_name("shard")
                .long("shard")
                .help("Enable sharding"),
        )
        .arg(
            Arg::with_name("partial")
                .long("partial")
                .help("Enable partial materialization"),
        )
        .arg(
            Arg::with_name("populate")
                .long("populate")
                .default_value("before")
                .possible_values(&["after", "before", "nopopulate"])
                .help("Populate app with randomly generated data"),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .default_value("1")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("nlogged")
                .short("l")
                .default_value("1")
                .help(
                "Number of logged users. Must be less or equal than the number of users in the db",
            ),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .default_value("1")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("10")
                .help("Number of posts in the db"),
        )
        .arg(
            Arg::with_name("private")
                .long("private")
                .default_value("0.0")
                .help("Percentage of private posts"),
        )
        .arg(
            Arg::with_name("classes_per_user")
                .short("m")
                .default_value("10")
                .help("Number of classes each student is in"),
        )
        .arg(
            Arg::with_name("query_to_run")
                .short("z")
                .default_value("post_count")
                .help("Number of classes each student is in"),
        )
        .get_matches();

    println!("Starting benchmark...");

    // Read arguments
    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();
    let gloc = args.value_of("graph");
    let iloc = args.value_of("info");
    let partial = args.is_present("partial");
    let shard = args.is_present("shard");
    let reuse = args.value_of("reuse").unwrap();
    let populate = args.value_of("populate").unwrap_or("nopopulate");
    let nusers = value_t_or_exit!(args, "nusers", i32);
    let nlogged = value_t_or_exit!(args, "nlogged", i32);
    let nclasses = value_t_or_exit!(args, "nclasses", i32);
    let nposts = value_t_or_exit!(args, "nposts", i32);
    let private = value_t_or_exit!(args, "private", f32);
    let classes_per_student = value_t_or_exit!(args, "classes_per_user", i32);
    let query_type = args.value_of("query_to_run").unwrap_or("post_count");

    let correctness_test = false;

    assert!(
        nlogged <= nusers,
        "nusers must be greater or equal to nlogged"
    );
    assert!(
        nusers >= populate::TAS_PER_CLASS as i32,
        "nusers must be greater or equal to TAS_PER_CLASS"
    );

    println!("Initializing database schema...");
    // let mut backend = Backend::new(partial, shard, reuse);
    // backend.migrate(sloc, None).unwrap();
    // backend.set_security_config(ploc);
    // backend.migrate(sloc, Some(qloc)).unwrap();
    let mut write_df = Backend::new(partial, shard, reuse, DataflowType::Write);
    let wexecutor = write_df.rt.executor();
    let mut write_db = SyncControllerHandle::from_zk("127.0.0.1:2181/write", wexecutor.clone()).unwrap();
    write_df.migrate(sloc, None, &mut write_db).unwrap();
    write_df.set_security_config(ploc);
    write_df.migrate(sloc, Some(qloc), &mut write_db).unwrap();


    let populate = match populate {
        "before" => PopulateType::Before,
        "after" => PopulateType::After,
        _ => PopulateType::NoPopulate,
    };

    let mut p = Populate::new(nposts, nusers, nclasses, private);

    p.enroll_students(classes_per_student);

    let classes = p.get_classes();
    let users = p.get_users();
    let roles = p.get_roles();
    let posts = p.get_posts();

    write_df.populate("Role", roles, &mut write_db);
    println!("Waiting for groups to be constructed...");
    thread::sleep(time::Duration::from_millis(120 * (nclasses as u64)));

    write_df.populate("User", users, &mut write_db);
    write_df.populate("Class", classes, &mut write_db);

    if !correctness_test {
        if populate == PopulateType::Before {
            write_df.populate("Post", posts.clone(), &mut write_db);
            println!("Waiting for posts to propagate...");
            thread::sleep(time::Duration::from_millis((nposts / 10) as u64));
        }

        println!("Finished writing! Sleeping for 2 seconds...");
        thread::sleep(time::Duration::from_millis(2000));

        // if partial, read 25% of the keys
        // if partial && query_type == "posts" {
        //     let leaf = "posts".to_string();
        //     let mut getter = write_df.g.view(&leaf).unwrap().into_sync();
        //     for author in 0..nusers / 4 {
        //         getter.lookup(&[author.into()], false).unwrap();
        //     }
        // }

        // if partial && query_type == "post_count" {
        //     let leaf = "post_count".to_string();
        //     let mut getter = backend.g.view(&leaf).unwrap().into_sync();
        //     for author in 0..nusers / 4 {
        //         getter.lookup(&[author.into()], false).unwrap();
        //     }
        // }

        // Login a user
        println!("Login users...");
        for i in 0..nlogged {
            let start = time::Instant::now();
            write_df.login(make_user(i)).is_ok();
            let dur = start.elapsed().as_secs_f64();
            println!("Migration {} took {:.2}s!", i, dur,);

            // if partial, read 25% of the keys
            // if partial && query_type == "posts" {
            //     let leaf = format!("posts_u{}", i);
            //     let mut getter = backend.g.view(&leaf).unwrap().into_sync();
            //     for author in 0..nusers / 4 {
            //         getter.lookup(&[author.into()], false).unwrap();
            //     }
            // }
            // if partial && query_type == "post_count" {
            //     let leaf = format!("post_count_u{}", i);
            //     let mut getter = backend.g.view(&leaf).unwrap().into_sync();
            //     for author in 0..nusers / 4 {
            //         getter.lookup(&[author.into()], false).unwrap();
            //     }
            // }

            // if iloc.is_some() && i % 50 == 0 {
            //     use std::fs;
            //     let fname = format!("{}-{}", iloc.unwrap(), i);
            //     fs::copy("/proc/self/status", fname).unwrap();
            // }
        }

        // if populate == PopulateType::After {
        //     backend.populate("Post", posts);
        // }

      
        // let mut dur = time::Duration::from_millis(0);

        // // --- Posts Query ---
        // if query_type == "posts" {
        //     let enrollment_info = p.get_enrollment();
        //     for uid in 0..nlogged {
        //         match enrollment_info.get(&uid.into()) {
        //             Some(classes) => {
        //                 // println!("user {:?} is enrolled in classes: {:?}", uid, classes);
        //                 let mut class_vec = Vec::new();
        //                 for class in classes {
        //                     class_vec.push([class.clone()].to_vec());
        //                 }
        //                 let leaf = format!("posts_u{}", uid);
        //                 let mut getter = backend.g.view(&leaf).unwrap().into_sync();
        //                 let start = time::Instant::now();
        //                 getter.multi_lookup(class_vec.clone(), true).unwrap();
        //                 dur += start.elapsed();
        //             }
        //             None => println!("why isn't user {:?} enrolled", uid),
        //         }
        //     }
        // }

        // // cid version of post_count query
        // if query_type == "post_count" {
        //     let counts = p.classes();
        //     for (class, count) in &counts {
        //         println!("class: {:?}, count: {:?}", class, count);
        //     }
        //     let enrollment_info = p.get_enrollment();
        //     for uid in 0..nlogged {
        //         match enrollment_info.get(&uid.into()) {
        //             Some(classes) => {
        //                 // println!("user {:?} is enrolled in classes: {:?}", uid, classes);
        //                 let mut class_vec = Vec::new();
        //                 for class in classes {
        //                     class_vec.push([class.clone()].to_vec());
        //                 }
        //                 let leaf = format!("post_count_u{}", uid);

        //                 let mut getter = backend.g.view(&leaf).unwrap().into_sync();
        //                 let start = time::Instant::now();
        //                 getter.multi_lookup(class_vec.clone(), true).unwrap();
        //                 dur += start.elapsed();
        //             }
        //             None => println!("why isn't user {:?} enrolled", uid),
        //         }
        //     }
        // }

        // let dur = dur.as_secs_f64();

        // if query_type == "posts" {
        //     let posts_per_class = nposts / nclasses;
        //     println!(
        //         "Read {} keys in {:.4}s ({:.4} GETs/sec)!",
        //         classes_per_student * nlogged * posts_per_class,
        //         dur,
        //         f64::from(nclasses * nlogged * posts_per_class) / dur,
        //     );
        // } else if query_type == "post_count" {
        //     println!(
        //         "Read {} keys in {:.4}s ({:.4} GETs/sec)!",
        //         classes_per_student * nlogged,
        //         dur,
        //         f64::from(classes_per_student * nlogged) / dur,
        //     );
        // }

        // println!("Done with benchmark.");

        // if gloc.is_some() {
        //     let graph_fname = gloc.unwrap();
        //     let mut gf = File::create(graph_fname).unwrap();
        //     assert!(write!(gf, "{}", backend.g.graphviz().unwrap()).is_ok());
        // }
    } 
}

// author version of post count query
// if query_type == "post_count" {
//     println!("post count query");
//     let mut authors = p.authors();
//     for (author, count) in &authors {
//         println!("author: {:?}, count: {:?}", author, count);
//     }
//
//     let mut lookup_vec : Vec<Vec<DataType>>= Vec::new();
//     for inner in 0..nlogged {
//         lookup_vec.push([inner.clone().into()].to_vec());
//     }
//     for uid in 0..nlogged {
//         let leaf = format!("post_count_u{}", uid);
//         let mut getter = backend.g.view(&leaf).unwrap();
//         let start = time::Instant::now();
//         let res = getter.multi_lookup(lookup_vec.clone(), true);
//         dur += start.elapsed();
//         // println!("res: {:?}", res);
//     }
// }

// if !partial && query_type == "post_count" {
//     let mut authors = p.authors();
//     for (author, count) in &authors {
//         println!("author: {:?}, count: {:?}", author, count);
//     }
//
//     let num_at_once = nclasses as usize;
//     let mut enrollment_info = p.get_enrollment();
//
//     let mut class_to_students: HashMap<DataType, Vec<DataType>> = HashMap::new();
//     for (student, classes) in &enrollment_info {
//         for class in classes {
//             match class_to_students.get_mut(&class) {
//                 Some(student_list) => {
//                     student_list.push(student.clone());
//                 },
//                 None => {
//                     let mut stud_list = Vec::new();
//                     stud_list.push(student.clone());
//                     class_to_students.insert(class.clone(), stud_list);
//                 }
//             }
//         }
//     }
//
//
//     for uid in 0..nlogged {
//         match enrollment_info.get(&uid.into()) {
//             Some(classes) => {
//                 println!("user {:?} is enrolled in classes: {:?}", uid, classes);
//                 let mut query_vec = Vec::new();
//                 for class_id in classes {
//                     match class_to_students.get(&class_id) {
//                         Some(list) => {
//                             for student in list {
//                                 query_vec.push([student.clone()].to_vec());
//                             }
//                         },
//                         None => {},
//                     }
//                 }
//                 let leaf = format!("post_count_u{}", uid);
//                 let mut getter = backend.g.view(&leaf).unwrap();
//                 let start = time::Instant::now();
//                 let res = getter.multi_lookup(query_vec.clone(), true);
//                 println!("res: {:?}", res);
//                 dur += start.elapsed();
//
//             },
//             None => println!("why isn't user {:?} enrolled", uid),
//         }
//     }
// }
