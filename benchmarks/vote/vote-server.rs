extern crate clap;
extern crate distributary;
extern crate gulaschkanone;

mod graph;

use distributary::{srv, Blender};

use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use std::{time, thread};

fn main() {
    use clap::{Arg, App};
    let args = App::new("vote")
        .version("0.1")
        .about(
            "Benchmarks user-curated news aggregator throughput for different storage \
             backends.",
        )
        .arg(
            Arg::with_name("ADDR")
                .index(1)
                .help("Address and port to listen on")
                .required(true),
        )
        .arg(
            Arg::with_name("distributed")
                .long("distributed")
                .takes_value(false)
                .help("Run in distributed mode."),
        )
        .get_matches();

    let addr = args.value_of("ADDR").unwrap();
    println!("Attempting to start soup on {}", addr);

    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::DeleteOnExit,
        512,
        time::Duration::from_millis(1),
    );

    let blender = Arc::new(Mutex::new(Blender::new()));

    if args.is_present("distributed") {
        use gulaschkanone::{Config, Controller};

        let config = Config {
            hostname: String::from("localhost"),
            addr: String::from("127.0.0.1"),
            port: 9999,
            controller: None,        // we are the controller
            heartbeat_freq: 1000,    // 1s
            healthcheck_freq: 10000, // 10s
        };

        let log = distributary::logger_pls();

        let mut controller = Controller::new(
            blender.clone(),
            &config.addr,
            config.port,
            time::Duration::from_millis(config.heartbeat_freq),
            time::Duration::from_millis(config.healthcheck_freq),
            log.clone(),
        );

        // run controller in the background
        let _ = thread::spawn(move || { controller.listen(); });

        // wait for a worker to connect
        println!("waiting 10s for a worker to connect...");
        thread::sleep(time::Duration::from_millis(10000));
    }

    // scoped needed to ensure lock is released
    let g = graph::make(blender.clone(), true, false, persistence_params);

    // start processing
    // TODO: what about the node indices?
    srv::run(g.graph, addr.to_socket_addrs().unwrap().next().unwrap());
}
