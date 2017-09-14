#[macro_use]
extern crate clap;
extern crate distributary;
extern crate gulaschkanone;
#[macro_use]
extern crate slog;

mod graph;

use distributary::{srv, Blender};

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::{thread, time};

fn main() {
    use clap::{App, Arg};
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
                .requires("NUM_WORKERS")
                .takes_value(false)
                .help("Run in distributed mode."),
        )
        .arg(
            Arg::with_name("durability")
                .long("durability")
                .takes_value(false)
                .help("Enable durability for Base nodes"),
        )
        .arg(
            Arg::with_name("NUM_WORKERS")
                .long("workers")
                .requires("distributed")
                .takes_value(true)
                .help(
                    "Number of workers to expect. Once this many workers are present, \
                     data-flow graph is set up.",
                ),
        )
        .get_matches();

    let addr = args.value_of("ADDR").unwrap();
    let durability = if args.is_present("durability") {
        distributary::DurabilityMode::DeleteOnExit
    } else {
        distributary::DurabilityMode::MemoryOnly
    };

    println!("Attempting to start soup on {}", addr);

    let persistence_params =
        distributary::PersistenceParameters::new(durability, 512, time::Duration::from_millis(1));

    let sock_addr: SocketAddr = addr.parse()
        .expect("ADDR must be a valid HOST:PORT combination");
    let blender = Arc::new(Mutex::new(Blender::with_listen(sock_addr.ip())));

    let jh = if args.is_present("distributed") {
        use gulaschkanone::{Config, Controller};

        let num_workers_expected = value_t_or_exit!(args, "NUM_WORKERS", usize);

        let config = Config {
            hostname: String::from("localhost"),
            addr: sock_addr.ip().to_string(),
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
        let builder = thread::Builder::new().name("gulaschkanone-ctrl".into());
        let jh = builder
            .spawn(move || {
                controller.listen();
            })
            .unwrap();

        // wait for a worker to connect
        info!(
            log,
            "waiting for {} workers to connect...",
            num_workers_expected
        );

        let mut wc = 0;
        while wc < num_workers_expected {
            // need this nesting so that we don't hold the lock for too long (worker
            // registration needs it!)
            {
                let blender = blender.lock().unwrap();
                wc = blender.worker_count();
            }
            thread::sleep(time::Duration::from_millis(1000));
        }

        info!(log, "workers are here; let's get going!");

        Some(jh)
    } else {
        None
    };

    // scoped needed to ensure lock is released
    let g = graph::make(blender.clone(), true, false, persistence_params);

    // start processing
    // TODO: what about the node indices?
    srv::run(g.graph, addr.to_socket_addrs().unwrap().next().unwrap());

    if jh.is_some() {
        jh.unwrap()
            .join()
            .expect("failed to join controller thread");
    }
}
