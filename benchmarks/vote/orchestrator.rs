extern crate clap;
extern crate ssh2;
extern crate whoami;

mod ssh;
use ssh::Ssh;

mod server;

use std::collections::HashMap;
use std::io::prelude::*;
use std::error::Error;

#[derive(Debug, PartialEq, Eq)]
enum Backend {
    Netsoup {
        workers: usize,
        readers: usize,
        shards: usize,
    },
}

impl Backend {
    fn multiclient_name(&self) -> &'static str {
        match *self {
            Backend::Netsoup { .. } => "netsoup",
        }
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-orchestrator")
        .version("0.1")
        .about("Orchestrate many runs of the vote benchmark")
        .arg(
            Arg::with_name("addr")
                .long("address")
                .default_value("127.0.0.1")
                .required(true)
                .help("Listening address for server"),
        )
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .value_name("N")
                .default_value("4")
                .help("Number of client load threads to run"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("15")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("server")
                .help("Sets a host to use as the server")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("client")
                .help("Sets a host to use as a client")
                .required(true)
                .multiple(true)
                .index(2),
        )
        .get_matches();

    // what backends are we benchmarking?
    let mut backends = HashMap::new();
    backends.insert(
        "ns",
        Backend::Netsoup {
            workers: 1,
            readers: 1,
            shards: 1,
        },
    );

    // make sure we can connect to the server
    let server = Ssh::connect(args.value_of("server").unwrap()).unwrap();

    // connect to each of the clients
    let clients: HashMap<_, _> = args.values_of("client")
        .unwrap()
        .map(|client| {
            Ssh::connect(client).map(|ssh| {
                let has_pl = ssh.just_exec("perflock").unwrap();
                (client, (ssh, has_pl))
            })
        })
        .collect::<Result<_, _>>()
        .unwrap();

    let server_has_pl = server.just_exec("perflock").unwrap();
    eprintln!("server has perflock? {:?}", server_has_pl);

    // build `vote` on each client (in parallel)
    let build_chans: Vec<_> = clients
        .iter()
        .map(|(host, &(ref ssh, has_pl))| {
            let mut c = ssh.channel_session().unwrap();
            c.shell().unwrap();
            c.write_all(b"cd distributary\n").unwrap();

            if has_pl {
                c.write_all(b"pls ").unwrap();
            }
            c.write_all(b"cargo b --release --manifest-path benchmarks/Cargo.toml --bin vote")
                .unwrap();
            c.send_eof().unwrap();

            (host, c)
        })
        .collect();
    for (host, mut chan) in build_chans {
        let mut stderr = String::new();
        chan.stderr().read_to_string(&mut stderr).unwrap();
        chan.wait_eof().unwrap();

        match chan.exit_status().unwrap() {
            0 => eprintln!("{} finished building vote", host),
            _ => {
                eprintln!("{} failed to build vote:", host);
                eprintln!("{}", stderr);
                return;
            }
        }
    }

    let listen_addr = args.value_of("addr").unwrap();
    for (name, backend) in backends {
        let s = match server::start(&server, server_has_pl, &backend).unwrap() {
            Ok(s) => s,
            Err(e) => {
                println!("failed to start {:?}: {:?}", backend, e);
                continue;
            }
        };

        let results = run_clients(&clients, &backend);

        // TODO: actually log the results somewhere
        // maybe we create a file and give to run_clients to avoid storing in memory?

        s.end(&backend).unwrap();
    }
}

fn run_clients(clients: &HashMap<&str, (Ssh, bool)>, benchmark: &Backend) -> () {
    // start all the benchmark clients
    let workers: Vec<_> = clients
        .iter()
        .map(|(host, &(ref ssh, has_pl))| {
            // closure just to enable use of ?
            let c = || -> Result<_, Box<Error>> {
                let mut c = ssh.channel_session()?;
                if has_pl {
                    c.write_all(b"perflock ")?;
                }
                c.write_all(b"multiclient.sh ")?;
                c.write_all(benchmark.multiclient_name().as_bytes())?;
                // TODO: pass listen address to multiclient.sh
                // TODO: pass other useful flags to vote
                c.send_eof()?;
                Ok(c)
            };

            (host, c())
        })
        .collect();

    // let's see how we did
    for (host, chan) in workers {
        match chan {
            Ok(mut chan) => {
                let mut stderr = String::new();
                chan.stderr().read_to_string(&mut stderr).unwrap();
                chan.wait_eof().unwrap();

                if chan.exit_status().unwrap() != 0 {
                    eprintln!("{} failed to run benchmark client:", host);
                    eprintln!("{}", stderr);
                    eprintln!("");
                }
            }
            Err(e) => {
                eprintln!("{} failed to run benchmark client:", host);
                eprintln!("{:?}", e);
                eprintln!("");
            }
        }
    }

    // TODO: gather stats or something? stdout maybe?
}
