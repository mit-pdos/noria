#![feature(catch_expr)]

#[macro_use]
extern crate clap;
extern crate shellwords;
extern crate ssh2;
extern crate whoami;

mod ssh;
use ssh::Ssh;

mod server;

mod backends;
use backends::Backend;

use std::io::prelude::*;
use std::error::Error;
use std::borrow::Cow;

#[derive(Clone, Copy)]
struct ClientParameters<'a> {
    listen_addr: &'a str,
    backend: &'a Backend,
    runtime: usize,
    articles: usize,
    // TODO: ratio
    // TODO: distribution
}

impl<'a> ClientParameters<'a> {
    fn add_params(&'a self, cmd: &mut Vec<Cow<'a, str>>) {
        cmd.push(self.backend.multiclient_name().into());
        cmd.push(self.listen_addr.into());
        cmd.push("-r".into());
        cmd.push(format!("{}", self.runtime).into());
        cmd.push("-a".into());
        cmd.push(format!("{}", self.articles).into());
    }

    fn name(&self, target: usize, ext: &str) -> String {
        format!(
            "{}.{}a.{}t.{}",
            self.backend.uniq_name(),
            self.articles,
            target,
            ext
        )
    }
}

struct HostDesc<'a> {
    name: &'a str,
    has_perflock: bool,
    threads: usize,
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-orchestrator")
        .version("0.1")
        .about("Orchestrate many runs of the vote benchmark")
        .arg(
            Arg::with_name("addr")
                .long("address")
                // 172.16.0.29
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
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("20")
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
                .help("Sets a host to use as a client ([user@]host[:port],threads)")
                .required(true)
                .multiple(true)
                .index(2),
        )
        .get_matches();

    let runtime = value_t_or_exit!(args, "runtime", usize);
    let articles = value_t_or_exit!(args, "articles", usize);

    // what backends are we benchmarking?
    let backends = vec![
        Backend::Netsoup {
            workers: 2,
            readers: 16,
            shards: None,
        },
        Backend::Netsoup {
            workers: 2,
            readers: 16,
            shards: Some(2),
        },
        Backend::Mysql,
        Backend::Mssql,
        Backend::Memcached,
    ];

    // make sure we can connect to the server
    eprintln!("==> connecting to server");
    let server = Ssh::connect(args.value_of("server").unwrap()).unwrap();

    let server_has_pl = server.just_exec(&["perflock"]).unwrap().is_ok();
    eprintln!(" -> perflock? {:?}", server_has_pl);

    // connect to each of the clients
    eprintln!("==> connecting to clients");
    let clients: Vec<_> = args.values_of("client")
        .unwrap()
        .map(|client| {
            let mut parts = client.split(',');
            let client = parts.next().unwrap();
            let threads: usize = parts
                .next()
                .ok_or(format!("no thread count given for host {}", client))
                .and_then(|t| {
                    t.parse().map_err(|e| {
                        format!("invalid thread count given for host {}: {}", client, e)
                    })
                })?;

            eprintln!(" -> {}", client);
            Ssh::connect(client).map(|ssh| {
                let has_perflock = ssh.just_exec(&["perflock"]).unwrap().is_ok();
                eprintln!(" .. connected; perflock? {:?}", has_perflock);
                (
                    ssh,
                    HostDesc {
                        has_perflock,
                        threads,
                        name: client,
                    },
                )
            })
        })
        .collect::<Result<_, _>>()
        .unwrap();

    // build `vote` on each client (in parallel)
    eprintln!("==> building vote benchmark on clients");
    let build_chans: Vec<_> = clients
        .iter()
        .map(|&(ref ssh, ref host)| {
            eprintln!(" -> starting on {}", host.name);
            let mut cmd = vec!["cd", "distributary", "&&"];
            if host.has_perflock {
                cmd.push("pls");
            }
            cmd.extend(vec![
                "cargo",
                "b",
                "--release",
                "--manifest-path",
                "benchmarks/Cargo.toml",
                "--bin",
                "vote",
            ]);

            (host, ssh.exec(&cmd[..]).unwrap())
        })
        .collect();
    for (host, mut chan) in build_chans {
        let mut stderr = String::new();
        chan.stderr().read_to_string(&mut stderr).unwrap();
        chan.wait_eof().unwrap();

        match chan.exit_status().unwrap() {
            0 => eprintln!(" .. {} finished", host.name),
            _ => {
                eprintln!("{} failed to build vote:", host.name);
                eprintln!("{}", stderr);
                return;
            }
        }
    }

    let listen_addr = args.value_of("addr").unwrap();
    for backend in backends {
        eprintln!("==> {}", backend.uniq_name());

        eprintln!(" -> starting server");
        let mut s = match server::start(&server, listen_addr, server_has_pl, &backend).unwrap() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to start {:?}: {:?}", backend, e);
                continue;
            }
        };

        // give the server a little time to get its stuff together
        if let Err(e) = s.wait(&clients[0].0, &backend) {
            eprintln!("failed to start {:?}: {:?}", backend, e);
            continue;
        }
        eprintln!(" .. server started ");

        let params = ClientParameters {
            backend: &backend,
            listen_addr,
            runtime,
            articles,
        };

        let targets = [
            5000, 10000, 50000, 100000, 500000, 1000000, 2000000, 3000000, 4000000
        ];
        // TODO: run more iterations
        for (i, &target) in targets.iter().enumerate() {
            if i != 0 {
                s = s.between_targets(&backend).unwrap();

                // wait in case server was restarted
                if let Err(e) = s.wait(&clients[0].0, &backend) {
                    eprintln!("failed to restart {:?}: {:?}", backend, e);
                    continue;
                }
            }

            eprintln!(" -> {}", params.name(target, ""));
            run_clients(&clients, &mut s, target, params);

            // TODO: if backend clearly couldn't handle the load, don't run higher targets
        }

        eprintln!(" -> stopping server");
        s.end(&backend).unwrap();
        eprintln!(" .. server stopped ");
    }
}

fn run_clients(
    clients: &Vec<(Ssh, HostDesc)>,
    server: &mut server::Server,
    target: usize,
    params: ClientParameters,
) -> () {
    // first, we need to prime from some host -- doesn't really matter which
    {
        let (ref ssh, ref host) = clients[0];
        ssh.set_timeout(0);
        eprintln!(" .. prepopulating on {}", host.name);

        let mut prime_params = params.clone();
        prime_params.runtime = 0;
        let mut cmd = Vec::<Cow<str>>::new();
        if host.has_perflock {
            cmd.push("perflock".into());
        }
        cmd.push("multiclient.sh".into());
        prime_params.add_params(&mut cmd);
        cmd.push("--threads".into());
        cmd.push(format!("{}", host.threads).into());
        let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();

        match ssh.just_exec(&cmd[..]) {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("{} failed to populate:", host.name);
                eprintln!("{}", e);
                eprintln!("");
                return;
            }
            Err(e) => {
                eprintln!("{} failed to populate:", host.name);
                eprintln!("{:?}", e);
                eprintln!("");
                return;
            }
        }

        eprintln!(" .. finished prepopulation");
    }

    let total_threads: usize = clients.iter().map(|&(_, ref host)| host.threads).sum();
    let ops_per_thread = target as f64 / total_threads as f64;

    // start all the benchmark clients
    let workers: Vec<_> = clients
        .iter()
        .filter_map(|&(ref ssh, ref host)| {
            // so, target ops...
            // target ops is actually not entirely straightforward to determine. here, we'll
            // assume that each client thread is able to process requests at roughly the same
            // rate, so we divide the target ops among machines based on the number of threads
            // they have. this may or may not be the right thing.
            let target = (ops_per_thread * host.threads as f64).ceil() as usize;

            eprintln!(
                " .. starting benchmarker on {} with target {}",
                host.name, target
            );

            let c: Result<_, Box<Error>> = do catch {
                let mut cmd = Vec::<Cow<str>>::new();
                if host.has_perflock {
                    cmd.push("perflock".into());
                }
                cmd.push("multiclient.sh".into());
                params.add_params(&mut cmd);
                cmd.push("--no-prime".into());
                cmd.push("--threads".into());
                cmd.push(format!("{}", host.threads).into());

                // so, target ops...
                // target ops is actually not entirely straightforward to determine. here, we'll
                // assume that each client thread is able to process requests at roughly the same
                // rate, so we divide the target ops among machines based on the number of threads
                // they have. this may or may not be the right thing.
                cmd.push("--target".into());
                cmd.push(format!("{}", target).into());

                let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                let c = ssh.exec(&cmd[..])?;
                Ok(c)
            };

            match c {
                Ok(c) => Some((host, c)),
                Err(e) => {
                    eprintln!("{} failed to run benchmark client:", host.name);
                    eprintln!("{:?}", e);
                    eprintln!("");
                    None
                }
            }
        })
        .collect();

    // let's see how we did
    use std::fs::File;
    let fname = params.name(target, "log");
    let mut outf = File::create(&fname);
    if let Err(ref e) = outf {
        eprintln!(" !! failed to open output file {}: {}", fname, e);
    }

    eprintln!(" .. waiting for benchmark to complete");
    for (host, mut chan) in workers {
        if let Ok(ref mut f) = outf {
            use std::io;

            // TODO: should we get histogram files here instead and merge them?
            io::copy(&mut chan, f).unwrap();
        }

        let mut stderr = String::new();
        chan.stderr().read_to_string(&mut stderr).unwrap();
        chan.wait_eof().unwrap();
        chan.wait_close().unwrap();

        if chan.exit_status().unwrap() != 0 {
            eprintln!("{} failed to run benchmark client:", host.name);
            eprintln!("{}", stderr);
            eprintln!("");
        }
    }

    if let Ok(mut f) = outf {
        // also gather memory usage and stuff
        if f.write_all(b"# server stats:\n").is_ok() {
            if let Err(e) = server.write_stats(params.backend, &mut f) {
                eprintln!(" !! failed to gather server stats: {}", e);
            }
        }
    }
}
