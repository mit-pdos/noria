#![feature(catch_expr)]

#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rusoto_core;
extern crate rusoto_ec2;
extern crate rusoto_sts;
extern crate scopeguard;
extern crate shellwords;
extern crate ssh2;
extern crate timeout_readwrite;
extern crate whoami;

mod ssh;
use ssh::Ssh;

mod server;

mod backends;
use backends::Backend;

use std::io::prelude::*;
use std::error::Error;
use std::borrow::Cow;

const SOUP_AMI: &str = "ami-1bfdf761";

#[derive(Clone, Copy)]
struct ClientParameters<'a> {
    listen_addr: &'a str,
    backend: &'a Backend,
    runtime: usize,
    articles: usize,
    read_percentage: usize,
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
        cmd.push("--write-every".into());
        let write_every = match self.read_percentage {
            0 => 1,
            100 => {
                // very rare
                1_000_000_000_000
            }
            n => {
                // 95 (%) == 1 in 20
                // we want a whole number x such that 1-1/x ~= n/100
                // so x - 1 = nx/100
                // so x - nx/100 = 1
                // so (1 - n/100) x = 1
                // so x = 1 / (1 - n/100)
                // so 1 / (100 - n)/100
                // so 100 / (100 - n)
                let div = 100usize.checked_sub(n).expect("percentage >= 100");
                if 100 % div != 0 {
                    panic!("{}% cannot be expressed as 1 in n", n);
                }
                100 / div
            }
        };
        cmd.push(format!("{}", write_every).into());
    }

    fn name(&self, target: usize, ext: &str) -> String {
        format!(
            "{}.{}a.{}t.{}r.{}",
            self.backend.uniq_name(),
            self.articles,
            self.read_percentage,
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
    use clap::{App, Arg, SubCommand};

    let args = App::new("vote-orchestrator")
        .version("0.1")
        .about("Orchestrate many runs of the vote benchmark")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .takes_value(true)
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("20")
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("read_percentage")
                .short("p")
                .default_value("95")
                .takes_value(true)
                .help("The percentage of operations that are reads"),
        )
        .subcommand(
            SubCommand::with_name("ec2")
                .about("run on ec2 spot blocks")
                .arg(
                    Arg::with_name("stype")
                        .long("server")
                        .default_value("c5.large")
                        .required(true)
                        .takes_value(true)
                        .help("Instance type for server"),
                )
                .arg(
                    Arg::with_name("ctype")
                        .long("client")
                        .default_value("c5.large")
                        .required(true)
                        .takes_value(true)
                        .help("Instance type for clients"),
                )
                .arg(
                    Arg::with_name("clients")
                        .long("clients")
                        .short("c")
                        .required(true)
                        .takes_value(true)
                        .help("Number of client machines to spawn"),
                ),
        )
        .subcommand(
            SubCommand::with_name("manual")
                .about("manually specify target machines")
                .arg(
                    Arg::with_name("addr")
                        .long("address")
                        .default_value("127.0.0.1")
                        .required(true)
                        .help("Listening address for server"),
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
                ),
        )
        .get_matches();

    let runtime = value_t_or_exit!(args, "runtime", usize);
    let articles = value_t_or_exit!(args, "articles", usize);
    let read_percentage = value_t_or_exit!(args, "read_percentage", usize);

    let mut server = None;
    let mut clients = Vec::new();
    let mut listen_addr = None;
    let mut ec2_cleanup = None; // drop guard that terminates instances

    match args.subcommand() {
        ("manual", Some(args)) => {
            server = Some(args.value_of("server").unwrap().to_string());
            clients = args.values_of("client")
                .unwrap()
                .into_iter()
                .map(|c| c.to_string())
                .collect();
            listen_addr = Some(args.value_of("addr").unwrap().to_string());
        }
        ("ec2", Some(args)) => {
            use rusoto_core::{EnvironmentProvider, Region};
            use rusoto_core::default_tls_client;
            use rusoto_ec2::{Ec2, Ec2Client};
            use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};

            let nclients = value_t_or_exit!(args, "clients", i64);

            // guess the core count
            let cores = args.value_of("ctype")
                .and_then(ec2_instance_type_cores)
                .map(|cores| {
                    // one core for load generators to be on the safe side
                    match cores {
                        1 => 1,
                        2 => 1,
                        n => n - 1,
                    }
                });

            if cores.is_none() {
                eprintln!(
                    "unknown core count for client instance type {}",
                    args.value_of("ctype").unwrap()
                );
                return;
            }
            let cores = cores.unwrap();

            // https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
            let sts = StsClient::new(
                default_tls_client().unwrap(),
                EnvironmentProvider,
                Region::UsEast1,
            );
            let provider = StsAssumeRoleSessionCredentialsProvider::new(
                sts,
                "arn:aws:sts::125163634912:role/soup".to_owned(),
                "vote-benchmark".to_owned(),
                None,
                None,
                None,
                None,
            );

            let ec2 = Ec2Client::new(default_tls_client().unwrap(), provider, Region::UsEast1);
            let mut all_spot_requests = Vec::new();

            let mut template = rusoto_ec2::RequestSpotInstancesRequest::default();
            template.block_duration_minutes = Some(60);
            let mut launch = rusoto_ec2::RequestSpotLaunchSpecification::default();
            launch.image_id = Some(SOUP_AMI.to_string());
            launch.instance_type = Some(args.value_of("stype").unwrap().to_string());
            launch.security_groups = Some(vec!["pdos-ssh".to_string(), "vpc-internal".to_string()]);
            launch.key_name = Some("jfrg-old".to_string());
            template.launch_specification = Some(launch);

            // launch server
            eprintln!("==> requesting ec2 spot instances");
            match ec2.request_spot_instances(&template) {
                Ok(requests) => {
                    let requests = requests.spot_instance_requests.unwrap();
                    let mut placement = rusoto_ec2::SpotPlacement::default();
                    placement.availability_zone = requests[0].launched_availability_zone.clone();
                    template.launch_specification.as_mut().unwrap().placement = Some(placement);
                    all_spot_requests.extend(requests);
                }
                Err(e) => {
                    eprintln!("failed to launch server instance: {}", e);
                    return;
                }
            }

            // launch clients
            template
                .launch_specification
                .as_mut()
                .unwrap()
                .instance_type = Some(args.value_of("ctype").unwrap().to_string());
            template.instance_count = Some(nclients);
            match ec2.request_spot_instances(&template) {
                Ok(requests) => {
                    let requests = requests.spot_instance_requests.unwrap();
                    all_spot_requests.extend(requests);
                }
                Err(e) => {
                    eprintln!("failed to launch client instances: {}", e);
                }
            }

            let mut request_ids: Vec<_> = all_spot_requests
                .into_iter()
                .map(|r| r.spot_instance_request_id.unwrap())
                .collect();

            // wait for them all to launch
            let ec2_instances: Vec<_>;
            eprintln!("==> waiting for all instances to start");
            let mut desc = rusoto_ec2::DescribeSpotInstanceRequestsRequest::default();
            desc.spot_instance_request_ids = Some(request_ids.clone());
            loop {
                match ec2.describe_spot_instance_requests(&desc) {
                    Ok(reqs) => {
                        let reqs = reqs.spot_instance_requests.unwrap();
                        if reqs.iter().any(|req| req.state.as_ref().unwrap() == "open") {
                            continue;
                        }

                        // server comes first
                        assert_eq!(
                            reqs[0].spot_instance_request_id.as_ref().unwrap(),
                            &request_ids[0]
                        );
                        ec2_instances =
                            reqs.into_iter().filter_map(|req| req.instance_id).collect();
                        break;
                    }
                    Err(e) => {
                        let e = format!("failed to describe spot instances: {}", e);
                        if !e.contains("does not exist") {
                            eprintln!("{}", e);
                        }
                    }
                }
            }

            // stop all the requests so more instances aren't created
            let mut c = rusoto_ec2::CancelSpotInstanceRequestsRequest::default();
            c.spot_instance_request_ids = request_ids;
            while let Err(e) = ec2.cancel_spot_instance_requests(&c) {
                eprintln!("failed to stop requests: {}; retrying", e);
            }

            // make sure we got what we asked for
            if ec2_instances.len() as i64 != nclients + 1 {
                eprintln!(
                    "only {} instances were created. exiting...",
                    ec2_instances.len()
                );
                let mut c = rusoto_ec2::TerminateInstancesRequest::default();
                c.instance_ids = ec2_instances;
                while let Err(e) = ec2.terminate_instances(&c) {
                    eprintln!("failed to terminate requests: {}; retrying", e);
                }
                return;
            }

            // collect the various hosts to connect to
            'retry: loop {
                let mut c = rusoto_ec2::DescribeInstancesRequest::default();
                c.instance_ids = Some(ec2_instances.clone());
                match ec2.describe_instances(&c) {
                    Ok(instances) => {
                        for res in instances.reservations.unwrap() {
                            for instance in res.instances.unwrap() {
                                if instance
                                    .public_dns_name
                                    .as_ref()
                                    .map(|n| n.is_empty())
                                    .unwrap_or(true)
                                {
                                    // no dns name yet -- retry
                                    clients.clear();
                                    continue 'retry;
                                }

                                if instance.instance_id.as_ref().unwrap() == &ec2_instances[0] {
                                    // server
                                    server = Some(format!(
                                        "ec2-user@{}",
                                        instance.public_dns_name.as_ref().unwrap()
                                    ));
                                    listen_addr =
                                        Some(instance.private_ip_address.clone().unwrap());
                                } else {
                                    // client
                                    clients.push(format!(
                                        "ec2-user@{},{}",
                                        instance.public_dns_name.as_ref().unwrap(),
                                        cores
                                    ));
                                }
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        eprintln!("failed to get instance info: {}", e);
                    }
                }
            }

            ec2_cleanup = Some(scopeguard::guard((), move |_| {
                let mut c = rusoto_ec2::TerminateInstancesRequest::default();
                // clone b/c https://github.com/bluss/scopeguard/issues/15
                c.instance_ids = ec2_instances.clone();
                while let Err(e) = ec2.terminate_instances(&c) {
                    let x = format!("{}", e);
                    if !x.contains("broken pipe") && !x.contains("stream disconnected") {
                        // we're going to get a bunch of "broken pipe" things first
                        // because we've been running for so long.
                        eprintln!("failed to terminate instances: {}; retrying", e);
                    }
                }
            }));
        }
        ("", None) => {
            eprintln!("Must specify manual or ec2 run mode");
            return;
        }
        _ => unreachable!(),
    }

    if server.is_none() {
        eprintln!("no server chosen");
        return;
    }
    let server = server.unwrap();
    let server_host = &server;
    let listen_addr = listen_addr.unwrap();
    let listen_addr = &listen_addr;
    if clients.is_empty() {
        eprintln!("no clients chosen");
        return;
    }

    // what backends are we benchmarking?
    let backends = vec![
        Backend::Netsoup {
            workers: 2,
            readers: 32,
            shards: None,
        },
        Backend::Netsoup {
            workers: 2,
            readers: 32,
            shards: Some(2),
        },
        Backend::Mysql,
        Backend::Mssql,
        Backend::Memcached,
    ];

    // make sure we can connect to the server
    eprintln!("==> connecting to server");
    let server = Ssh::connect(server_host).unwrap();

    let server_has_pl = server.just_exec(&["perflock"]).unwrap().is_ok();

    if let ("ec2", Some(args)) = args.subcommand() {
        let scores = args.value_of("stype")
            .and_then(ec2_instance_type_cores)
            .expect("could not determine server core count");
        eprintln!(" -> adjusting ec2 server ami for {} cores", scores);
        server
            .just_exec(&["sudo", "/opt/mssql/ramdisk.sh"])
            .unwrap()
            .is_ok();

        // TODO: memcached cache size?
        // TODO: mssql setup?
        // TODO: mariadb params?
        let optstr = format!("/OPTIONS=/ s/\"$/ -t {}\"/", scores);
        server
            .just_exec(&["sudo", "sed", "-i", &optstr, "/etc/sysconfig/memcached"])
            .unwrap()
            .is_ok();
    }

    // connect to each of the clients
    eprintln!("==> connecting to clients");
    let clients: Vec<_> = clients
        .iter()
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

            eprintln!(" -> {} with {} cores", client, threads);
            Ssh::connect(client).map(|ssh| {
                let has_perflock = ssh.just_exec(&["perflock"]).unwrap().is_ok();
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

    // if the user wants us to terminate, finish whatever we're currently doing first
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }) {
        eprintln!("==> failed to set ^C handler: {}", e);
    }

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
            read_percentage,
            articles,
        };

        let targets = [
            5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 4_000_000,
            6_000_000, 8_000_000, 12_000_000, 14_000_000,
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
            if !run_clients(&clients, &mut s, target, params) {
                // backend clearly couldn't handle the load, so don't run higher targets
                break;
            }

            if !running.load(Ordering::SeqCst) {
                // user pressed ^C
                break;
            }
        }

        eprintln!(" -> stopping server");
        s.end(&backend).unwrap();
        eprintln!(" .. server stopped ");

        if !running.load(Ordering::SeqCst) {
            // user pressed ^C
            break;
        }
    }

    if !running.load(Ordering::SeqCst) {
        eprintln!("==> terminating early due to ^C");
    }

    if ec2_cleanup.is_some() {
        use std::{io, time};
        eprintln!("==> about to terminate ec2 instances -- press enter to interrupt");
        match timeout_readwrite::TimeoutReader::new(
            io::stdin(),
            Some(time::Duration::from_secs(1 * 60)),
        ).read(&mut [0u8])
        {
            Ok(_) => {
                // user pressed enter to interrupt
                // show all the host addresses, and let them do their thing
                // then wait for an enter to actually terminate
                eprintln!(" -> delaying shutdown, here are the hosts:");
                eprintln!("server: {}", server_host);
                for &(_, ref desc) in &clients {
                    eprintln!("client: {}", desc.name);
                }
                eprintln!(" -> press enter to terminate all instances");
                io::stdin().read(&mut [0u8]).is_ok();
            }
            Err(_) => {
                // doesn't really matter what the error was
                // we should shutdown immediately (which we do by not waiting...)
            }
        }
    }

    drop(ec2_cleanup);
}

// returns true if next target is feasible
fn run_clients(
    clients: &Vec<(Ssh, HostDesc)>,
    server: &mut server::Server,
    target: usize,
    params: ClientParameters,
) -> bool {
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
        cmd.push("env".into());
        cmd.push("RUST_BACKTRACE=1".into());
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
                return false;
            }
            Err(e) => {
                eprintln!("{} failed to populate:", host.name);
                eprintln!("{:?}", e);
                eprintln!("");
                return false;
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
    let mut overloaded = None;
    let mut any_not_overloaded = false;
    use std::fs::File;
    let fname = params.name(target, "log");
    let mut outf = File::create(&fname);
    if let Err(ref e) = outf {
        eprintln!(" !! failed to open output file {}: {}", fname, e);
    }

    eprintln!(" .. waiting for benchmark to complete");
    for (host, mut chan) in workers {
        if let Ok(ref mut f) = outf {
            // TODO: should we get histogram files here instead and merge them?

            let mut stdout = String::new();
            chan.read_to_string(&mut stdout).unwrap();
            f.write_all(stdout.as_bytes()).unwrap();

            let mut is_overloaded = false;
            for line in stdout.lines() {
                if !line.starts_with('#') {
                    let mut fields = line.split_whitespace().skip(1);
                    let pct: u32 = fields.next().unwrap().parse().unwrap();
                    let sjrn: u32 = fields.next().unwrap().parse().unwrap();

                    if pct == 50 && sjrn > 100_000 {
                        is_overloaded = true;
                    }
                }
            }

            if is_overloaded {
                eprintln!(" !! client {} was overloaded", host.name);
            } else {
                any_not_overloaded = true;
            }

            let was_overloaded = overloaded.unwrap_or(false);
            if overloaded.is_some() && is_overloaded != was_overloaded {
                // one client was overloaded, while another wasn't....
                eprintln!(" !! unequal overload detected");
            }

            overloaded = Some(is_overloaded || was_overloaded);
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

    any_not_overloaded
}

fn ec2_instance_type_cores(it: &str) -> Option<u16> {
    it.rsplitn(2, '.').next().and_then(|itype| match itype {
        "nano" | "micro" | "small" => Some(1),
        "medium" | "large" => Some(2),
        t if t.ends_with("xlarge") => {
            let mult = t.trim_right_matches("xlarge").parse::<u16>().unwrap_or(1);
            Some(4 * mult)
        }
        _ => None,
    })
}
