#![deny(unused_extern_crates)]
#![feature(catch_expr)]

#[macro_use]
extern crate clap;
extern crate chrono;
extern crate ctrlc;
#[macro_use]
extern crate failure;
extern crate rayon;
extern crate rusoto_core;
extern crate rusoto_sts;
extern crate shellwords;
extern crate ssh2;
extern crate timeout_readwrite;
extern crate tsunami;

mod backends;
mod server;
use backends::Backend;

use failure::Error;
use rusoto_core::default_tls_client;
use rusoto_core::{EnvironmentProvider, Region};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::borrow::Cow;
use std::io::prelude::*;
use std::{io, thread, time};

const SOUP_SERVER_AMI: &str = "ami-82c57dfd";
const SOUP_CLIENT_AMI: &str = "ami-5172ca2e";

#[derive(Clone, Copy)]
struct ClientParameters<'a> {
    listen_addr: &'a str,
    backend: &'a Backend,
    runtime: usize,
    warmup: usize,
    articles: usize,
    read_percentage: usize,
    skewed: bool,
}

impl<'a> ClientParameters<'a> {
    fn add_params(&'a self, cmd: &mut Vec<Cow<'a, str>>) {
        cmd.push(self.backend.multiclient_name().into());
        cmd.push(self.listen_addr.into());
        cmd.push("--warmup".into());
        cmd.push(format!("{}", self.warmup).into());
        cmd.push("-r".into());
        cmd.push(format!("{}", self.runtime).into());
        cmd.push("-d".into());
        if self.skewed {
            cmd.push("skewed".into());
        } else {
            cmd.push("uniform".into());
        }
        cmd.push("-a".into());
        cmd.push(format!("{}", self.articles).into());
        cmd.push("--write-every".into());
        let write_every = match self.read_percentage {
            0 => 1,
            100 => {
                // very rare
                2_000_000_000
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
            "{}.{}a.{}t.{}r.{}.{}",
            self.backend.uniq_name(),
            self.articles,
            target,
            self.read_percentage,
            if self.skewed { "skewed" } else { "uniform" },
            ext
        )
    }
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote-orchestrator")
        .version("0.1")
        .about("Orchestrate many runs of the vote benchmark")
        .arg(
            Arg::with_name("articles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("2000000")
                .takes_value(true)
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("40")
                .takes_value(true)
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .default_value("20")
                .takes_value(true)
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("read_percentage")
                .short("p")
                .default_value("95")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help("The percentage of operations that are reads"),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .possible_values(&["uniform", "skewed", "both"])
                .default_value("both")
                .takes_value(true)
                .help("How to distribute keys."),
        )
        .arg(
            Arg::with_name("stype")
                .long("server")
                .default_value("c5.4xlarge")
                .required(true)
                .takes_value(true)
                .help("Instance type for server"),
        )
        .arg(
            Arg::with_name("ctype")
                .long("client")
                .default_value("c5.4xlarge")
                .required(true)
                .takes_value(true)
                .help("Instance type for clients"),
        )
        .arg(
            Arg::with_name("clients")
                .long("clients")
                .short("c")
                .default_value("6")
                .required(true)
                .takes_value(true)
                .help("Number of client machines to spawn"),
        )
        .get_matches();

    let runtime = value_t_or_exit!(args, "runtime", usize);
    let warmup = value_t_or_exit!(args, "warmup", usize);
    let articles = value_t_or_exit!(args, "articles", usize);
    let read_percentages: Vec<usize> = args
        .values_of("read_percentage")
        .unwrap()
        .map(|rp| rp.parse().unwrap())
        .collect();
    let skewed = match args.value_of("distribution").unwrap() {
        "uniform" => &[false][..],
        "skewed" => &[true][..],
        "both" => &[false, true][..],
        _ => unreachable!(),
    };
    let nclients = value_t_or_exit!(args, "clients", i64);

    // guess the core counts
    let ccores = args
        .value_of("ctype")
        .and_then(ec2_instance_type_cores)
        .map(|cores| {
            // one core for load generators to be on the safe side
            match cores {
                1 => 1,
                2 => 1,
                n => n - 1,
            }
        })
        .expect("could not determine client core count");
    let scores = args
        .value_of("stype")
        .and_then(ec2_instance_type_cores)
        .expect("could not determine server core count");

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

    let mut b = tsunami::TsunamiBuilder::default();
    b.set_region(Region::UsEast1);
    b.use_term_logger();
    b.add_set(
        "server",
        1,
        tsunami::MachineSetup::new(
            args.value_of("stype").unwrap(),
            SOUP_SERVER_AMI,
            move |host| {
                // ensure we don't have stale soup (yuck)
                host.just_exec(&["git", "-C", "distributary", "pull", "2>&1"])?
                    .is_ok();

                eprintln!(" -> adjusting ec2 server ami for {} cores", scores);
                host.just_exec(&["sudo", "/opt/mssql/ramdisk.sh"])?.is_ok();

                // TODO: memcached cache size?
                // TODO: mssql setup?
                // TODO: mariadb params?
                let optstr = format!("/OPTIONS=/ s/\"$/ -m 4096 -t {}\"/", scores);
                host.just_exec(&["sudo", "sed", "-i", &optstr, "/etc/sysconfig/memcached"])?
                    .is_ok();
                Ok(())
            },
        ),
    );
    b.add_set(
        "client",
        nclients as u32,
        tsunami::MachineSetup::new(args.value_of("ctype").unwrap(), SOUP_CLIENT_AMI, |host| {
            eprintln!(" -> building vote client on client");
            host.just_exec(&["git", "-C", "distributary", "pull", "2>&1"])?
                .is_ok();
            host.just_exec(&[
                "cd",
                "distributary",
                "&&",
                "cargo",
                "b",
                "--release",
                "--manifest-path",
                "benchmarks/Cargo.toml",
                "--bin",
                "vote",
                "2>&1",
            ])?
                .is_ok();
            Ok(())
        }).as_user("ubuntu"),
    );

    // what backends are we benchmarking?
    let backends = vec![
        Backend::Memcached,
        Backend::Mysql,
        Backend::Netsoup {
            workers: 1,
            readers: 80,
            shards: None,
        },
        Backend::Hybrid,
        Backend::Netsoup {
            workers: 4,
            readers: 80,
            shards: Some(4),
        },
        Backend::Mssql,
    ];

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

    rayon::ThreadPoolBuilder::new()
        .num_threads(nclients as usize + 1)
        .build_global()
        .unwrap();

    b.wait_limit(time::Duration::from_secs(5 * 60));
    b.set_max_duration(6);
    b.run_as(provider, |mut hosts| {
        let server = hosts.remove("server").unwrap().swap_remove(0);
        let clients = hosts.remove("client").unwrap();
        let listen_addr = &server.private_ip;
        for backend in backends {
            eprintln!("==> {}", backend.uniq_name());

            eprintln!(" -> starting server");
            let mut s =
                match server::start(server.ssh.as_ref().unwrap(), listen_addr, &backend).unwrap() {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("failed to start {:?}: {:?}", backend, e);
                        continue;
                    }
                };

            // give the server a little time to get its stuff together
            if let Err(e) = s.wait(clients[0].ssh.as_ref().unwrap(), &backend) {
                eprintln!("failed to start {:?}: {:?}", backend, e);
                continue;
            }
            eprintln!(" .. server started ");

            let targets = [
                5_000, 10_000, 50_000, 100_000, 175_000, 250_000, 500_000, 1_000_000, 1_500_000,
                2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 8_000_000, 10_000_000,
                11_000_000, 12_000_000, 13_000_000, 14_000_000, 15_000_000, 16_000_000,
            ];

            let mut first = true;
            'out: for &read_percentage in &read_percentages {
                for &skewed in skewed {
                    let params = ClientParameters {
                        backend: &backend,
                        listen_addr,
                        runtime,
                        warmup,
                        read_percentage,
                        articles,
                        skewed,
                    };

                    let iters = 1;
                    for iter in 0..iters {
                        for &target in &targets {
                            if first {
                                first = false;
                            } else {
                                s = s.between_targets(&backend).unwrap();

                                // wait in case server was restarted
                                if let Err(e) = s.wait(clients[0].ssh.as_ref().unwrap(), &backend) {
                                    eprintln!("failed to restart {:?}: {:?}", backend, e);
                                    continue;
                                }
                            }

                            eprintln!(
                                " -> {} [run {}/{}] @ {}",
                                params.name(target, ""),
                                iter + 1,
                                iters,
                                chrono::Local::now().time()
                            );
                            if !run_clients(iter, &clients, ccores, &mut s, target, params) {
                                // backend clearly couldn't handle the load, so don't run higher targets
                                break;
                            }

                            if !running.load(Ordering::SeqCst) {
                                // user pressed ^C
                                break 'out;
                            }
                        }
                    }
                }
            }

            eprintln!(" -> stopping server");
            s.end(&backend).unwrap();
            eprintln!(" .. server stopped ");

            if !running.load(Ordering::SeqCst) {
                // user pressed ^C
                break;
            }

            thread::sleep(time::Duration::from_secs(5));
        }

        if !running.load(Ordering::SeqCst) {
            eprintln!("==> terminating early due to ^C");
        }

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
                eprintln!("server: {}", server.public_dns);
                for client in &clients {
                    eprintln!("client: {}", client.public_dns);
                }
                eprintln!(" -> press enter to terminate all instances");
                io::stdin().read(&mut [0u8]).is_ok();
            }
            Err(_) => {
                // doesn't really matter what the error was
                // we should shutdown immediately (which we do by not waiting...)
            }
        }

        Ok(())
    }).unwrap();
}

// returns true if next target is feasible
fn run_clients(
    iter: usize,
    clients: &Vec<tsunami::Machine>,
    ccores: u16,
    server: &mut server::Server,
    target: usize,
    params: ClientParameters,
) -> bool {
    // first, we need to prime from some host -- doesn't really matter which
    {
        clients[0].ssh.as_ref().unwrap().set_timeout(0);
        eprintln!(" .. prepopulating on {}", clients[0].public_dns);

        let mut prime_params = params.clone();
        prime_params.warmup = 0;
        prime_params.runtime = 0;
        let mut cmd = Vec::<Cow<str>>::new();
        cmd.push("env".into());
        cmd.push("RUST_BACKTRACE=1".into());
        cmd.push("multiclient.sh".into());
        prime_params.add_params(&mut cmd);
        cmd.push("--threads".into());
        cmd.push(format!("{}", 6 * ccores).into());
        cmd.push("2>&1".into());
        let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();

        match clients[0].ssh.as_ref().unwrap().just_exec(&cmd[..]) {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("{} failed to populate:", clients[0].public_dns);
                eprintln!("{}", e);
                eprintln!("");
                return false;
            }
            Err(e) => {
                eprintln!("{} failed to populate:", clients[0].public_dns);
                eprintln!("{:?}", e);
                eprintln!("");
                return false;
            }
        }

        eprintln!(" .. finished prepopulation");
    }

    let total_threads = ccores as usize * clients.len();
    let ops_per_thread = target as f64 / total_threads as f64;

    // start all the benchmark clients
    let workers: Vec<_> = clients
        .iter()
        .filter_map(
            |&tsunami::Machine {
                 ref ssh,
                 ref public_dns,
                 ..
             }| {
                let ssh = ssh.as_ref().unwrap();

                // so, target ops...
                // target ops is actually not entirely straightforward to determine. here, we'll
                // assume that each client thread is able to process requests at roughly the same
                // rate, so we divide the target ops among machines based on the number of threads
                // they have. this may or may not be the right thing.
                let target = (ops_per_thread * ccores as f64).ceil() as usize;

                eprintln!(
                    " .. starting benchmarker on {} with target {}",
                    public_dns, target
                );

                let c: Result<_, Box<Error>> = do catch {
                    let mut cmd = Vec::<Cow<str>>::new();
                    cmd.push("multiclient.sh".into());
                    params.add_params(&mut cmd);
                    cmd.push("--no-prime".into());
                    cmd.push("--threads".into());
                    cmd.push(format!("{}", 6 * ccores).into());

                    // so, target ops...
                    // target ops is actually not entirely straightforward to determine. here, we'll
                    // assume that each client thread is able to process requests at roughly the same
                    // rate, so we divide the target ops among machines based on the number of threads
                    // they have. this may or may not be the right thing.
                    cmd.push("--target".into());
                    cmd.push(format!("{}", target).into());

                    let cmd: Vec<_> = cmd.iter().map(|s| &**s).collect();
                    let c = ssh.exec(&cmd[..])?;
                    c
                };

                match c {
                    Ok(c) => Some((public_dns, c)),
                    Err(e) => {
                        eprintln!("{} failed to run benchmark client:", public_dns);
                        eprintln!("{:?}", e);
                        eprintln!("");
                        None
                    }
                }
            },
        )
        .collect();

    // let's see how we did
    let mut overloaded = None;
    let mut any_not_overloaded = false;
    let fname = params.name(target, "log");
    let mut outf = if iter == 0 {
        use std::fs::File;
        File::create(&fname)
    } else {
        use std::fs::OpenOptions;
        OpenOptions::new()
            .truncate(false)
            .append(true)
            .create(true)
            .open(&fname)
    };
    if let Err(ref e) = outf {
        eprintln!(" !! failed to open output file {}: {}", fname, e);
    }

    eprintln!(" .. waiting for benchmark to complete");
    for (host, mut chan) in workers {
        let mut got_lines = true;
        if let Ok(ref mut f) = outf {
            // TODO: should we get histogram files here instead and merge them?

            let mut stdout = String::new();
            chan.read_to_string(&mut stdout).unwrap();
            f.write_all(stdout.as_bytes()).unwrap();

            got_lines = false;
            let mut is_overloaded = false;
            for line in stdout.lines() {
                if !line.starts_with('#') {
                    let mut fields = line.split_whitespace().skip(1);
                    let pct: u32 = fields.next().unwrap().parse().unwrap();
                    let sjrn: u32 = fields.next().unwrap().parse().unwrap();
                    got_lines = true;

                    if pct == 50 && sjrn > 100_000 {
                        is_overloaded = true;
                    }
                }
            }

            if is_overloaded {
                eprintln!(" !! client {} was overloaded", host);
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

        if !got_lines || chan.exit_status().unwrap() != 0 {
            eprintln!("{} failed to run benchmark client:", host);
            eprintln!("{}", stderr);
            eprintln!("");
            any_not_overloaded = false;
        } else if !stderr.is_empty() {
            eprintln!("{} reported:", host);
            let stderr = stderr.trim_right().replace('\n', "\n > ");
            eprintln!(" > {}", stderr);
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

impl ConvenientSession for tsunami::Session {
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error> {
        let cmd: Vec<_> = cmd
            .iter()
            .map(|&arg| match arg {
                "&&" | "<" | ">" | "2>" | "2>&1" | "|" => arg.to_string(),
                _ => shellwords::escape(arg),
            })
            .collect();
        let cmd = cmd.join(" ");
        eprintln!("    :> {}", cmd);

        // ensure we're using a Bourne shell (that's what shellwords supports too)
        let cmd = format!("bash -c {}", shellwords::escape(&cmd));

        let mut c = self.channel_session()?;
        c.exec(&cmd)?;
        Ok(c)
    }
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<(), String>, Error> {
        let mut c = self.exec(cmd)?;

        let mut stderr = String::new();
        c.stderr().read_to_string(&mut stderr)?;
        c.wait_eof()?;

        if c.exit_status()? != 0 {
            return Ok(Err(stderr));
        }
        Ok(Ok(()))
    }

    fn in_distributary(&self, cmd: &[&str]) -> Result<Result<(), String>, Error> {
        let mut args = vec!["cd", "distributary", "&&"];
        args.extend(cmd);
        self.just_exec(&args[..])
    }
}

trait ConvenientSession {
    fn exec<'a>(&'a self, cmd: &[&str]) -> Result<ssh2::Channel<'a>, Error>;
    fn just_exec(&self, cmd: &[&str]) -> Result<Result<(), String>, Error>;
    fn in_distributary(&self, cmd: &[&str]) -> Result<Result<(), String>, Error>;
}
