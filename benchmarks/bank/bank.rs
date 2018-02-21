#![deny(unused_extern_crates)]

#[macro_use]
extern crate clap;
extern crate distributary;
extern crate mio;
extern crate rand;

use std::thread;
use std::time;

use std::collections::HashMap;

use mio::net::TcpListener;

use distributary::{ControllerBuilder, ControllerHandle, DataType, LocalAuthority, Token};

use rand::Rng;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "\
EXAMPLES:
  bank --avg";

pub struct Bank {
    blender: ControllerHandle<LocalAuthority>,
    transfers: distributary::NodeIndex,
    balances: distributary::NodeIndex,
    debug_channel: Option<TcpListener>,
}

pub fn setup() -> Box<Bank> {
    let log = distributary::logger_pls();

    // set up graph
    let mut b = ControllerBuilder::default();
    b.log_with(log);
    b.set_worker_threads(2);
    b.set_read_threads(1);

    //let debug_channel = g.create_debug_channel();

    let mut g = b.build_local();

    let recipe = "CREATE TABLE transfers (src_acct int, dst_acct int, amount int, PRIMARY KEY(src_acct));

                  debits: SELECT src_acct AS acct_id, SUM(amount) AS total \
                          FROM transfers GROUP BY src_acct;
                  credits: SELECT dst_acct AS acct_id, SUM(amount) AS total \
                           FROM transfers GROUP BY dst_acct;

                  QUERY balances: SELECT debits.acct_id, credits.total AS credit, debits.total AS debit \
                                  FROM credits JOIN debits ON (credits.acct_id = debits.acct_id);";

    g.install_recipe(recipe.to_owned()).unwrap();

    let inputs = g.inputs();
    let outputs = g.outputs();

    Box::new(Bank {
        blender: g,
        transfers: inputs["transfers"],
        balances: outputs["balances"],
        debug_channel: None, // Some(debug_channel),
    })
}

impl Bank {
    fn getter(&mut self) -> distributary::RemoteGetter {
        self.blender.get_getter(self.balances).unwrap()
    }
    pub fn migrate(&mut self) {
        // XXX(malte): re-implement this
        unimplemented!();
        /*let balances = self.balances;
        self.blender.migrate(|mig| {
            let identity = mig.add_ingredient(
                "identity",
                &["acct_id", "credit", "debit"],
                distributary::Identity::new(balances),
            );
            mig.maintain(identity, 0);
        });*/
    }
}

fn populate(naccounts: i64, mut mutator: distributary::Mutator, transactions: bool) {
    // prepopulate non-transactionally (this is okay because we add no accounts while running the
    // benchmark)
    println!("Connected. Setting up {} accounts.", naccounts);
    {
        for i in 0..naccounts {
            mutator.put(vec![0.into(), i.into(), 1000.into()]).unwrap();
            mutator.put(vec![i.into(), 0.into(), 1.into()]).unwrap();
        }

        if !transactions {
            // Insert a bunch of empty transfers to make sure any buffers are flushed
            for _ in 0..1024 {
                mutator.put(vec![0.into(), 0.into(), 0.into()]).unwrap();
            }
            thread::sleep(time::Duration::new(0, 50000000));
        }
    }

    println!("Done with account creation");
}

fn client(
    _i: usize,
    mut mutator: distributary::Mutator,
    mut balances_get: distributary::RemoteGetter,
    naccounts: i64,
    start: time::Instant,
    runtime: time::Duration,
    _verbose: bool,
    audit: bool,
    measure_latency: Option<TcpListener>,
    coarse: bool,
    transactions: bool,
    is_transfer_deterministic: bool,
) -> Vec<f64> {
    let mut count = 0u64;
    let mut committed = 0u64;
    let mut aborted = 0u64;
    let mut last_reported = start;
    let mut throughputs = Vec::new();
    let mut event_times = Vec::new();

    let mut t_rng = rand::thread_rng();

    let mut successful_transfers = Vec::new();

    {
        let f = |(res, token): (distributary::Datas, _)| {
            assert_eq!(res.len(), 1);
            res.into_iter().next().map(|row| {
                // we only care about the first result
                let mut row = row.into_iter();
                let _: i64 = row.next().unwrap().into();
                let credit: i64 = row.next().unwrap().into();
                let debit: i64 = row.next().unwrap().into();
                (credit - debit, token)
            })
        };

        //let mut get = |id: &DataType| if balances_get.supports_transactions() {
        let mut get = |id: &DataType| {
            if transactions {
                balances_get.transactional_lookup(id).map(&f)
            } else {
                balances_get
                    .lookup(id, true)
                    .map(|rs| f((rs, Token::empty())))
            }
        };

        let mut num_requests = 1;
        while start.elapsed() < runtime {
            let dst;
            let src;
            if is_transfer_deterministic {
                dst = num_requests % (naccounts - 2) + 2;
                src = dst - 1;
                num_requests += 1;
            } else {
                dst = t_rng.gen_range(1, naccounts);
                src = (dst - 1 + t_rng.gen_range(1, naccounts - 1)) % (naccounts - 1) + 1;
            }
            assert_ne!(dst, src);

            let transaction_start = time::Instant::now();
            let (balance, mut token) = get(&src.into()).unwrap().unwrap();

            assert!(
                balance >= 0 || !transactions,
                format!("{} balance is {}", src, balance)
            );

            if balance >= 100 {
                if coarse {
                    token.make_coarse();
                }

                if measure_latency.is_some() {
                    mutator.start_tracing(count);
                };

                let write_start = time::Instant::now();
                let res = if transactions {
                    mutator
                        .transactional_put(vec![src.into(), dst.into(), 100.into()], token.into())
                } else {
                    mutator
                        .put(vec![src.into(), dst.into(), 100.into()])
                        .unwrap();
                    Ok(0)
                };
                let write_end = time::Instant::now();
                mutator.stop_tracing();

                match res {
                    Ok(_) => {
                        if audit {
                            successful_transfers.push((src, dst, 100));
                        }
                        // Skip the first sample since it is frequently an outlier
                        if measure_latency.is_some() {
                            event_times.push(Some((transaction_start, write_start, write_end)));
                            thread::sleep(time::Duration::new(0, 1_000_000_000));
                        }
                        committed += 1;
                    }
                    Err(_) => {
                        if measure_latency.is_some() {
                            event_times.push(None);
                        }
                        aborted += 1;
                    }
                }

                count += 1;
            }

            // check if we should report
            if measure_latency.is_none() && last_reported.elapsed() > time::Duration::from_secs(1) {
                let ts = last_reported.elapsed();
                let throughput = committed as f64
                    / (ts.as_secs() as f64 + ts.subsec_nanos() as f64 / 1_000_000_000f64);
                let commit_rate = committed as f64 / count as f64;
                let abort_rate = aborted as f64 / count as f64;
                println!(
                    "{:?} PUT: {:.2} {:.2} {:.2}",
                    dur_to_ns!(start.elapsed()),
                    throughput,
                    commit_rate,
                    abort_rate
                );
                throughputs.push(throughput);

                last_reported = time::Instant::now();
                count = 0;
                committed = 0;
                aborted = 0;
            }
        }

        if audit {
            let mut target_balances = HashMap::new();
            for i in 0..naccounts {
                target_balances.insert(i as i64, 0);
            }
            for i in 0i64..(naccounts as i64) {
                *target_balances.get_mut(&0).unwrap() -= 999;
                *target_balances.get_mut(&i).unwrap() += 999;
            }

            for (src, dst, amt) in successful_transfers {
                *target_balances.get_mut(&src).unwrap() -= amt;
                *target_balances.get_mut(&dst).unwrap() += amt;
                assert!(*target_balances.get(&src).unwrap() >= 0);
            }

            for (account, balance) in target_balances {
                assert_eq!(get(&account.into()).unwrap().unwrap().0, balance);
            }
            println!("Audit found no irregularities");
        }
    }
    if let Some(debug_channel) = measure_latency {
        process_latencies(event_times, debug_channel);
    }

    throughputs
}

/// Given a Vec of (transaction_start, write_start) and the global debug channel, compute and output
/// latency statistics.
fn process_latencies(
    _times: Vec<Option<(time::Instant, time::Instant, time::Instant)>>,
    _debug_channel: TcpListener,
) {
    unimplemented!();

    // use distributary::{DebugEvent, DebugEventType, PacketEvent};

    // let mut read_latencies: Vec<u64> = Vec::new();
    // let mut write_latencies = Vec::new();
    // let mut settle_latencies = Vec::new();
    // for _ in 0..(times.iter().filter(|t| t.is_some()).count()) {
    //     for DebugEvent { instant, event } in debug_channel.iter() {
    //         match event {
    //             DebugEventType::PacketEvent(PacketEvent::ReachedReader, tag) => {
    //                 if let Some((transaction_start, write_start, write_end))
    //                     = times[tag as usize] {
    //                     read_latencies.push(dur_to_ns!(write_start - transaction_start));
    //                     write_latencies.push(dur_to_ns!(write_end - write_start));
    //                     settle_latencies.push(
    //                           dur_to_ns!(cmp::max(instant, write_end) - write_end));
    //                 }
    //                 break;
    //             }
    //             DebugEventType::PacketEvent(PacketEvent::Merged(_), _) => unimplemented!(),
    //             _ => {}
    //         }
    //     }
    // }

    // // Print average latencies.
    // let rl: u64 = read_latencies.iter().sum();
    // let wl: u64 = write_latencies.iter().sum();
    // let sl: u64 = settle_latencies.iter().sum();

    // let n = write_latencies.len() as f64;
    // println!("read latency: {:.3} μs", rl as f64 / n * 0.001);
    // println!("write latency: {:.3} μs", wl as f64 / n * 0.001);
    // println!("settle latency: {:.3} μs", sl as f64 / n * 0.001);
    // println!(
    //     "write + settle latency: {:.3} μs",
    //     (wl + sl) as f64 / n * 0.001
    // );

    // let mut latencies_hist = Histogram::<u64>::new_with_bounds(10, 10000000, 4).unwrap();
    // for i in 0..write_latencies.len() {
    //     let sample_nanos = write_latencies[i] + settle_latencies[i];
    //     let sample_micros = (sample_nanos as f64 * 0.001).round() as u64;
    //     latencies_hist.record(sample_micros).unwrap();
    // }

    // for iv in latencies_hist.iter_recorded() {
    //     // XXX: Print CDF in the format expected by the print_latency_cdf script.
    //     println!("percentile PUT {:.2} {:.2}", iv.value(), iv.percentile());
    // }
}

fn main() {
    use clap::{App, Arg};
    let args = App::new("bank")
        .version("0.1")
        .about("Benchmarks Soup transactions and reports abort rate.")
        .arg(
            Arg::with_name("avg")
                .long("avg")
                .takes_value(false)
                .help("compute average throughput at the end of benchmark"),
        )
        .arg(
            Arg::with_name("naccounts")
                .short("a")
                .long("accounts")
                .value_name("N")
                .default_value("5")
                .help("Number of bank accounts to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .value_name("N")
                .default_value("60")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("migrate")
                .short("m")
                .long("migrate")
                .value_name("M")
                .help("Perform a migration after this many seconds")
                .conflicts_with("stage"),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .value_name("T")
                .default_value("2")
                .help("Number of client threads"),
        )
        .arg(
            Arg::with_name("latency")
                .short("l")
                .long("latency")
                .takes_value(false)
                .help("Measure latency of requests"),
        )
        .arg(
            Arg::with_name("coarse")
                .short("c")
                .long("coarse")
                .takes_value(false)
                .help("Use only coarse grained checktables"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .help("Verbose (debugging) output"),
        )
        .arg(
            Arg::with_name("audit")
                .short("A")
                .long("audit")
                .takes_value(false)
                .help("Audit results after benchmark completes"),
        )
        .arg(
            Arg::with_name("nontransactional")
                .long("nontransactional")
                .takes_value(false)
                .help("Use non-transactional writes"),
        )
        .arg(
            Arg::with_name("deterministic")
                .long("deterministic")
                .takes_value(false)
                .help("Use deterministic money transfers"),
        )
        .after_help(BENCH_USAGE)
        .get_matches();

    let avg = args.is_present("avg");
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let naccounts = value_t_or_exit!(args, "naccounts", i64) + 1;
    let nthreads = value_t_or_exit!(args, "threads", usize);
    let verbose = args.is_present("verbose");
    let audit = args.is_present("audit");
    let measure_latency = args.is_present("latency");
    let coarse_checktables = args.is_present("coarse");
    let transactions = !args.is_present("nontransactional");
    let is_transfer_deterministic = args.is_present("deterministic");

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }
    // setup db
    println!("Attempting to set up bank");
    let mut bank = setup();

    {
        let transfer_nid = bank.transfers;
        let mutator = bank.blender.get_mutator(transfer_nid).unwrap();
        populate(naccounts, mutator, transactions);
    }

    // let system settle
    thread::sleep(time::Duration::from_millis(100));
    let start = time::Instant::now();

    // benchmark
    let clients = (0..nthreads)
        .into_iter()
        .map(|i| {
            Some({
                let transfers_nid = bank.transfers;
                let mutator = bank.blender.get_mutator(transfers_nid).unwrap();
                let balances_get = bank.getter();

                thread::Builder::new()
                    .name(format!("bank{}", i))
                    .spawn(move || -> Vec<f64> {
                        client(
                            i,
                            mutator,
                            balances_get,
                            naccounts,
                            start,
                            runtime,
                            verbose,
                            audit,
                            None, /* measure_latency */
                            coarse_checktables,
                            transactions,
                            is_transfer_deterministic,
                        )
                    })
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();

    let latency_client = if measure_latency {
        Some({
            let transfers_nid = bank.transfers;
            let mutator = bank.blender.get_mutator(transfers_nid).unwrap();
            let balances_get = bank.getter();
            let debug_channel = bank.debug_channel.take();
            assert!(debug_channel.is_some());

            thread::Builder::new()
                .name(format!("bank{}", nthreads))
                .spawn(move || -> Vec<f64> {
                    client(
                        nthreads,
                        mutator,
                        balances_get,
                        naccounts,
                        start,
                        runtime,
                        verbose,
                        audit,
                        debug_channel,
                        coarse_checktables,
                        transactions,
                        is_transfer_deterministic,
                    )
                })
                .unwrap()
        })
    } else {
        None
    };

    if let Some(duration) = migrate_after {
        thread::sleep(duration);
        println!("----- starting migration -----");
        let start = time::Instant::now();
        bank.migrate();
        let duration = start.elapsed();
        let length = 1000000000u64 * duration.as_secs() + duration.subsec_nanos() as u64;
        println!(
            "----- completed migration -----\nElapsed time = {} ms",
            1e-6 * (length as f64)
        );
    }

    if let Some(c) = latency_client {
        c.join().unwrap();
    }

    // clean
    let mut throughput = 0.0;
    for c in clients {
        if let Some(client) = c {
            match client.join() {
                Err(e) => panic!(e),
                Ok(th) => {
                    let sum: f64 = th.iter().sum();
                    throughput += sum / (th.len() as f64);
                }
            }
        }
    }

    if avg {
        println!("avg PUT: {:.2}", throughput);
    }
}
