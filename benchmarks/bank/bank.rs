#[macro_use]
extern crate clap;

extern crate rand;

extern crate distributary;

extern crate hdrsample;

extern crate timekeeper;

use hdrsample::Histogram;

use std::thread;
use std::time;

use std::collections::HashMap;

use distributary::{Blender, Base, BaseDurabilityLevel, Aggregation, Join, JoinType, Datas,
                   DataType, Token, Mutator};

use rand::Rng;

use timekeeper::{Source, RealTime};

#[allow(dead_code)]
type TxGet = Box<Fn(&DataType) -> Result<(Datas, Token), ()> + Send>;

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
    blender: Blender,
    transfers: distributary::NodeAddress,
    balances: distributary::NodeAddress,
    transactions: bool,
}

pub fn setup(transactions: bool, durability_level: Option<BaseDurabilityLevel>) -> Box<Bank> {
    // set up graph
    let mut g = Blender::new();

    let transfers;
    let credits;
    let debits;
    let balances;
    {
        // migrate
        let mut mig = g.start_migration();

        // add transfers base table
        let base = if durability_level.is_none() {
            Base::default()
        } else {
            Base::default().with_durability(durability_level.unwrap())
        };

        transfers = if transactions {
            mig.add_transactional_base("transfers", &["src_acct", "dst_acct", "amount"], base)
        } else {
            mig.add_ingredient("transfers", &["src_acct", "dst_acct", "amount"], base)
        };

        // add all debits
        debits = mig.add_ingredient("debits",
                                    &["acct_id", "total"],
                                    Aggregation::SUM.over(transfers, 2, &[0]));

        // add all credits
        credits = mig.add_ingredient("credits",
                                     &["acct_id", "total"],
                                     Aggregation::SUM.over(transfers, 2, &[1]));

        // add join of credits and debits; this is a hack as we don't currently have multi-parent
        // aggregations or arithmetic on columns.
        use distributary::JoinSource::*;
        let j2 = Join::new(credits, debits, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        balances = mig.add_ingredient("balances", &["acct_id", "credit", "debit"], j2);
        mig.maintain(balances, 0);

        let d = mig.add_domain();
        mig.assign_domain(transfers, d);
        mig.assign_domain(credits, d);
        mig.assign_domain(debits, d);
        mig.assign_domain(balances, d);

        // start processing
        mig.commit();
    };

    Box::new(Bank {
                 blender: g,
                 transfers: transfers,
                 balances: balances,
                 transactions: transactions,
             })
}

impl Bank {
    fn getter(&mut self) -> Box<Getter> {
        if self.transactions {
            Box::new(self.blender
                         .get_transactional_getter(self.balances)
                         .unwrap())
        } else {
            let g = self.blender.get_getter(self.balances).unwrap();
            let b = Box::new(move |d: &DataType| g(d, true).map(|r| (r, Token::empty()))) as Box<_>;
            Box::new(b)
        }
    }
    pub fn migrate(&mut self) {
        let mut mig = self.blender.start_migration();
        let identity =
            mig.add_ingredient("identity",
                               &["acct_id", "credit", "debit"],
                               distributary::Identity::new(self.balances));
        mig.maintain(identity, 0);
        mig.commit();
    }
}

pub trait Getter: Send {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Result<Option<(i64, Token)>, ()> + 'a>;
}

impl Getter for TxGet {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Result<Option<(i64, Token)>, ()> + 'a> {
        Box::new(move |id| {
            self(&id.into()).map(|(res, token)| {
                assert_eq!(res.len(), 1);
                res.into_iter()
                    .next()
                    .map(|row| {
                             // we only care about the first result
                             let mut row = row.into_iter();
                             let _: i64 = row.next().unwrap().into();
                             let credit: i64 = row.next().unwrap().into();
                             let debit: i64 = row.next().unwrap().into();
                             (credit - debit, token)
                         })
            })
        })
    }
}

fn populate(naccounts: i64, mutator: Mutator, transactions: bool) {
    // prepopulate non-transactionally (this is okay because we add no accounts while running the
    // benchmark)
    println!("Connected. Setting up {} accounts.", naccounts);
    {
        for i in 0..naccounts {
            mutator.put(vec![0.into(), i.into(), 1000.into()]);
            mutator.put(vec![i.into(), 0.into(), 1.into()]);
        }

        if !transactions {
            // Insert a bunch of empty transfers to make sure any buffers are flushed
            for _ in 0..1024 {
                mutator.put(vec![0.into(), 0.into(), 0.into()]);
            }
            thread::sleep(time::Duration::new(0, 50000000));
        }
    }

    println!("Done with account creation");
}

fn client(_i: usize,
          mut mutator: Mutator,
          balances_get: Box<Getter>,
          naccounts: i64,
          start: time::Instant,
          runtime: time::Duration,
          verbose: bool,
          audit: bool,
          measure_latency: bool,
          coarse: bool,
          transactions: bool,
          is_transfer_deterministic: bool)
          -> Vec<f64> {
    let clock = RealTime::default();

    let mut count = 0;
    let mut committed = 0;
    let mut aborted = 0;
    let mut last_reported = start;
    let mut throughputs = Vec::new();
    let mut read_latencies: Vec<u64> = Vec::new();
    let mut write_latencies = Vec::new();
    let mut settle_latencies = Vec::new();

    let mut t_rng = rand::thread_rng();

    let mut successful_transfers = Vec::new();

    {
        let mut get = balances_get.get();

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

            let transaction_start = clock.get_time();
            let (balance, mut token) = get(src).unwrap().unwrap();

            assert!(balance >= 0 || !transactions,
                    format!("{} balance is {}", src, balance));

            if balance >= 100 {
                if coarse {
                    token.make_coarse();
                }

                let tracer = if measure_latency {
                    Some(mutator.start_tracing())
                } else {
                    None
                };

                let mut last_instant = time::Instant::now();
                let write_start = clock.get_time();
                let res = if transactions {
                    mutator
                        .transactional_put(vec![src.into(), dst.into(), 100.into()], token.into())
                } else {
                    mutator.put(vec![src.into(), dst.into(), 100.into()]);
                    Ok(0)
                };
                let write_end = clock.get_time();
                mutator.stop_tracing();

                match res {
                    Ok(_) => {
                        if audit {
                            successful_transfers.push((src, dst, 100));
                        }
                        // Skip the first sample since it is frequently an outlier
                        let mut transaction_end = None;
                        if measure_latency && count > 0 {
                            for (instant, event) in tracer.unwrap() {
                                if verbose {
                                    let dt = dur_to_ns!(instant.duration_since(last_instant)) as
                                             f64;
                                    println!("{:.3} μs: {:?}", dt * 0.001, event);
                                    last_instant = instant;
                                }
                                if let distributary::PacketEvent::ReachedReader = event {
                                    transaction_end = Some(clock.get_time());
                                }
                            }

                            read_latencies.push(write_start - transaction_start);
                            write_latencies.push(write_end - write_start);
                            settle_latencies.push(transaction_end.unwrap() - write_end);
                        }
                        if measure_latency {
                            thread::sleep(time::Duration::new(0, 1_000_000_000));
                        }
                        committed += 1;
                    }
                    Err(_) => aborted += 1,
                }

                count += 1;
            }

            // check if we should report
            if !measure_latency && last_reported.elapsed() > time::Duration::from_secs(1) {
                let ts = last_reported.elapsed();
                let throughput = committed as f64 /
                                 (ts.as_secs() as f64 +
                                  ts.subsec_nanos() as f64 / 1_000_000_000f64);
                let commit_rate = committed as f64 / count as f64;
                let abort_rate = aborted as f64 / count as f64;
                println!("{:?} PUT: {:.2} {:.2} {:.2}",
                         dur_to_ns!(start.elapsed()),
                         throughput,
                         commit_rate,
                         abort_rate);
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
                assert_eq!(get(account).unwrap().unwrap().0, balance);
            }
            println!("Audit found no irregularities");
        }
    }

    if measure_latency {
        // Print average latencies.
        let rl: u64 = read_latencies.iter().sum();
        let wl: u64 = write_latencies.iter().sum();
        let sl: u64 = settle_latencies.iter().sum();

        let n = write_latencies.len() as f64;
        println!("read latency: {:.3} μs", rl as f64 / n * 0.001);
        println!("write latency: {:.3} μs", wl as f64 / n * 0.001);
        println!("settle latency: {:.3} μs", sl as f64 / n * 0.001);
        println!("write + settle latency: {:.3} μs",
                 (wl + sl) as f64 / n * 0.001);

        let mut latencies_hist = Histogram::<u64>::new_with_bounds(10, 10000000, 4).unwrap();
        for i in 0..write_latencies.len() {
            let sample_nanos = write_latencies[i] + settle_latencies[i];
            let sample_micros = (sample_nanos as f64 * 0.001).round() as u64;
            latencies_hist.record(sample_micros).unwrap();
        }

        for iv in latencies_hist.iter_recorded() {
            // XXX: Print CDF in the format expected by the print_latency_cdf script.
            println!("percentile PUT {:.2} {:.2}", iv.value(), iv.percentile());
        }
    }
    throughputs
}

fn main() {
    use clap::{Arg, App};
    let args = App::new("bank")
        .version("0.1")
        .about("Benchmarks Soup transactions and reports abort rate.")
        .arg(Arg::with_name("avg")
                 .long("avg")
                 .takes_value(false)
                 .help("compute average throughput at the end of benchmark"))
        .arg(Arg::with_name("naccounts")
                 .short("a")
                 .long("accounts")
                 .value_name("N")
                 .default_value("5")
                 .help("Number of bank accounts to prepopulate the database with"))
        .arg(Arg::with_name("runtime")
                 .short("r")
                 .long("runtime")
                 .value_name("N")
                 .default_value("60")
                 .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("migrate")
                 .short("m")
                 .long("migrate")
                 .value_name("M")
                 .help("Perform a migration after this many seconds")
                 .conflicts_with("stage"))
        .arg(Arg::with_name("threads")
                 .short("t")
                 .long("threads")
                 .value_name("T")
                 .default_value("2")
                 .help("Number of client threads"))
        .arg(Arg::with_name("latency")
                 .short("l")
                 .long("latency")
                 .takes_value(false)
                 .help("Measure latency of requests"))
        .arg(Arg::with_name("coarse")
                 .short("c")
                 .long("coarse")
                 .takes_value(false)
                 .help("Use only coarse grained checktables"))
        .arg(Arg::with_name("verbose")
                 .short("v")
                 .long("verbose")
                 .takes_value(false)
                 .help("Verbose (debugging) output"))
        .arg(Arg::with_name("audit")
                 .short("A")
                 .long("audit")
                 .takes_value(false)
                 .help("Audit results after benchmark completes"))
        .arg(Arg::with_name("nontransactional")
                 .long("nontransactional")
                 .takes_value(false)
                 .help("Use non-transactional writes"))
        .arg(Arg::with_name("durability")
                 .long("durability")
                 .takes_value(true)
                 .possible_values(&["buffered", "immediate"])
                 .help("Durability level used for Base nodes"))
        .arg(Arg::with_name("deterministic")
                 .long("deterministic")
                 .takes_value(false)
                 .help("Use deterministic money transfers"))
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

    let durability_level;
    match args.value_of("durability") {
        Some("buffered") => durability_level = Some(BaseDurabilityLevel::Buffered),
        Some("immediate") => durability_level = Some(BaseDurabilityLevel::SyncImmediately),
        None | Some(&_) => durability_level = None,
    }

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }
    // setup db
    println!("Attempting to set up bank");
    let mut bank = if measure_latency {
        setup(transactions, durability_level)
    } else {
        setup(transactions, durability_level)
    };


    {
        let mutator = bank.blender.get_mutator(bank.transfers);
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
                     let mutator = bank.blender.get_mutator(bank.transfers);
                     let balances_get: Box<Getter> = bank.getter();

                     thread::Builder::new()
                         .name(format!("bank{}", i))
                         .spawn(move || -> Vec<f64> {
                    client(i,
                           mutator,
                           balances_get,
                           naccounts,
                           start,
                           runtime,
                           verbose,
                           audit,
                           false, /* measure_latency */
                           coarse_checktables,
                           transactions,
                           is_transfer_deterministic)
                })
                         .unwrap()
                 })
        })
        .collect::<Vec<_>>();

    let latency_client = if measure_latency {
        Some({
                 let mutator = bank.blender.get_mutator(bank.transfers);
                 let balances_get: Box<Getter> = bank.getter();

                 thread::Builder::new()
                     .name(format!("bank{}", nthreads))
                     .spawn(move || -> Vec<f64> {
                client(nthreads,
                       mutator,
                       balances_get,
                       naccounts,
                       start,
                       runtime,
                       verbose,
                       audit,
                       true, /* measure_latency */
                       coarse_checktables,
                       transactions,
                       is_transfer_deterministic)
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
        println!("----- completed migration -----\nElapsed time = {} ms",
                 1e-6 * (length as f64));
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

    if let Some(c) = latency_client {
        c.join().unwrap();
    }
}
