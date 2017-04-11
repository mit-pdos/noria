#[macro_use]
extern crate clap;

extern crate rand;

extern crate distributary;

extern crate timekeeper;

use std::thread;
use std::time;

use std::collections::HashMap;

use distributary::{Blender, Base, Aggregation, Join, JoinType, Datas, DataType, Token, Mutator};

use timekeeper::{Source, RealTime};

use rand::Rng;

type TxPut = Box<Fn(Vec<DataType>, Token) -> Result<i64, ()> + Send + 'static>;
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
    transfers: Vec<Mutator>,
    balances: distributary::NodeAddress,
}

pub fn setup(num_putters: usize) -> Box<Bank> {
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
        transfers = mig.add_ingredient("transfers",
                                       &["src_acct", "dst_acct", "amount"],
                                       Base::default());

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
        mig.transactional_maintain(balances, 0);

        let d = mig.add_domain();
        mig.assign_domain(transfers, d);
        mig.assign_domain(credits, d);
        mig.assign_domain(debits, d);
        mig.assign_domain(balances, d);

        // start processing
        mig.commit();
    };

    let transfers = (0..num_putters).into_iter().map(|_| g.get_mutator(transfers)).collect();
    Box::new(Bank {
                 blender: g,
                 transfers: transfers,
                 balances: balances,
             })
}

impl Bank {
    fn getter(&mut self) -> Box<Getter> {
        Box::new(self.blender.get_transactional_getter(self.balances).unwrap())
    }
    fn putter(&mut self) -> Box<Putter> {
        let m = self.transfers.pop().unwrap();
        let p: TxPut = Box::new(move |u: Vec<DataType>, t: Token| m.transactional_put(u, t));

        Box::new(p)
    }
    pub fn migrate(&mut self) {
        let mut mig = self.blender.start_migration();
        let identity =
            mig.add_ingredient("identity",
                               &["acct_id", "credit", "debit"],
                               distributary::Identity::new(self.balances));
        let _ = mig.transactional_maintain(identity, 0);
        let _ = mig.commit();
    }
}

pub trait Putter: Send {
    fn transfer<'a>(&'a mut self) -> Box<FnMut(i64, i64, i64, Token) -> Result<i64, ()> + 'a>;
}

impl Putter for TxPut {
    fn transfer<'a>(&'a mut self) -> Box<FnMut(i64, i64, i64, Token) -> Result<i64, ()> + 'a> {
        Box::new(move |src, dst, amount, token| {
                     self(vec![src.into(), dst.into(), amount.into()], token.into())
                 })
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
                res.into_iter().next().map(|row| {
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

fn populate(naccounts: i64, transfers_put: &mut Box<Putter>) {
    // prepopulate non-transactionally (this is okay because we add no accounts while running the
    // benchmark)
    println!("Connected. Setting up {} accounts.", naccounts);
    {
        // let accounts_put = bank.accounts.as_ref().unwrap();
        let mut money_put = transfers_put.transfer();
        for i in 0..naccounts {
            // accounts_put(vec![DataType::Number(i as i64), format!("user {}", i).into()]);
            money_put(0, i, 1000, Token::empty()).unwrap();
            money_put(i, 0, 1, Token::empty()).unwrap();
        }
    }
    println!("Done with account creation");
}

fn client(i: usize,
          mut transfers_put: Box<Putter>,
          balances_get: Box<Getter>,
          naccounts: i64,
          start: time::Instant,
          runtime: time::Duration,
          verbose: bool,
          audit: bool,
          measure_latency: bool,
          coarse: bool,
          transactions: &mut Vec<(i64, i64, i64)>)
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

    {
        let mut get = balances_get.get();
        let mut put = transfers_put.transfer();

        while start.elapsed() < runtime {
            let dst = t_rng.gen_range(1, naccounts);
            let src = (dst - 1 + t_rng.gen_range(1, naccounts - 1)) % (naccounts - 1) + 1;
            assert_ne!(dst, src);

            let transaction_start = clock.get_time();
            let (balance, mut token) = get(src).unwrap().unwrap();
            if verbose {
                println!("t{} read {}: {} @ {:#?} (for {})",
                         i,
                         src,
                         balance,
                         token,
                         dst);
            }

            assert!(balance >= 0, format!("{} balance is {}", src, balance));

            if balance >= 100 {
                if verbose {
                    println!("trying {} -> {} of {}", src, dst, 100);
                }

                if coarse {
                    token.make_coarse();
                }

                let write_start = clock.get_time();
                let res = put(src, dst, 100, token);
                let write_end = clock.get_time();

                match res {
                    Ok(ts) => {
                        if verbose {
                            println!("commit @ {}", ts);
                        }
                        if audit {
                            transactions.push((src, dst, 100));
                        }
                        if measure_latency {
                            let mut token = get(src).unwrap().unwrap().1;
                            while token.get_timestamp() < ts {
                                token = get(src).unwrap().unwrap().1;
                            }
                            let transaction_end = clock.get_time();
                            read_latencies.push(write_start - transaction_start);
                            write_latencies.push(write_end - write_start);
                            settle_latencies.push(transaction_end - write_end);
                        }
                        committed += 1;
                    }
                    Err(_) => {
                        if verbose {
                            println!("abort");
                        }
                        aborted += 1
                    }
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

            for &mut (src, dst, amt) in transactions {
                *target_balances.get_mut(&src).unwrap() -= amt;
                *target_balances.get_mut(&dst).unwrap() += amt;
            }

            for (account, balance) in target_balances {
                assert_eq!(get(account).unwrap().unwrap().0, balance);
            }
            println!("Audit found no irregularities");
        }
    }

    if measure_latency {
        let rl: u64 = read_latencies.iter().sum();
        let wl: u64 = write_latencies.iter().sum();
        let sl: u64 = settle_latencies.iter().sum();
        let n = write_latencies.len() as f64;
        println!("read latency: {:.3} μs", rl as f64 / n * 0.001);
        println!("write latency: {:.3} μs", wl as f64 / n * 0.001);
        println!("settle latency: {:.3} μs", sl as f64 / n * 0.001);
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

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    // setup db
    println!("Attempting to set up bank");
    let mut bank = if measure_latency {
        setup(nthreads + 1)
    } else {
        setup(nthreads)
    };

    // let system settle
    // thread::sleep(time::Duration::new(1, 0));
    let start = time::Instant::now();

    // benchmark
    let clients = (0..nthreads)
        .into_iter()
        .map(|i| {
            Some({
                     let mut transfers_put = bank.putter();
                     let balances_get: Box<Getter> = bank.getter();

                     let mut transactions = vec![];

                     if i == 0 {
                         populate(naccounts, &mut transfers_put);
                     }

                     thread::Builder::new()
                         .name(format!("bank{}", i))
                         .spawn(move || -> Vec<f64> {
                    client(i,
                           transfers_put,
                           balances_get,
                           naccounts,
                           start,
                           runtime,
                           verbose,
                           audit,
                           false,  /* measure_latency */
                           coarse_checktables,
                           &mut transactions)
                })
                         .unwrap()
                 })
        })
        .collect::<Vec<_>>();

    let latency_client = if measure_latency {
        Some({
                 let mut transfers_put = bank.putter();
                 let balances_get: Box<Getter> = bank.getter();

                 if nthreads == 0 {
                     populate(naccounts, &mut transfers_put);
                 }

                 let mut transactions = vec![];
                 thread::Builder::new()
                     .name(format!("bank{}", nthreads))
                     .spawn(move || -> Vec<f64> {
                client(nthreads,
                       transfers_put,
                       balances_get,
                       naccounts,
                       start,
                       runtime,
                       verbose,
                       audit,
                       true,  /* measure_latency */
                       coarse_checktables,
                       &mut transactions)
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
