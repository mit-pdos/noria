#[macro_use]
extern crate clap;

extern crate rand;
extern crate randomkit;

extern crate distributary;

use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use std::sync;
use std::thread;
use std::time;

use std::collections::HashMap;

use distributary::{Blender, Base, Aggregation, JoinBuilder, Datas, DataType, Token,
                   TransactionResult};

extern crate hdrsample;
use hdrsample::Histogram;

#[allow(dead_code)]
type Put = Box<Fn(Vec<DataType>) + Send + 'static>;
type TxPut = Box<Fn(Vec<DataType>, Token) -> TransactionResult + Send + 'static>;
#[allow(dead_code)]
type Get = Box<Fn(&DataType) -> Datas + Send + Sync>;
type TxGet = Box<Fn(&DataType) -> (Datas, Token) + Send + Sync>;

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
    transfers: Vec<TxPut>,
    balances: sync::Arc<Option<TxGet>>,
    _g: Blender,
}

pub fn setup(num_putters: usize) -> Box<Bank> {
    // set up graph
    let mut g = Blender::new();

    let transfers;
    let credits;
    let debits;
    let balances;
    let (_, balancesq) = {
        // migrate
        let mut mig = g.start_migration();

        // add transfers base table
        transfers = mig.add_ingredient("transfers", &["src_acct", "dst_acct", "amount"], Base {});

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
        let j2 = JoinBuilder::new(vec![(credits, 0), (credits, 1), (debits, 1)])
            .from(credits, vec![1, 0])
            .join(debits, vec![1, 0]);
        balances = mig.add_ingredient("balances", &["acct_id", "credit", "debit"], j2);
        let balancesq = Some(mig.transactional_maintain(balances, 0));

        let d = mig.add_domain();
        mig.assign_domain(transfers, d);
        mig.assign_domain(credits, d);
        mig.assign_domain(debits, d);
        mig.assign_domain(balances, d);

        // start processing
        (mig.commit(), balancesq)
    };

    Box::new(Bank {
        transfers: (0..num_putters)
            .into_iter()
            .map(|_| g.get_putter(transfers.clone()).1)
            .collect::<Vec<_>>(),
        balances: sync::Arc::new(balancesq),
        _g: g, // so it's not dropped and waits for threads
    })
}

impl Bank {
    fn getter(&mut self) -> Box<Getter> {
        Box::new(self.balances.clone())
    }
    fn putter(&mut self) -> Box<Putter> {
        Box::new(self.transfers.pop().unwrap())
    }
}

pub trait Putter: Send {
    fn transfer<'a>(&'a mut self) -> Box<FnMut(i64, i64, i64, Token) -> TransactionResult + 'a>;
}

impl Putter for TxPut {
    fn transfer<'a>(&'a mut self) -> Box<FnMut(i64, i64, i64, Token) -> TransactionResult + 'a> {
        Box::new(move |src, dst, amount, token| {
            self(vec![src.into(), dst.into(), amount.into()], token.into())
        })
    }
}

pub trait Getter: Send {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, Token)> + 'a>;
}

impl Getter for sync::Arc<Option<TxGet>> {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, Token)> + 'a> {
        Box::new(move |id| {
            if let Some(ref g) = *self.as_ref() {
                let (res, token) = g(&id.into());
                assert_eq!(res.len(), 1);
                res.into_iter().next().map(|row| {
                    // we only care about the first result
                    let mut row = row.into_iter();
                    let _: i64 = row.next().unwrap().into();
                    let credit: i64 = row.next().unwrap().into();
                    let debit: i64 = row.next().unwrap().into();
                    (credit - debit, token)
                })
            } else {
                use std::time::Duration;
                use std::thread;
                // avoid spinning
                thread::sleep(Duration::from_secs(1));
                None
            }
        })
    }
}

fn populate(naccounts: isize, transfers_put: &mut Box<Putter>) {
    // prepopulate non-transactionally (this is okay because we add no accounts while running the
    // benchmark)
    println!("Connected. Setting up {} accounts.", naccounts);
    {
        // let accounts_put = bank.accounts.as_ref().unwrap();
        let mut money_put = transfers_put.transfer();
        for i in 0..naccounts {
            // accounts_put(vec![DataType::Number(i as i64), format!("user {}", i).into()]);
            money_put(0, i as i64, 1000, Token::empty());
            money_put(i as i64, 0, 1, Token::empty());
        }
    }
    println!("Done with account creation");
}

fn client(i: usize,
          mut transfers_put: Box<Putter>,
          balances_get: Box<Getter>,
          distribution: &str,
          naccounts: isize,
          start: time::Instant,
          runtime: time::Duration,
          verbose: bool,
          cdf: bool,
          audit: bool,
          transactions: &mut Vec<(i64, i64, i64)>)
          -> Vec<f64> {
    let mut count = 0;
    let mut committed = 0;
    let mut aborted = 0;
    let mut samples = Histogram::<u64>::new_with_bounds(1, 100000, 3).unwrap();
    let mut last_reported = start;
    let mut throughputs = Vec::new();

    let mut a_rng = Rng::from_seed(42);
    let zipf_dist = Zipf::new(1.07).unwrap();
    let uniform_dist = Uniform::new(1.0, naccounts as f64).unwrap();

    let mut sample = |distribution| -> isize {
        match distribution {
            "uniform" => uniform_dist.sample(&mut a_rng) as isize,
            "zipf" => zipf_dist.sample(&mut a_rng) as isize,
            _ => panic!("unknown account ID distribution {}!", distribution),
        }
    };

    {
        let mut get = balances_get.get();
        let mut put = transfers_put.transfer();

        let mut sample_pair = || -> (i64, i64) {
            let dst_acct_rnd_id = sample(&*distribution);
            assert!(dst_acct_rnd_id > 0);
            let mut src_acct_rnd_id = sample(&*distribution);
            while src_acct_rnd_id == dst_acct_rnd_id {
                src_acct_rnd_id = sample(&*distribution);
            }
            let dst_acct_rnd_id = std::cmp::min(dst_acct_rnd_id, naccounts - 1) as i64;
            let src_acct_rnd_id = std::cmp::min(src_acct_rnd_id, naccounts - 1) as i64;
            assert!(src_acct_rnd_id > 0);
            (src_acct_rnd_id, dst_acct_rnd_id)
        };

        while start.elapsed() < runtime {
            let pair = sample_pair();

            let (balance, token) = get(pair.0).unwrap();
            if verbose {
                println!("t{} read {}: {} @ {:#?} (for {})",
                         i,
                         pair.0,
                         balance,
                         token,
                         pair.1);
            }

            // try to make both transfers
            {
                let mut do_tx = |src, dst, amt, tkn| {
                    let mut count_result = |res| match res {
                        TransactionResult::Committed(ts) => {
                            if verbose {
                                println!("commit @ {}", ts);
                            }
                            if audit {
                                transactions.push((src, dst, amt));
                            }
                            committed += 1
                        }
                        TransactionResult::Aborted => {
                            if verbose {
                                println!("abort");
                            }
                            aborted += 1
                        }
                    };

                    if verbose {
                        println!("trying {} -> {} of {}", src, dst, amt);
                    }

                    if cdf {
                        let t = time::Instant::now();
                        count_result(put(src, dst, amt, tkn));
                        let t = (dur_to_ns!(t.elapsed()) / 1000) as i64;
                        if samples.record(t).is_err() {
                            println!("failed to record slow put ({}ns)", t);
                        }
                    } else {
                        count_result(put(src, dst, amt, tkn));
                    }
                    count += 1;
                };

                if pair.0 != 0 {
                    assert!(balance >= 0, format!("{} balance is {}", pair.0, balance));
                }

                if balance >= 100 {
                    do_tx(pair.0, pair.1, 100, token);
                }
            }

            // check if we should report
            if last_reported.elapsed() > time::Duration::from_secs(1) {
                let ts = last_reported.elapsed();
                let throughput = count as f64 /
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
                assert_eq!(get(account).unwrap().0, balance);
            }
            println!("Audit found no irregularities");
        }
    }

    if cdf {
        for (v, p, _, _) in samples.iter_percentiles(1) {
            println!("percentile PUT {:.2} {:.2}", v, p);
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
        .arg(Arg::with_name("cdf")
            .long("cdf")
            .takes_value(false)
            .help("produce a CDF of recorded latencies for each client at the end"))
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
        .arg(Arg::with_name("distribution")
            .short("d")
            .long("distribution")
            .value_name("D")
            .possible_values(&["zipf", "uniform"])
            .default_value("uniform")
            .help("Transfer source/destination distribution"))
        .arg(Arg::with_name("threads")
            .short("t")
            .long("threads")
            .value_name("T")
            .default_value("2")
            .help("Number of client threads"))
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
    let cdf = args.is_present("cdf");
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let naccounts = value_t_or_exit!(args, "naccounts", isize);
    let nthreads = value_t_or_exit!(args, "threads", usize);
    let distribution = args.value_of("distribution").unwrap();
    let verbose = args.is_present("verbose");
    let audit = args.is_present("audit");

    // setup db
    println!("Attempting to set up bank");
    let mut bank = setup(nthreads);

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
                let distribution = distribution.to_owned();
                let naccounts = naccounts.clone();

                let mut transactions = vec![];

                if i == 0 {
                    populate(naccounts, &mut transfers_put);
                }

                thread::spawn(move || -> Vec<f64> {
                    client(i,
                           transfers_put,
                           balances_get,
                           &distribution,
                           naccounts,
                           start,
                           runtime,
                           verbose,
                           cdf,
                           audit,
                           &mut transactions)
                })
            })
        })
        .collect::<Vec<_>>();

    let avg_put_throughput = |th: Vec<f64>| if avg {
        let sum: f64 = th.iter().sum();
        println!("avg PUT: {:.2}", sum / th.len() as f64);
    };

    // clean
    for c in clients {
        if let Some(client) = c {
            match client.join() {
                Err(e) => panic!(e),
                Ok(th) => avg_put_throughput(th),
            }
        }
    }
}
