#[macro_use]
extern crate clap;
extern crate distributary;
extern crate hdrsample;
extern crate rand;
extern crate rayon;

use hdrsample::Histogram;
use rand::Rng;
use std::time;
use std::thread;
use std::sync::{Arc, Barrier, Mutex};
use std::cell::RefCell;

thread_local! {
    static THREAD_ID: RefCell<usize> = RefCell::new(1);
    static SJRN: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
    static RMT: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
}

const MAX_BATCH_SIZE: usize = 512;
const MAX_BATCH_TIME_US: u32 = 10;

pub trait VoteClient {
    type Constructor;

    fn new(&clap::ArgMatches, articles: usize) -> Self::Constructor;
    fn from(&mut Self::Constructor) -> Self;
    fn handle(&mut self, requests: &[(time::Instant, usize)]);
}

mod clients;

fn run<C>(global_args: &clap::ArgMatches, local_args: &clap::ArgMatches)
where
    C: VoteClient + Send + 'static,
{
    let nthreads = value_t_or_exit!(global_args, "threads", usize);
    let articles = value_t_or_exit!(global_args, "articles", usize);
    let mut c = C::new(local_args, articles);

    let clients: Vec<Mutex<C>> = (0..nthreads)
        .map(|_| VoteClient::from(&mut c))
        .map(Mutex::new)
        .collect();
    let clients = Arc::new(clients);

    let sjrn_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let rmt_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let finished = Arc::new(Barrier::new(nthreads + 1));
    let ts = (sjrn_t.clone(), rmt_t.clone(), finished.clone());

    let pool = rayon::Configuration::new()
        .thread_name(|i| format!("client-{}", i))
        .num_threads(nthreads)
        .start_handler(|i| {
            THREAD_ID.with(|tid| {
                *tid.borrow_mut() = i;
            })
        })
        .exit_handler(move |_| {
            SJRN.with(|h| ts.0.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            RMT.with(|h| ts.1.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            ts.2.wait();
        })
        .build()
        .unwrap();

    let runtime = time::Duration::from_secs(value_t_or_exit!(global_args, "runtime", u64));
    let end = time::Instant::now() + runtime;

    let mut rng = rand::thread_rng();
    let max_batch_time = time::Duration::new(0, MAX_BATCH_TIME_US * 1_000);
    let interarrival = rand::distributions::exponential::Exp::new(
        value_t_or_exit!(global_args, "ops", f64) * 1e-9,
    );

    // TODO: warmup

    let mut queued = Vec::new();
    loop {
        use rand::distributions::IndependentSample;

        let next =
            time::Instant::now() + time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
        if next > end {
            break;
        }

        loop {
            let now = time::Instant::now();

            if now >= next {
                // only queue a new request if we're told to. if this is not the case, we've
                // just been woken up so we can realize we need to send a batch
                queued.push((now, rng.gen_range(0, articles)));
            }

            if queued.len() >= MAX_BATCH_SIZE
                || (!queued.is_empty() && now.duration_since(queued[0].0) > max_batch_time)
            {
                let batch = queued.split_off(0);
                let clients = clients.clone();
                pool.spawn(move || {
                    let tid = THREAD_ID.with(|tid| *tid.borrow());

                    // TODO: avoid the overhead of taking an uncontended lock
                    let mut client = clients[tid].try_lock().unwrap();

                    let sent = time::Instant::now();
                    client.handle(&batch[..]);
                    let done = time::Instant::now();

                    let remote_t = done.duration_since(sent);
                    RMT.with(|h| {
                        h.borrow_mut()
                            .record(remote_t.subsec_nanos() as u64 / 1_000)
                            .unwrap()
                    });

                    for (started, _) in batch {
                        let sjrn_t = done.duration_since(started);
                        SJRN.with(|h| {
                            h.borrow_mut()
                                .record(sjrn_t.subsec_nanos() as u64 / 1_000)
                                .unwrap()
                        });
                    }
                });
            }

            if now >= next {
                break;
            } else {
                let mut left = next - now;
                if !queued.is_empty() {
                    let qnext = queued[0].0 + max_batch_time;
                    if next > qnext {
                        // we know we didn't just flush, so qnext *must* be in the future
                        left = qnext - now;
                    }
                }

                thread::sleep(left);
            }
        }
    }

    drop(pool);
    finished.wait();
    drop(clients);
    drop(c);

    // all done!
    for iv in sjrn_t.lock().unwrap().iter_quantiles(1) {
        println!(
            "percentile SJRN {:.2} {:.2}",
            iv.value_iterated_to(),
            iv.percentile()
        );
    }
    for iv in rmt_t.lock().unwrap().iter_quantiles(1) {
        println!(
            "percentile RMT {:.2} {:.2}",
            iv.value_iterated_to(),
            iv.percentile()
        );
    }
}

fn main() {
    use clap::{App, Arg, SubCommand};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for in-memory Soup")
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
                .default_value("1")
                .help("Number of client load threads to run"),
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
            Arg::with_name("ops")
                .long("target-ops")
                .default_value("1000")
                .help("Target operations per second"),
        )
        .subcommand(
            SubCommand::with_name("localsoup")
                .arg(
                    Arg::with_name("workers")
                        .short("w")
                        .long("workers")
                        .takes_value(true)
                        .default_value("2")
                        .help("Number of workers to use"),
                )
                .arg(
                    Arg::with_name("shards")
                        .long("shards")
                        .takes_value(true)
                        .default_value("2")
                        .help("Shard the graph this many ways (0 = disable sharding)."),
                )
                .arg(
                    Arg::with_name("durability")
                        .long("durability")
                        .takes_value(false)
                        .help("Enable durability for Base nodes"),
                )
                .arg(
                    Arg::with_name("retain-logs-on-exit")
                        .long("retain-logs-on-exit")
                        .takes_value(false)
                        .requires("durability")
                        .help("Do not delete the base node logs on exit."),
                )
                .arg(
                    Arg::with_name("write-batch-size")
                        .long("write-batch-size")
                        .takes_value(true)
                        .default_value("512")
                        .help("Size of batches processed at base nodes."),
                ),
        )
        .get_matches();

    match args.subcommand() {
        ("localsoup", Some(largs)) => run::<clients::localsoup::Client>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
