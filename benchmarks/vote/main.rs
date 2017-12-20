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
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::cell::RefCell;

thread_local! {
    static THREAD_ID: RefCell<usize> = RefCell::new(1);
    static SJRN_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
    static SJRN_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
    static RMT_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
    static RMT_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 1_000_000, 4).unwrap());
}

const MAX_BATCH_SIZE: usize = 512;
const MAX_BATCH_TIME_US: u32 = 10;

pub trait VoteClient {
    type Constructor;

    fn new(&clap::ArgMatches, articles: usize) -> Self::Constructor;
    fn from(&mut Self::Constructor) -> Self;
    fn handle_reads(&mut self, requests: &[(time::Instant, usize)]);
    fn handle_writes(&mut self, requests: &[(time::Instant, usize)]);
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

    let sjrn_w_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let sjrn_r_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let rmt_w_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let rmt_r_t = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(10, 10_000_000, 4).unwrap(),
    ));
    let finished = Arc::new(Barrier::new(nthreads + 1));
    let ts = (
        sjrn_w_t.clone(),
        sjrn_r_t.clone(),
        rmt_w_t.clone(),
        rmt_r_t.clone(),
        finished.clone(),
    );

    let pool = rayon::Configuration::new()
        .thread_name(|i| format!("client-{}", i))
        .num_threads(nthreads)
        .start_handler(|i| {
            THREAD_ID.with(|tid| {
                *tid.borrow_mut() = i;
            })
        })
        .exit_handler(move |_| {
            SJRN_W
                .with(|h| ts.0.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            SJRN_R
                .with(|h| ts.1.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            RMT_W
                .with(|h| ts.2.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            RMT_R
                .with(|h| ts.3.lock().unwrap().add(&*h.borrow()))
                .unwrap();
            ts.4.wait();
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

    {
        let enqueue = |batch: Vec<_>, write| {
            let clients = clients.clone();
            move || {
                let tid = THREAD_ID.with(|tid| *tid.borrow());

                // TODO: avoid the overhead of taking an uncontended lock
                let mut client = clients[tid].try_lock().unwrap();

                let sent = time::Instant::now();
                if write {
                    client.handle_writes(&batch[..]);
                } else {
                    client.handle_reads(&batch[..]);
                }
                let done = time::Instant::now();

                let remote_t = done.duration_since(sent);
                assert_eq!(remote_t.as_secs(), 0);

                let rmt = if write { &RMT_W } else { &RMT_R };
                rmt.with(|h| {
                    h.borrow_mut()
                        .record(remote_t.subsec_nanos() as u64 / 1_000)
                        .unwrap()
                });

                let sjrn = if write { &SJRN_W } else { &SJRN_R };
                for (started, _) in batch {
                    let sjrn_t = done.duration_since(started);
                    assert_eq!(sjrn_t.as_secs(), 0);
                    sjrn.with(|h| {
                        h.borrow_mut()
                            .record(sjrn_t.subsec_nanos() as u64 / 1_000)
                            .unwrap()
                    });
                }
            }
        };

        let every = value_t_or_exit!(global_args, "ratio", u32);
        let mut queued_w = Vec::new();
        let mut queued_r = Vec::new();
        loop {
            use rand::distributions::IndependentSample;

            let next = time::Instant::now()
                + time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
            if next > end {
                break;
            }

            loop {
                let now = time::Instant::now();

                if now >= next {
                    // only queue a new request if we're told to. if this is not the case, we've
                    // just been woken up so we can realize we need to send a batch
                    let q = (now, rng.gen_range(0, articles));
                    if rng.gen_weighted_bool(every) {
                        queued_w.push(q);
                    } else {
                        queued_r.push(q);
                    }
                }

                if queued_w.len() >= MAX_BATCH_SIZE
                    || (!queued_w.is_empty() && now.duration_since(queued_w[0].0) > max_batch_time)
                {
                    pool.spawn(enqueue(queued_w.split_off(0), true));
                }

                if queued_r.len() >= MAX_BATCH_SIZE
                    || (!queued_r.is_empty() && now.duration_since(queued_r[0].0) > max_batch_time)
                {
                    pool.spawn(enqueue(queued_r.split_off(0), false));
                }

                if now >= next {
                    break;
                } else {
                    let mut left = next - now;
                    if !queued_w.is_empty() {
                        let qleft = (queued_w[0].0 + max_batch_time) - now;
                        if left > qleft {
                            left = qleft;
                        }
                    }
                    if !queued_r.is_empty() {
                        let qleft = (queued_r[0].0 + max_batch_time) - now;
                        if left > qleft {
                            left = qleft;
                        }
                    }

                    if left.as_secs() == 0 && left.subsec_nanos() < 1_000 {
                        atomic::spin_loop_hint();
                    } else {
                        thread::sleep(left);
                    }
                }
            }
        }
    }

    drop(pool);
    finished.wait();
    drop(clients);
    drop(c);

    // all done!
    println!("op\tpct\tsojourn\tremote");

    let sjrn_w_t = sjrn_w_t.lock().unwrap();
    let rmt_w_t = rmt_w_t.lock().unwrap();
    let sjrn_r_t = sjrn_r_t.lock().unwrap();
    let rmt_r_t = rmt_r_t.lock().unwrap();
    println!(
        "write\t50\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.5),
        rmt_w_t.value_at_quantile(0.5)
    );
    println!(
        "read\t50\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.5),
        rmt_r_t.value_at_quantile(0.5)
    );
    println!(
        "write\t95\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.95),
        rmt_w_t.value_at_quantile(0.95)
    );
    println!(
        "read\t95\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.95),
        rmt_r_t.value_at_quantile(0.95)
    );
    println!(
        "write\t99\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.value_at_quantile(0.99),
        rmt_w_t.value_at_quantile(0.99)
    );
    println!(
        "read\t99\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.value_at_quantile(0.99),
        rmt_r_t.value_at_quantile(0.99)
    );
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
                .long("target")
                .default_value("20000")
                .help("Target operations per second"),
        )
        .arg(
            Arg::with_name("ratio")
                .long("write-every")
                .default_value("19")
                .value_name("N")
                .help("1-in-N chance of a write"),
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
                )
                .arg(
                    Arg::with_name("stupid")
                        .long("stupid")
                        .help("Make the migration stupid")
                        .requires("migrate"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .help("Include logging output"),
                ),
        )
        .get_matches();

    match args.subcommand() {
        ("localsoup", Some(largs)) => run::<clients::localsoup::Client>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
