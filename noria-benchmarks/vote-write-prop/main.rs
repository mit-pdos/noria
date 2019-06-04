#![feature(try_blocks)]
#![feature(existential_type)]
#![feature(duration_float)]

#[macro_use]
extern crate clap;
#[macro_use]
extern crate lazy_static;

use failure::ResultExt;
use hdrhistogram::Histogram;
use noria::DataType;
use std::cell::RefCell;
use std::mem;
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::thread;
use std::time;
use tokio::prelude::*;
use tower_service::Service;

thread_local! {
    static WP_DELAY: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
}

lazy_static! {
    static ref W_TIME: Arc<Mutex<Option<time::Instant>>> = Arc::new(Mutex::new(None));
    static ref ACTUAL_COUNT: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
}

fn throughput(ops: usize, took: time::Duration) -> f64 {
    ops as f64 / took.as_secs_f64()
}

const MAX_BATCH_TIME_US: u32 = 1000;

mod clients;
use self::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};

fn run<C>(global_args: &clap::ArgMatches, local_args: &clap::ArgMatches)
where
    C: VoteClient + 'static,
    C: Service<ReadRequest, Response = Vec<Vec<Vec<DataType>>>, Error = failure::Error> + Clone + Send,
    C: Service<WriteRequest, Response = Vec<Vec<Vec<DataType>>>, Error = failure::Error> + Clone + Send,
    <C as Service<ReadRequest>>::Future: Send,
    <C as Service<ReadRequest>>::Response: Send,
    <C as Service<WriteRequest>>::Future: Send,
    <C as Service<WriteRequest>>::Response: Send,
{
    // zipf takes ~66ns to generate a random number depending on the CPU,
    // so each load generator cannot reasonably generate much more than ~1M reqs/s.
    let per_generator = 3_000_000;
    let mut target = value_t_or_exit!(global_args, "ops", f64);
    let ngen = (target as usize + per_generator - 1) / per_generator; // rounded up
    target /= ngen as f64;

    let nthreads = value_t_or_exit!(global_args, "threads", usize);
    let articles = value_t_or_exit!(global_args, "articles", usize);

    let params = Parameters {
        prime: !global_args.is_present("no-prime"),
        articles,
    };

    let hist = Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap();
    let wp_delay = Arc::new(Mutex::new(hist));
    let finished = Arc::new(Barrier::new(nthreads + ngen));

    let ts = (
        wp_delay.clone(),
        finished.clone(),
    );
    let mut rt = tokio::runtime::Builder::new()
        .name_prefix("vote-")
        .before_stop(move || {
            WP_DELAY
                .with(|h| ts.0.lock().unwrap().add(&*h.borrow()))
                .unwrap();

        })
        .build()
        .unwrap();

    let handle: C = {
        let local_args = local_args.clone();
        // we know that we won't drop the original args until the runtime has exited
        let local_args: clap::ArgMatches<'static> = unsafe { mem::transmute(local_args) };
        let ex = rt.executor();
        rt.block_on(future::lazy(move || C::new(ex, params, local_args)))
            .unwrap()
    };

    let generators: Vec<_> = (0..ngen)
        .map(|geni| {
            let ex = rt.executor();
            let handle = handle.clone();
            let global_args = global_args.clone();

            // we know that we won't drop the original args until the thread has exited
            let global_args: clap::ArgMatches<'static> = unsafe { mem::transmute(global_args) };

            thread::Builder::new()
                .name(format!("load-gen{}", geni))
                .spawn(move || {
                    run_generator(
                        handle,
                        ex,
                        target,
                        global_args,
                    )
                })
                .unwrap()
        })
        .collect();

    let mut ops = 0.0;
    let mut wops = 0.0;
    for gen in generators {
        let (gen, completed) = gen.join().unwrap();
        ops += gen;
        wops += completed;
    }
    drop(handle);
    rt.shutdown_on_idle().wait().unwrap();

    // all done!
    let wp_delay = wp_delay.lock().unwrap();

    println!("# generated ops/s: {:.2}", ops);
    println!("# actual ops/s: {:.2}", wops);
    println!("# op\tpct\tdelay");
    println!("write\t50\t{:.2}\t(us)", wp_delay.value_at_quantile(0.5));
    println!("write\t95\t{:.2}\t(us)", wp_delay.value_at_quantile(0.95));
    println!("write\t99\t{:.2}\t(us)", wp_delay.value_at_quantile(0.99));
    println!("write\t100\t{:.2}\t(us)\n", wp_delay.max());
}

fn run_generator<C>(
    mut handle: C,
    ex: tokio::runtime::TaskExecutor,
    target: f64,
    global_args: clap::ArgMatches,
) -> (f64, f64)
where
    C: VoteClient + 'static,
    C: Service<ReadRequest, Response = Vec<Vec<Vec<DataType>>>, Error = failure::Error> + Clone + Send,
    C: Service<WriteRequest, Response = Vec<Vec<Vec<DataType>>>, Error = failure::Error> + Clone + Send,
    <C as Service<ReadRequest>>::Future: Send,
    <C as Service<ReadRequest>>::Response: Send,
    <C as Service<WriteRequest>>::Future: Send,
    <C as Service<WriteRequest>>::Response: Send,
{
    let early_exit = !global_args.is_present("no-early-exit");
    let runtime = time::Duration::from_secs(value_t_or_exit!(global_args, "runtime", u64));
    let warmup = time::Duration::from_secs(value_t_or_exit!(global_args, "warmup", u64));

    let start = time::Instant::now();
    let end = start + warmup + runtime;

    let max_batch_time = time::Duration::new(0, MAX_BATCH_TIME_US * 1_000);
    let interarrival = rand::distributions::exponential::Exp::new(target * 1e-9);

    let measure_delay_every = value_t_or_exit!(global_args, "measure-delay-every", u64);

    let mut ops = 0;

    let first = time::Instant::now();
    let mut next = time::Instant::now();
    let mut next_send = None;

    let mut queued_w = Vec::new();
    let mut queued_w_keys = Vec::new();
    let mut queued_r = Vec::new();
    let mut queued_r_keys = Vec::new();

    let mut rng = rand::thread_rng();
    let mut task = tokio_mock_task::MockTask::new();

    // we *could* use a rayon::scope here to safely access stack variables from inside each job,
    // but that would *also* force us to place the load generators *on* the thread pool (because of
    // https://github.com/rayon-rs/rayon/issues/562). that comes with a number of unfortunate
    // side-effects, such as having to manage allocations of clients to workers, clean exiting,
    // etc. we *instead* just leak the one thing we care about (`ndone`) so that they can be
    // accessed from inside the jobs.
    //
    // this may change with https://github.com/rayon-rs/rayon/issues/544, but that's what we have
    // to do for now.
    let ndone: &'static _ = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let errd: &'static _ = &*Box::leak(Box::new(atomic::AtomicBool::new(false)));

    // when https://github.com/rust-lang/rust/issues/56556 is fixed, take &[i32] instead, make
    // Request hold &'a [i32] (then need for<'a> C: Service<Request<'a>>). then we no longer need
    // .split_off in calls to enqueue.
    let id = 1;
    let enqueue = move |client: &mut C, _queued: Vec<_>, mut keys: Vec<i32>, write| {
        let n = keys.len();
        let sent = time::Instant::now();
        let fut = if write {
            future::Either::A(
                client
                    .call(WriteRequest(keys))
                    .then(|r| r.context("failed to handle writes")),
            )
        } else {
            // deduplicate requested keys, because not doing so would be silly
            keys.sort_unstable();
            keys.dedup();
            future::Either::B(
                client
                    .call(ReadRequest(keys))
                    .then(|r| r.context("failed to handle reads")),
            )
        }
        .map(move |rows| {
            let done = time::Instant::now();
            ndone.fetch_add(n, atomic::Ordering::AcqRel);

            if sent.duration_since(start) > warmup && !write {
                for row in rows {
                    let read_count = row[0][2].clone();
                    if read_count == DataType::None {
                        // no writes yet
                        continue;
                    }
                    let read_count: i64 = read_count.into();
                    let read_count = read_count as u64;

                    let locks = (W_TIME.clone(), ACTUAL_COUNT.clone());
                    let mut w_time = locks.0.lock().unwrap();
                    let actual_count = locks.1.lock().unwrap();
                    if read_count != *actual_count {
                        // haven't read our write yet
                        // println!("read {}, actual {}", read_count, actual_count);
                        assert!(read_count < *actual_count);
                        continue;
                    }

                    // println!("Read {}th vote at {:?} ({} sent at time {:?})", read_count, done, *actual_count, *w_time);
                    if let Some(w_time) = w_time.take() {
                        let delay = done.duration_since(w_time);
                        let us =
                            delay.as_secs() * 1_000_000 / measure_delay_every +
                            u64::from(delay.subsec_nanos()) / 1_000 / measure_delay_every;
                        &WP_DELAY.with(|h| {
                            let mut h = h.borrow_mut();
                            if h.record(us).is_err() {
                                let m = h.high();
                                h.record(m).unwrap();
                            }
                        });
                    }
                }
            }
        });

        ex.spawn(fut.map_err(move |e| {
            if time::Instant::now() < end && !errd.swap(true, atomic::Ordering::SeqCst) {
                eprintln!("failed to enqueue request: {:?}", e)
            }
        }));
    };

    let mut worker_ops = None;
    let mut last_total = 0;
    let mut seconds = 0;

    'outer: while next < end {
        let now = time::Instant::now();
        // NOTE: while, not if, in case we start falling behind
        while next <= now {
            use rand::distributions::Distribution;

            // only queue a new request if we're told to. if this is not the case, we've
            // just been woken up so we can realize we need to send a batch
            let locks = (W_TIME.clone(), ACTUAL_COUNT.clone());
            let mut w_time = locks.0.lock().unwrap();
            let mut actual_count = locks.1.lock().unwrap();
            if w_time.is_none() {
                if queued_w.is_empty() && next_send.is_none() {
                    next_send = Some(next + max_batch_time);
                }
                queued_w_keys.push(id);
                queued_w.push(next);
                *actual_count += 1;
                if *actual_count % measure_delay_every == 0 {
                    *w_time = Some(now);
                }
            } else {
                if queued_r.is_empty() && next_send.is_none() {
                    next_send = Some(next + max_batch_time);
                }
                queued_r_keys.push(id);
                queued_r.push(next);
            }

            // schedule next delivery
            next += time::Duration::new(0, interarrival.sample(&mut rng) as u32);
        }

        // in case that took a while:
        let now = time::Instant::now();

        if let Some(f) = next_send {
            if f <= now {
                // time to send at least one batch
                if !queued_w.is_empty() && now.duration_since(queued_w[0]) >= max_batch_time {
                    if let Async::Ready(()) =
                        task.enter(|| Service::<WriteRequest>::poll_ready(&mut handle).unwrap())
                    {
                        ops += queued_w.len();
                        enqueue(
                            &mut handle,
                            queued_w.split_off(0),
                            queued_w_keys.split_off(0),
                            true,
                        );
                    } else {
                        // we can't send the request yet -- generate a larger batch
                    }
                }

                if !queued_r.is_empty() && now.duration_since(queued_r[0]) >= max_batch_time {
                    if let Async::Ready(()) =
                        task.enter(|| Service::<ReadRequest>::poll_ready(&mut handle).unwrap())
                    {
                        ops += queued_r.len();
                        enqueue(
                            &mut handle,
                            queued_r.split_off(0),
                            queued_r_keys.split_off(0),
                            false,
                        );
                    } else {
                        // we can't send the request yet -- generate a larger batch
                    }
                }

                // since next_send = Some, we better have sent at least one batch!
                // if not, the service must have not been ready, so we just continue
                if !queued_r.is_empty() && !queued_w.is_empty() {
                    continue 'outer;
                }

                next_send = None;
                if let Some(&qw) = queued_w.get(0) {
                    next_send = Some(qw + max_batch_time);
                }
                if let Some(&qr) = queued_r.get(0) {
                    next_send = Some(qr + max_batch_time);
                }

                // if the clients aren't keeping up, we want to make sure that we'll still
                // finish around the stipulated end time. we unfortunately can't rely on just
                // dropping the thread pool (https://github.com/rayon-rs/rayon/issues/544), so
                // we instead need to stop issuing requests earlier than we otherwise would
                // have. but make sure we're not still in the warmup phase, because the clients
                // *could* speed up
                let clients_completed = ndone.load(atomic::Ordering::Acquire) as u64;
                let dur = first.elapsed().as_secs();
                let rate = if dur != seconds {
                    let diff = clients_completed - last_total;
                    last_total = clients_completed;
                    seconds = dur;
                    Some((diff, clients_completed / seconds))
                } else {
                    None
                };
                if now.duration_since(start) > warmup {
                    if let Some(rate) = rate {
                        println!("{}: current {}, avg {}", seconds, rate.0, rate.1);
                    }
                    if worker_ops.is_none() {
                        worker_ops =
                            Some((time::Instant::now(), ndone.load(atomic::Ordering::Acquire)));
                    }

                    if early_exit && now < end {
                        let queued = ops as u64 - clients_completed;
                        if dur > 0 {
                            let client_rate = clients_completed / dur;
                            if client_rate > 0 {
                                let client_work_left = queued / client_rate;
                                if client_work_left > (end - now).as_secs() + 1 {
                                    // no point in continuing to feed work to the clients
                                    // they have enough work to keep them busy until the end
                                    eprintln!(
                                    "load generator quitting early as clients are falling behind"
                                );
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    if let Some(rate) = rate {
                        println!("{}: current {}, avg {} (warmup)", seconds, rate.0, rate.1);
                    }
                }
            }
        }

        atomic::spin_loop_hint();
    }

    println!("generated: {} {:?}", ops, start.elapsed());
    let gen = throughput(ops, start.elapsed());
    let worker_ops = worker_ops.map(|(measured, start)| {
        let ops = ndone.load(atomic::Ordering::Acquire) - start;
        let time = measured.elapsed();
        println!("actual: {} {:?}", ops, time);
        throughput(ops, time)
    });

    // need to drop the pool before waiting so that workers will exit
    // and thus hit the barrier
    drop(handle);
    (gen, worker_ops.unwrap_or(0.0))
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
                .default_value("1")
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
                .default_value("5")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(true)
                .default_value("0")
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("measure-delay-every")
                .long("measure-delay-every")
                .takes_value(true)
                .default_value("1")
                .help("Do reads to measure propagation delay every x writes"),
        )
        .arg(
            Arg::with_name("ops")
                .long("target")
                .default_value("1000000")
                .help("Target operations per second"),
        )
        .arg(
            Arg::with_name("no-prime")
                .long("no-prime")
                .help("Indicates that the client should not set up the database"),
        )
        .arg(
            Arg::with_name("no-early-exit")
                .long("no-early-exit")
                .help("Don't stop generating load when clients fall behind."),
        )
        .subcommand(SubCommand::with_name("null"))
        .subcommand(
            SubCommand::with_name("localsoup")
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
                    Arg::with_name("fudge-rpcs")
                        .long("fudge-rpcs")
                        .help("Send pointers instead of serializing data for writes"),
                )
                .arg(
                    Arg::with_name("purge")
                        .long("purge")
                        .takes_value(true)
                        .possible_values(&["none", "reader", "all"])
                        .default_value("none")
                        .help("Choose which views, if any, are placed beyond the materialization_frontier"),
                )
                .arg(
                    Arg::with_name("log-dir")
                        .long("log-dir")
                        .takes_value(true)
                        .help(
                            "Absolute path to the directory where the log files will be written.",
                        ),
                )
                .arg(
                    Arg::with_name("retain-logs-on-exit")
                        .long("retain-logs-on-exit")
                        .takes_value(false)
                        .requires("durability")
                        .help("Do not delete the base node logs on exit."),
                )
                .arg(
                    Arg::with_name("flush-timeout")
                        .long("flush-timeout")
                        .takes_value(true)
                        .default_value("1000000")
                        .help("Time to wait before processing a merged packet, in nanoseconds."),
                )
                .arg(
                    Arg::with_name("persistence-threads")
                        .long("persistence-threads")
                        .takes_value(true)
                        .default_value("1")
                        .help("Number of background threads used by PersistentState."),
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
        ("localsoup", Some(largs)) => run::<clients::localsoup::LocalNoria>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
