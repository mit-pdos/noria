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
use rand::prelude::*;
use std::cell::RefCell;
use std::fs;
use std::mem;
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::thread;
use std::time;
use tokio::prelude::*;
use tower_service::Service;

thread_local! {
    static SJRN_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
    static SJRN_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
    static RMT_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
    static RMT_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
    static WP_DELAY: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(10, 1_000_000, 4).unwrap());
}

lazy_static! {
    static ref W_TIME_COUNT: Arc<Mutex<(Option<time::Instant>, u64)>> =
        Arc::new(Mutex::new((None, 0)));

    // The interval with which to write to the reserved key, in nanonseconds
    static ref WRITE_RESERVED_EVERY_US: time::Duration = time::Duration::new(0, 200_000_000);
    // The time after which to do the next write to the reserved key
    static ref NEXT_RESERVED_W: Arc<Mutex<time::Instant>> = Arc::new(Mutex::new(time::Instant::now()));
    // The write time of the reserved key
    static ref W_RESERVED_TIME: Arc<Mutex<Vec<time::Instant>>> = Arc::new(Mutex::new(vec![]));
    // The time each write was first observed of the reserved key
    static ref R_RESERVED_TIME: Arc<Mutex<Vec<time::Instant>>> = Arc::new(Mutex::new(vec![]));
}

// Reserved article id
const RESERVED_W_KEY: i32 = 1;
// Reserved author id
const RESERVED_R_KEY: i32 = 1;
// Max write propagation delay
const MAX_DELAY_US: u64 = 2000;

fn throughput(ops: usize, took: time::Duration) -> f64 {
    ops as f64 / took.as_secs_f64()
}

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
    let authors = value_t_or_exit!(global_args, "authors", usize);
    let warmup_ms = value_t_or_exit!(global_args, "warmup", u64) * 1_000;

    let params = Parameters {
        prime: !global_args.is_present("no-prime"),
        articles,
        authors,
    };

    let skewed = match global_args.value_of("distribution") {
        Some("skewed") => true,
        Some("uniform") => false,
        _ => unreachable!(),
    };

    let hists = if let Some(mut f) = global_args
        .value_of("histogram")
        .and_then(|h| fs::File::open(h).ok())
    {
        use hdrhistogram::serialization::Deserializer;
        let mut deserializer = Deserializer::new();
        (
            deserializer.deserialize(&mut f).unwrap(),
            deserializer.deserialize(&mut f).unwrap(),
            deserializer.deserialize(&mut f).unwrap(),
            deserializer.deserialize(&mut f).unwrap(),
            deserializer.deserialize(&mut f).unwrap(),
        )
    } else {
        (
            Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(10, 1_000_000, 4).unwrap(),
        )
    };

    let sjrn_w_t = Arc::new(Mutex::new(hists.0));
    let sjrn_r_t = Arc::new(Mutex::new(hists.1));
    let rmt_w_t = Arc::new(Mutex::new(hists.2));
    let rmt_r_t = Arc::new(Mutex::new(hists.3));
    let wp_delay = Arc::new(Mutex::new(hists.4));
    let finished = Arc::new(Barrier::new(nthreads + ngen));

    let ts = (
        sjrn_w_t.clone(),
        sjrn_r_t.clone(),
        rmt_w_t.clone(),
        rmt_r_t.clone(),
        wp_delay.clone(),
        finished.clone(),
    );
    let mut rt = tokio::runtime::Builder::new()
        .name_prefix("vote-")
        .before_stop(move || {
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
            WP_DELAY
                .with(|h| ts.4.lock().unwrap().add(&*h.borrow()))
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
                    if skewed {
                        run_generator(
                            handle,
                            ex,
                            zipf::ZipfDistribution::new(articles, 1.08).unwrap(),
                            zipf::ZipfDistribution::new(authors, 1.08).unwrap(),
                            target,
                            global_args,
                        )
                    } else {
                        run_generator(
                            handle,
                            ex,
                            rand::distributions::Uniform::new(1, articles + 1),
                            rand::distributions::Uniform::new(1, authors + 1),
                            target,
                            global_args,
                        )
                    }
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

    // write propagation delay over time
    let w_reserved_time = W_RESERVED_TIME.clone();
    let r_reserved_time = R_RESERVED_TIME.clone();
    let w_reserved_time = w_reserved_time.lock().unwrap();
    let r_reserved_time = r_reserved_time.lock().unwrap();
    println!("\n(relative write time (ms since start), delay (us))");
    print!("[");
    let start = w_reserved_time[0];
    let mut down = vec![];
    for i in 0..r_reserved_time.len() {
        let w_time = w_reserved_time[i];
        let r_time = r_reserved_time[i];
        let relative_w_time = w_time.duration_since(start);
        let relative_w_time_ms =
            relative_w_time.as_secs() * 1_000 +
            u64::from(relative_w_time.subsec_nanos()) / 1_000_000;
        if relative_w_time_ms < warmup_ms {
            // don't use data from warmup time
            continue;
        }
        let relative_w_time_ms = relative_w_time_ms - warmup_ms;

        let delay_us = if r_time == w_time {
            down.push(relative_w_time_ms);
            MAX_DELAY_US
        } else {
            let delay = r_time.duration_since(w_time);
            let us = delay.as_secs() * 1_000_000 + u64::from(delay.subsec_nanos()) / 1_000;
            if us > MAX_DELAY_US {
                down.push(relative_w_time_ms);
                MAX_DELAY_US
            } else {
                us
            }
        };
        if i == r_reserved_time.len() - 1 {
            print!("[{},{}]]\n", relative_w_time_ms, delay_us);
        } else {
            print!("[{},{}],", relative_w_time_ms, delay_us);
        }
    }
    if !down.is_empty() {
        println!("downtime: {}ms\n", down[down.len() - 1] - down[0]);
    }

    // write propagation delay
    let wp_delay = wp_delay.lock().unwrap();
    println!("write\t50\t{:.2}\t(us)", wp_delay.value_at_quantile(0.5));
    println!("write\t95\t{:.2}\t(us)", wp_delay.value_at_quantile(0.95));
    println!("write\t99\t{:.2}\t(us)", wp_delay.value_at_quantile(0.99));
    println!("write\t100\t{:.2}\t(us)\n", wp_delay.max());

    // sojourn/remote write/read time
    println!("# generated ops/s: {:.2}", ops);
    println!("# actual ops/s: {:.2}", wops);
    println!("# op\tpct\tsojourn\tremote");

    let sjrn_w_t = sjrn_w_t.lock().unwrap();
    let sjrn_r_t = sjrn_r_t.lock().unwrap();
    let rmt_w_t = rmt_w_t.lock().unwrap();
    let rmt_r_t = rmt_r_t.lock().unwrap();

    if let Some(h) = global_args.value_of("histogram") {
        match fs::File::create(h) {
            Ok(mut f) => {
                use hdrhistogram::serialization::Serializer;
                use hdrhistogram::serialization::V2Serializer;
                let mut s = V2Serializer::new();
                s.serialize(&sjrn_w_t, &mut f).unwrap();
                s.serialize(&sjrn_r_t, &mut f).unwrap();
                s.serialize(&rmt_w_t, &mut f).unwrap();
                s.serialize(&rmt_r_t, &mut f).unwrap();
            }
            Err(e) => {
                eprintln!("failed to open histogram file for writing: {:?}", e);
            }
        }
    }

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
    println!(
        "write\t100\t{:.2}\t{:.2}\t(all µs)",
        sjrn_w_t.max(),
        rmt_w_t.max()
    );
    println!(
        "read\t100\t{:.2}\t{:.2}\t(all µs)",
        sjrn_r_t.max(),
        rmt_r_t.max()
    );
}

fn run_generator<C, R>(
    mut handle: C,
    ex: tokio::runtime::TaskExecutor,
    w_id_rng: R,
    r_id_rng: R,
    target: f64,
    global_args: clap::ArgMatches,
) -> (f64, f64)
where
    C: VoteClient + 'static,
    R: rand::distributions::Distribution<usize>,
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

    let max_batch_time_us = value_t_or_exit!(global_args, "max-batch-time-us", u32);
    let max_batch_time = time::Duration::new(0, max_batch_time_us * 1_000);
    let interarrival = rand::distributions::Exp::new(target * 1e-9);

    let every = value_t_or_exit!(global_args, "ratio", u32);

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
    let enqueue = move |client: &mut C, queued: Vec<_>, mut keys: Vec<_>, write| {
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
            let warmup_done = sent.duration_since(start) > warmup;
            ndone.fetch_add(n, atomic::Ordering::AcqRel);

            if !write {
                for row in rows {
                    if row.is_empty() {
                        continue;
                    }
                    let author: i32 = row[0][0].clone().into();
                    if author != RESERVED_R_KEY {
                        continue;
                    }
                    let read_count = row[0][1].clone();
                    if read_count == DataType::None {
                        // no writes yet
                        continue;
                    }
                    let read_count: i64 = read_count.into();
                    let read_count = read_count as usize;
                    if read_count == 0 {
                        // no writes yet
                        continue;
                    }

                    let r_reserved_time = R_RESERVED_TIME.clone();
                    let mut r_reserved_time = r_reserved_time.lock().unwrap();
                    let w_reserved_time = W_RESERVED_TIME.clone();
                    let w_reserved_time = w_reserved_time.lock().unwrap();
                    while r_reserved_time.len() < read_count - 1 {
                        // for some reason, missed a read of a write.
                        // are writes happening too frequently?
                        println!("WARNING: missed read of vote {}", r_reserved_time.len() + 1);
                        let i = r_reserved_time.len();
                        r_reserved_time.push(w_reserved_time[i]);
                    }
                    if r_reserved_time.len() == read_count - 1 {
                        // println!("Read {}th vote at {:?}", read_count, done);
                        r_reserved_time.push(done);
                    }
                    // assert_eq!(r_reserved_time.len(), read_count);
                }
            }

            if warmup_done {
                let remote_t = done.duration_since(sent);
                let rmt = if write { &RMT_W } else { &RMT_R };
                let us =
                    remote_t.as_secs() * 1_000_000 + u64::from(remote_t.subsec_nanos()) / 1_000;
                rmt.with(|h| {
                    let mut h = h.borrow_mut();
                    if h.record(us).is_err() {
                        let m = h.high();
                        h.record(m).unwrap();
                    }
                });

                let sjrn = if write { &SJRN_W } else { &SJRN_R };
                for started in queued {
                    let sjrn_t = done.duration_since(started);
                    let us =
                        sjrn_t.as_secs() * 1_000_000 + u64::from(sjrn_t.subsec_nanos()) / 1_000;
                    sjrn.with(|h| {
                        let mut h = h.borrow_mut();
                        if h.record(us).is_err() {
                            let m = h.high();
                            h.record(m).unwrap();
                        }
                    });
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
    'outer: while next < end {
        let now = time::Instant::now();
        // NOTE: while, not if, in case we start falling behind
        while next <= now {
            // only queue a new request if we're told to. if this is not the case, we've
            // just been woken up so we can realize we need to send a batch
            if rng.gen_bool(1.0 / f64::from(every)) {
                let id = loop {
                    let id = w_id_rng.sample(&mut rng) as i32;
                    if id != RESERVED_W_KEY {
                        break id;
                    }
                };
                if queued_w.is_empty() && next_send.is_none() {
                    next_send = Some(next + max_batch_time);
                }
                queued_w_keys.push(id);
                queued_w.push(next);
            } else {
                let id = loop {
                    let id = r_id_rng.sample(&mut rng) as i32;
                    if id != RESERVED_R_KEY {
                        break id;
                    }
                };
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

        // send one write for the reserved key
        let next_reserved_w = NEXT_RESERVED_W.clone();
        let mut next_reserved_w = next_reserved_w.lock().unwrap();
        if now >= *next_reserved_w {
            // write down the time of write
            let w_reserved_time = W_RESERVED_TIME.clone();
            let mut w_reserved_time = w_reserved_time.lock().unwrap();
            w_reserved_time.push(now);

            // then queue the write
            if queued_w.is_empty() && next_send.is_none() {
                next_send = Some(now + max_batch_time);
            }
            if queued_w.is_empty() {
                queued_w_keys.push(RESERVED_W_KEY);
                queued_w.push(now);
            } else {
                queued_w_keys[0] = RESERVED_W_KEY;
            }

            // println!("Wrote {}th vote at {:?}", w_reserved_time.len(), now);
            *next_reserved_w += *WRITE_RESERVED_EVERY_US;
        }

        // send one read for the reserved key per batch
        if queued_r.is_empty() && next_send.is_none() {
            next_send = Some(now + max_batch_time);
        }
        if queued_r.is_empty() {
            queued_r_keys.push(RESERVED_R_KEY);
            queued_r.push(now);
        } else {
            queued_r_keys[0] = RESERVED_R_KEY;
        }

        if let Some(f) = next_send {
            if f <= now {
                // time to send at least one batch
                if !queued_w.is_empty() && now.duration_since(queued_w[0]) >= max_batch_time {
                    if let Async::Ready(()) =
                        task.enter(|| Service::<WriteRequest>::poll_ready(&mut handle).unwrap())
                    {
                        ops += queued_w.len();
                        print!("w");
                        enqueue(
                            &mut handle,
                            queued_w.split_off(0),
                            queued_w_keys.split_off(0),
                            true,
                        );
                    } else {
                        print!("a");
                        // we can't send the request yet -- generate a larger batch
                    }
                }

                if !queued_r.is_empty() && now.duration_since(queued_r[0]) >= max_batch_time {
                    if let Async::Ready(()) =
                        task.enter(|| Service::<ReadRequest>::poll_ready(&mut handle).unwrap())
                    {
                        ops += queued_r.len();
                        print!("r");
                        enqueue(
                            &mut handle,
                            queued_r.split_off(0),
                            queued_r_keys.split_off(0),
                            false,
                        );
                    } else {
                        print!("b");
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
                if now.duration_since(start) > warmup {
                    if worker_ops.is_none() {
                        worker_ops =
                            Some((time::Instant::now(), ndone.load(atomic::Ordering::Acquire)));
                    }

                    if early_exit && now < end {
                        let clients_completed = ndone.load(atomic::Ordering::Acquire) as u64;
                        let queued = ops as u64 - clients_completed;
                        let dur = first.elapsed().as_secs();

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
                }
            }
        }

        atomic::spin_loop_hint();
    }

    let gen = throughput(ops, start.elapsed());
    let worker_ops = worker_ops.map(|(measured, start)| {
        throughput(
            ndone.load(atomic::Ordering::Acquire) - start,
            measured.elapsed(),
        )
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
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("authors")
                .long("authors")
                .value_name("N")
                .default_value("400")
                .help("Number of authors to prepopulate the database with"),
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
                .default_value("30")
                .help("Benchmark runtime in seconds"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(true)
                .default_value("10")
                .help("Warmup time in seconds"),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .possible_values(&["uniform", "skewed"])
                .default_value("uniform")
                .help("Key distribution"),
        )
        .arg(
            Arg::with_name("histogram")
                .long("histogram")
                .help("Output serialized HdrHistogram to a file")
                .takes_value(true)
                .long_help(
                    "If the file already exists, the existing histogram is extended.\
                     There are four histograms, written out in order: \
                     sojourn-write, sojourn-read, remote-write, and remote-read",
                ),
        )
        .arg(
            Arg::with_name("ops")
                .long("target")
                .default_value("1000000")
                .help("Target operations per second"),
        )
        .arg(
            Arg::with_name("ratio")
                .long("write-every")
                .default_value("2")
                .value_name("N")
                .help("1-in-N chance of a write"),
        )
        .arg(
            Arg::with_name("max-batch-time-us")
                .long("max-batch-time-us")
                .default_value("200")
                .help("Time between sending batches to Noria."),
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
        .subcommand(
            SubCommand::with_name("netsoup")
                .arg(
                    Arg::with_name("zookeeper")
                        .short("z")
                        .long("zookeeper")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:2181")
                        .help("Address of Zookeeper instance"),
                )
                .arg(
                    Arg::with_name("deployment")
                        .long("deployment")
                        .required(true)
                        .takes_value(true)
                        .help("Soup deployment ID."),
                ),
        )
        .subcommand(
            SubCommand::with_name("memcached")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:11211")
                        .help("Address of memcached"),
                )
                .arg(
                    Arg::with_name("fast")
                        .long("fast")
                        .help("Only fetch vote counts, not titles."),
                ),
        )
        .subcommand(
            SubCommand::with_name("mssql")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value(
                            "server=tcp:127.0.0.1,1433;username=SA;TrustServerCertificate=true;",
                        )
                        .help("Address of MsSQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MsSQL database to use"),
                ),
        )
        .subcommand(SubCommand::with_name("null"))
        .subcommand(
            SubCommand::with_name("mysql")
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:3306")
                        .help("Address of MySQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MySQL database to use"),
                ),
        )
        .subcommand(
            SubCommand::with_name("hybrid")
                .arg(
                    Arg::with_name("memcached-address")
                        .long("memcached-address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:11211")
                        .help("Address of memcached"),
                )
                .arg(
                    Arg::with_name("mysql-address")
                        .long("mysql-address")
                        .takes_value(true)
                        .required(true)
                        .default_value("127.0.0.1:3306")
                        .help("Address of MySQL server"),
                )
                .arg(
                    Arg::with_name("database")
                        .long("database")
                        .takes_value(true)
                        .required(true)
                        .default_value("soup")
                        .help("MySQL database to use"),
                ),
        )
        .subcommand(
            SubCommand::with_name("localsoup")
                .arg(
                    Arg::with_name("shards")
                        .long("shards")
                        .takes_value(true)
                        .default_value("100")
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
        ("netsoup", Some(largs)) => run::<clients::netsoup::Conn>(&args, largs),
        //("memcached", Some(largs)) => run::<clients::memcached::Constructor>(&args, largs),
        //("mssql", Some(largs)) => run::<clients::mssql::Conf>(&args, largs),
        //("mysql", Some(largs)) => run::<clients::mysql::Conf>(&args, largs),
        //("hybrid", Some(largs)) => run::<clients::hybrid::Conf>(&args, largs),
        //("null", Some(largs)) => run::<()>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
