#![deny(unused_extern_crates)]

#[macro_use]
extern crate clap;
extern crate distributary;
extern crate futures;
extern crate futures_state_stream;
extern crate hdrhistogram;
extern crate hwloc;
extern crate libc;
extern crate memcached;
extern crate mysql;
extern crate rand;
extern crate rayon;
extern crate tiberius;
extern crate tokio_core;
extern crate zipf;

use hdrhistogram::Histogram;
use rand::Rng;
use std::cell::RefCell;
use std::fs;
use std::io;
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::thread;
use std::time;

thread_local! {
    static CLIENT: RefCell<Option<Box<VoteClient>>> = RefCell::new(None);
    static SJRN_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static SJRN_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static RMT_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static RMT_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
}

const MAX_BATCH_TIME_US: u32 = 1000;

fn set_thread_affinity(cpus: hwloc::Bitmap) -> io::Result<()> {
    use std::mem;
    let mut cpuset = unsafe { mem::zeroed::<libc::cpu_set_t>() };
    for cpu in cpus {
        unsafe { libc::CPU_SET(cpu as usize, &mut cpuset) };
    }
    let errno = unsafe {
        libc::pthread_setaffinity_np(
            libc::pthread_self(),
            mem::size_of::<libc::cpu_set_t>(),
            &cpuset as *const _,
        )
    };
    if errno != 0 {
        Err(io::Error::from_raw_os_error(errno))
    } else {
        Ok(())
    }
}

mod clients;
use clients::{Parameters, VoteClient, VoteClientConstructor};

fn run<CC>(global_args: &clap::ArgMatches, local_args: &clap::ArgMatches)
where
    CC: VoteClientConstructor + Send + 'static,
{
    // zipf takes ~66ns to generate a random number depending on the CPU,
    // so each load generator cannot reasonably generate much more than ~1M reqs/s.
    let per_generator = 1_000_000;
    let mut target = value_t_or_exit!(global_args, "ops", f64);
    let ngen = (target as usize + per_generator - 1) / per_generator; // rounded up
    target /= ngen as f64;

    let nthreads = value_t_or_exit!(global_args, "threads", usize);
    let articles = value_t_or_exit!(global_args, "articles", usize);

    let mut bind_generator = hwloc::Bitmap::new();
    let mut bind_server = hwloc::Bitmap::new();

    if CC::spawns_threads() {
        // we want any threads spawned by the VoteClient (e.g., distributary itself)
        // to be on their own NUMA node if there are many. or rather, we want to ensure that our
        // pool clients/load generators do not interfere with the server.
        let topo = hwloc::Topology::new();
        if let Ok(nodes) = topo.objects_with_type(&hwloc::ObjectType::NUMANode) {
            for node in nodes {
                if let Some(cpus) = node.allowed_cpuset() {
                    if bind_generator.weight() as usize >= ngen + nthreads {
                        for cpu in cpus {
                            bind_server.set(cpu);
                        }
                    } else {
                        for cpu in cpus {
                            bind_generator.set(cpu);
                        }
                    }
                }
            }
        }
    }

    if bind_generator.weight() == 0 && bind_server.weight() == 0 {
        eprintln!("# no numa binding");
    }

    if bind_generator.weight() != 0 && bind_server.weight() == 0 {
        // not enough nodes, so let the OS decide
        eprintln!("# not enough numa cores to do useful binding");
        bind_generator.clear();
    }

    if bind_server.weight() != 0 {
        // set affinity for *this* thread because (quoth man pthread_setaffinity_np):
        // A new thread created by pthread_create(3) inherits [..] its creator's CPU affinity mask.
        eprintln!("# bound server threads to: {:?}", bind_server);
        set_thread_affinity(bind_server).unwrap();
    }

    let params = Parameters {
        prime: !global_args.is_present("no-prime"),
        articles: articles,
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
        )
    } else {
        (
            Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap(),
        )
    };

    let sjrn_w_t = Arc::new(Mutex::new(hists.0));
    let sjrn_r_t = Arc::new(Mutex::new(hists.1));
    let rmt_w_t = Arc::new(Mutex::new(hists.2));
    let rmt_r_t = Arc::new(Mutex::new(hists.3));
    let finished = Arc::new(Barrier::new(nthreads + ngen));
    let ts = (
        sjrn_w_t.clone(),
        sjrn_r_t.clone(),
        rmt_w_t.clone(),
        rmt_r_t.clone(),
        finished.clone(),
    );

    if bind_generator.weight() != 0 {
        // now set the affinity for all of "our" generator/client threads
        eprintln!("# bound load generators to: {:?}", bind_generator);
        set_thread_affinity(bind_generator).unwrap();
    }

    let cc = Mutex::new(CC::new(&params, local_args));
    let pool = rayon::ThreadPoolBuilder::new()
        .thread_name(|i| format!("client-{}", i))
        .num_threads(nthreads)
        .start_handler(move |_| {
            CLIENT.with(|c| {
                *c.borrow_mut() = Some(Box::new(cc.lock().unwrap().make()));
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
    let pool = Arc::new(pool);

    let start = time::Instant::now();
    let generators: Vec<_> = (0..ngen)
        .map(|geni| {
            let pool = pool.clone();
            let finished = finished.clone();
            let global_args = global_args.clone();

            use std::mem;
            // we know that we won't drop the original args until the thread has exited
            let global_args: clap::ArgMatches<'static> = unsafe { mem::transmute(global_args) };

            thread::Builder::new()
                .name(format!("load-gen{}", geni))
                .spawn(move || {
                    let ops = if skewed {
                        run_generator(
                            pool,
                            zipf::ZipfDistribution::new(articles, 1.08).unwrap(),
                            finished,
                            target,
                            global_args,
                        )
                    } else {
                        run_generator(
                            pool,
                            rand::distributions::Range::new(1, articles + 1),
                            finished,
                            target,
                            global_args,
                        )
                    };
                    ops
                })
                .unwrap()
        })
        .collect();

    drop(pool);
    let ops: usize = generators.into_iter().map(|gen| gen.join().unwrap()).sum();

    // all done!
    let took = start.elapsed();
    println!(
        "# actual ops/s: {:.2}",
        ops as f64 / (took.as_secs() as f64 + took.subsec_nanos() as f64 / 1_000_000_000f64)
    );
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

fn run_generator<R>(
    pool: Arc<rayon::ThreadPool>,
    mut id_rng: R,
    finished: Arc<Barrier>,
    target: f64,
    global_args: clap::ArgMatches,
) -> usize
where
    R: rand::distributions::Sample<usize>,
{
    let runtime = time::Duration::from_secs(value_t_or_exit!(global_args, "runtime", u64));
    let warmup = time::Duration::from_secs(value_t_or_exit!(global_args, "warmup", u64));

    let start = time::Instant::now();
    let end = start + warmup + runtime;

    let max_batch_time = time::Duration::new(0, MAX_BATCH_TIME_US * 1_000);
    let interarrival = rand::distributions::exponential::Exp::new(target * 1e-9);

    let every = value_t_or_exit!(global_args, "ratio", u32);
    let ndone = atomic::AtomicUsize::new(0);

    let mut ops = 0;
    let mut enqueued = 0;

    let first = time::Instant::now();
    let mut next = time::Instant::now();
    let mut next_send = None;

    let mut queued_w = Vec::new();
    let mut queued_w_keys = Vec::new();
    let mut queued_r = Vec::new();
    let mut queued_r_keys = Vec::new();

    let mut rng = rand::thread_rng();

    // we *could* use a rayon::scope here to safely access stack variables from inside each job,
    // but that would *also* force us to place the load generators *on* the thread pool (because of
    // https://github.com/rayon-rs/rayon/issues/562). that comes with a number of unfortunate
    // side-effects, such as having to manage allocations of clients to workers, clean exiting,
    // etc. we *instead* unsafely make the one reference we care about (`ndone`) `&'static` so that
    // they can be accessed from inside the jobs. we know this is safe because of our barrier on
    // `finished`, which will only be passed (and hence the stack frame only destroyed) when there
    // are no more jobs in the pool. this may change with
    // https://github.com/rayon-rs/rayon/issues/544, but that's what we have to do for now.
    use std::mem;
    let ndone: &'static atomic::AtomicUsize = unsafe { mem::transmute(&ndone) };

    let enqueue = |queued: Vec<_>, mut keys: Vec<_>, write| {
        move || {
            CLIENT.with(|c| {
                let mut c = c.borrow_mut();
                let client = c.as_mut().unwrap();

                let sent = time::Instant::now();
                if write {
                    client.handle_writes(&keys[..]);
                } else {
                    // deduplicate requested keys, because not doing so would be silly
                    keys.sort_unstable();
                    keys.dedup();
                    client.handle_reads(&keys[..]);
                }
                let done = time::Instant::now();
                ndone.fetch_add(1, atomic::Ordering::AcqRel);

                if sent.duration_since(start) > warmup {
                    let remote_t = done.duration_since(sent);
                    let rmt = if write { &RMT_W } else { &RMT_R };
                    let us =
                        remote_t.as_secs() * 1_000_000 + remote_t.subsec_nanos() as u64 / 1_000;
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
                            sjrn_t.as_secs() * 1_000_000 + sjrn_t.subsec_nanos() as u64 / 1_000;
                        sjrn.with(|h| {
                            let mut h = h.borrow_mut();
                            if h.record(us).is_err() {
                                let m = h.high();
                                h.record(m).unwrap();
                            }
                        });
                    }
                }
            })
        }
    };

    while next < end {
        let now = time::Instant::now();
        // NOTE: while, not if, in case we start falling behind
        while next <= now {
            use rand::distributions::IndependentSample;

            // only queue a new request if we're told to. if this is not the case, we've
            // just been woken up so we can realize we need to send a batch
            let id = id_rng.sample(&mut rng) as i32;
            if rng.gen_weighted_bool(every) {
                if queued_w.is_empty() && next_send.is_none() {
                    next_send = Some(next + max_batch_time);
                }
                queued_w_keys.push(id);
                queued_w.push(next);
            } else {
                if queued_r.is_empty() && next_send.is_none() {
                    next_send = Some(next + max_batch_time);
                }
                queued_r_keys.push(id);
                queued_r.push(next);
            }

            // schedule next delivery
            next += time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);
        }

        // in case that took a while:
        let now = time::Instant::now();

        if let Some(f) = next_send {
            if f <= now {
                // time to send at least one batch

                if !queued_w.is_empty() && now.duration_since(queued_w[0]) >= max_batch_time {
                    ops += queued_w.len();
                    enqueued += 1;
                    pool.spawn(enqueue(
                        queued_w.split_off(0),
                        queued_w_keys.split_off(0),
                        true,
                    ));
                }

                if !queued_r.is_empty() && now.duration_since(queued_r[0]) >= max_batch_time {
                    ops += queued_r.len();
                    enqueued += 1;
                    pool.spawn(enqueue(
                        queued_r.split_off(0),
                        queued_r_keys.split_off(0),
                        false,
                    ));
                }

                // since next_send = Some, we better have sent at least one batch!
                next_send = None;
                assert!(queued_r.is_empty() || queued_w.is_empty());
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
                if now < end && now.duration_since(start) > warmup {
                    let clients_completed = ndone.load(atomic::Ordering::Acquire) as u64;
                    let queued = enqueued as u64 - clients_completed;
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

        atomic::spin_loop_hint();
    }

    // need to drop the pool before waiting so that workers will exit
    // and thus hit the barrier
    drop(pool);
    finished.wait();
    ops
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
                .default_value("19")
                .value_name("N")
                .help("1-in-N chance of a write"),
        )
        .arg(
            Arg::with_name("no-prime")
                .long("no-prime")
                .help("Indicates that the client should not set up the database"),
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
            SubCommand::with_name("memcached").arg(
                Arg::with_name("address")
                    .long("address")
                    .takes_value(true)
                    .required(true)
                    .default_value("127.0.0.1:11211")
                    .help("Address of memcached"),
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
                    Arg::with_name("workers")
                        .short("w")
                        .long("workers")
                        .takes_value(true)
                        .default_value("1")
                        .help("Number of workers to use"),
                )
                .arg(
                    Arg::with_name("readthreads")
                        .long("read-threads")
                        .value_name("N")
                        .default_value("2")
                        .help("Number of read threads to start"),
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
                    Arg::with_name("persist-bases")
                        .long("persist-bases")
                        .takes_value(false)
                        .requires("durability")
                        .help("Use SQlite for base nodes"),
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
                    Arg::with_name("flush-timeout")
                        .long("flush-timeout")
                        .takes_value(true)
                        .default_value("100000")
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
        ("localsoup", Some(largs)) => run::<clients::localsoup::Constructor>(&args, largs),
        ("netsoup", Some(largs)) => run::<clients::netsoup::Constructor>(&args, largs),
        ("memcached", Some(largs)) => run::<clients::memcached::Constructor>(&args, largs),
        ("mssql", Some(largs)) => run::<clients::mssql::Conf>(&args, largs),
        ("mysql", Some(largs)) => run::<clients::mysql::Conf>(&args, largs),
        ("hybrid", Some(largs)) => run::<clients::hybrid::Conf>(&args, largs),
        (name, _) => eprintln!("unrecognized backend type '{}'", name),
    }
}
