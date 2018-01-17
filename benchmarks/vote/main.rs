#[macro_use]
extern crate clap;
extern crate distributary;
extern crate hdrsample;
extern crate hwloc;
extern crate libc;
extern crate rand;
extern crate rayon;

use hdrsample::Histogram;
use rand::Rng;
use std::io;
use std::fs;
use std::time;
use std::thread;
use std::sync::{atomic, Arc, Barrier, Mutex};
use std::cell::RefCell;

thread_local! {
    static THREAD_ID: RefCell<usize> = RefCell::new(1);
    static SJRN_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static SJRN_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static RMT_W: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
    static RMT_R: RefCell<Histogram<u64>> = RefCell::new(Histogram::new_with_bounds(1, 100_000, 4).unwrap());
}

const MAX_BATCH_SIZE: usize = 1024;
const MAX_BATCH_TIME_US: u32 = 10;

pub trait VoteClient {
    type Constructor;

    fn new(&clap::ArgMatches, articles: usize) -> Self::Constructor;
    fn from(&mut Self::Constructor) -> Self;
    fn handle_reads(&mut self, requests: &[(time::Instant, usize)]);
    fn handle_writes(&mut self, requests: &[(time::Instant, usize)]);
}

fn set_thread_affinity(cpus: hwloc::Bitmap) -> io::Result<()> {
    use std::mem;
    let mut cpuset = unsafe { mem::zeroed::<libc::cpu_set_t>() };
    for cpu in cpus {
        unsafe { libc::CPU_SET(cpu as usize, &mut cpuset) };
    }
    let errno = unsafe {
        libc::pthread_setaffinity_np(libc::pthread_self(), mem::size_of::<libc::cpu_set_t>(), &cpuset as *const _)
    };
    if errno != 0 {
        Err(io::Error::from_raw_os_error(errno))
    } else {
        Ok(())
    }
}

mod clients;

fn run<C>(global_args: &clap::ArgMatches, local_args: &clap::ArgMatches)
where
    C: VoteClient + Send + 'static,
{
    // each load generator can generate ~3M reqs/s
    let per_generator = 3_000_000;
    let mut target = value_t_or_exit!(global_args, "ops", f64);
    let ngen = (target as usize + per_generator - 1) / per_generator; // rounded up
    target /= ngen as f64;

    let nthreads = value_t_or_exit!(global_args, "threads", usize);
    let articles = value_t_or_exit!(global_args, "articles", usize);

    // we want any threads spawned by the VoteClient (e.g., distributary itself)
    // to be on their own NUMA node if there are many. or rather, we want to ensure that our
    // pool clients/load generators do not interfere with the server.
    let topo = hwloc::Topology::new();
    let mut bind_generator = hwloc::Bitmap::new();
    let mut bind_server = hwloc::Bitmap::new();
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

    let mut c = C::new(local_args, articles);
    let clients: Vec<Mutex<C>> = (0..nthreads)
        .map(|_| VoteClient::from(&mut c))
        .map(Mutex::new)
        .collect();
    let clients = Arc::new(clients);

    let hists = if let Some(mut f) = global_args
        .value_of("histogram")
        .and_then(|h| fs::File::open(h).ok())
    {
        use hdrsample::serialization::Deserializer;
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
    let pool = Arc::new(pool);

    let start = time::Instant::now();
    let generators: Vec<_> = (0..ngen)
        .map(|geni| {
            let pool = pool.clone();
            let clients = clients.clone();
            let finished = finished.clone();
            let global_args = global_args.clone();

            use std::mem;
            // we know that we won't drop the original args until the thread has exited
            let global_args: clap::ArgMatches<'static> = unsafe { mem::transmute(global_args) };

            thread::Builder::new()
                .name(format!("load-gen{}", geni))
                .spawn(move || {
                    let ops = run_generator(pool, clients, target, global_args);
                    finished.wait();
                    ops
                })
                .unwrap()
        })
        .collect();

    drop(pool);
    let ops: usize = generators.into_iter().map(|gen| gen.join().unwrap()).sum();
    drop(clients);
    drop(c);

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
                use hdrsample::serialization::Serializer;
                use hdrsample::serialization::V2Serializer;
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

fn run_generator<C>(
    pool: Arc<rayon::ThreadPool>,
    clients: Arc<Vec<Mutex<C>>>,
    target: f64,
    global_args: clap::ArgMatches,
) -> usize
where
    C: VoteClient + Send + 'static,
{
    let articles = value_t_or_exit!(global_args, "articles", usize);
    let runtime = time::Duration::from_secs(value_t_or_exit!(global_args, "runtime", u64));
    let end = time::Instant::now() + runtime;

    let mut ops = 0;
    let mut rng = rand::thread_rng();
    let max_batch_time = time::Duration::new(0, MAX_BATCH_TIME_US * 1_000);
    let interarrival = rand::distributions::exponential::Exp::new(target * 1e-9);

    // TODO: warmup

    let every = value_t_or_exit!(global_args, "ratio", u32);
    let mut queued_w = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut queued_r = Vec::with_capacity(MAX_BATCH_SIZE);
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
                let rmt = if write { &RMT_W } else { &RMT_R };
                let us = remote_t.as_secs() * 1_000_000 + remote_t.subsec_nanos() as u64 / 1_000;
                rmt.with(|h| {
                    let mut h = h.borrow_mut();
                    if h.record(us).is_err() {
                        let m = h.high();
                        h.record(m).unwrap();
                    }
                });

                let sjrn = if write { &SJRN_W } else { &SJRN_R };
                for (started, _) in batch {
                    let sjrn_t = done.duration_since(started);
                    let us = sjrn_t.as_secs() * 1_000_000 + sjrn_t.subsec_nanos() as u64 / 1_000;
                    sjrn.with(|h| {
                        let mut h = h.borrow_mut();
                        if h.record(us).is_err() {
                            let m = h.high();
                            h.record(m).unwrap();
                        }
                    });
                }
            }
        };

        let mut next = time::Instant::now();
        while next < end {
            let mut now = time::Instant::now();
            while now >= next {
                use rand::distributions::IndependentSample;

                // schedule next delivery
                next += time::Duration::new(0, interarrival.ind_sample(&mut rng) as u32);

                // only queue a new request if we're told to. if this is not the case, we've
                // just been woken up so we can realize we need to send a batch
                let q = (now, rng.gen_range(0, articles));
                if rng.gen_weighted_bool(every) {
                    queued_w.push(q);
                } else {
                    queued_r.push(q);
                }

                now = time::Instant::now();
            }

            let now = time::Instant::now();
            if queued_w.len() >= MAX_BATCH_SIZE
                || (!queued_w.is_empty() && now.duration_since(queued_w[0].0) > max_batch_time)
            {
                ops += queued_w.len();
                pool.spawn(enqueue(queued_w.split_off(0), true));
            }

            if queued_r.len() >= MAX_BATCH_SIZE
                || (!queued_r.is_empty() && now.duration_since(queued_r[0].0) > max_batch_time)
            {
                ops += queued_r.len();
                pool.spawn(enqueue(queued_r.split_off(0), false));
            }

            atomic::spin_loop_hint();
        }
    }

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
                .default_value("15")
                .help("Benchmark runtime in seconds"),
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
