use targets;
use targets::{Putter, Getter};

use std::sync::mpsc;
use std::thread;
use std::time;
use std;

use rand;
use spmc;
use rand::Rng as StdRng;
use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use hdrsample::Histogram;
use hdrsample;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[derive(Clone, Copy)]
pub enum Distribution {
    Uniform,
    Zipf,
}

impl<'a> From<&'a str> for Distribution {
    fn from(s: &'a str) -> Self {
        match s {
            "uniform" => Distribution::Uniform,
            "zipf" => Distribution::Zipf,
            _ => panic!("unknown distribution '{}'", s),
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeConfig {
    ngetters: usize,
    narticles: isize,
    distribution: Distribution,
    runtime: time::Duration,
    cdf: bool,
    stage: bool,
    migrate_after: Option<time::Duration>,
}

impl RuntimeConfig {
    pub fn new(ngetters: usize, narticles: isize, runtime: time::Duration) -> Self {
        RuntimeConfig {
            ngetters: ngetters,
            narticles: narticles,
            distribution: Distribution::Uniform,
            runtime: runtime,
            cdf: true,
            stage: false,
            migrate_after: None,
        }
    }

    pub fn put_then_get(&mut self) {
        assert!(self.migrate_after.is_none(),
                "staged migration is unsupported");
        self.stage = true;
    }

    pub fn produce_cdf(&mut self, yes: bool) {
        self.cdf = yes;
    }

    pub fn set_distribution(&mut self, d: Distribution) {
        self.distribution = d;
    }

    pub fn perform_migration_at(&mut self, t: time::Duration) {
        assert!(!self.stage, "staged migration is unsupported");
        self.migrate_after = Some(t);
    }
}

#[derive(Clone, Copy)]
enum Period {
    PreMigration,
    PostMigration,
}

pub struct BenchmarkResult {
    throughputs: Vec<f64>,
    samples: Option<Histogram<u64>>,
}

impl Default for BenchmarkResult {
    fn default() -> Self {
        BenchmarkResult {
            throughputs: Vec::new(),
            samples: None,
        }
    }
}

impl BenchmarkResult {
    fn keep_cdf(&mut self) {
        self.samples = Some(Histogram::<u64>::new_with_bounds(10, 10000000, 4).unwrap());
    }

    pub fn avg_throughput(&self) -> f64 {
        let s: f64 = self.throughputs.iter().sum();
        s / self.throughputs.len() as f64
    }

    pub fn cdf_percentiles(&self) -> Option<hdrsample::iterators::HistogramIterator<u64, hdrsample::iterators::recorded::Iter<u64>>> {
        self.samples.as_ref().map(|s| s.iter_recorded())
    }

    pub fn sum_len(&self) -> (f64, usize) {
        (self.throughputs.iter().sum(), self.throughputs.len())
    }
}

#[derive(Default)]
pub struct BenchmarkResults {
    pub pre: BenchmarkResult,
    pub post: BenchmarkResult,
}

impl BenchmarkResults {
    fn keep_cdf(&mut self) {
        self.pre.keep_cdf();
        self.post.keep_cdf();
    }

    fn pick(&mut self, p: Period) -> &mut BenchmarkResult {
        match p {
            Period::PreMigration => &mut self.pre,
            Period::PostMigration => &mut self.post,
        }
    }

    fn record_latency(&mut self, p: Period, value: i64) -> Result<(), ()> {
        if let Some(ref mut samples) = self.pick(p).samples {
            samples.record(value)
        } else {
            Ok(())
        }
    }

    fn record_throughput(&mut self, p: Period, value: f64) {
        self.pick(p).throughputs.push(value)
    }
}

fn driver<I, F>(start: time::Instant,
                config: RuntimeConfig,
                init: I,
                desc: String)
                -> BenchmarkResults
    where I: FnOnce() -> Box<F>,
          F: ?Sized + FnMut(i64, i64) -> (bool, Period)
{
    let mut count = 0usize;
    let mut last_reported = start;
    let report_every = time::Duration::from_millis(200);

    let mut stats = BenchmarkResults::default();
    if config.cdf {
        stats.keep_cdf();
    }

    let mut t_rng = rand::thread_rng();
    let mut v_rng = Rng::from_seed(42);
    let zipf_dist = Zipf::new(1.07).unwrap();
    let uniform_dist = Uniform::new(1.0, config.narticles as f64).unwrap();

    {
        let mut f = init();
        while start.elapsed() < config.runtime {
            let uid = t_rng.gen::<i64>();

            // what article to vote for/retrieve?
            // note that we always *compute* both so that zipf and uniform performance numbers are
            // more directly comparable (zipf takes longer to generate).
            let u = uniform_dist.sample(&mut v_rng) as isize;
            let z = zipf_dist.sample(&mut v_rng) as isize;
            let aid = match config.distribution {
                Distribution::Uniform => u,
                Distribution::Zipf => z,
            };
            let aid = std::cmp::min(aid, config.narticles - 1) as i64;
            assert!(aid > 0);

            let (register, period) = if config.cdf {
                let t = time::Instant::now();
                let (reg, period) = f(uid, aid);
                let t = (dur_to_ns!(t.elapsed()) / 1000) as i64;
                if stats.record_latency(period, t).is_err() {
                    println!("failed to record slow {} ({}μs)", desc, t);
                }
                (reg, period)
            } else {
                f(uid, aid)
            };
            if register {
                count += 1;
            }

            // check if we should report
            if last_reported.elapsed() > report_every {
                let count_per_ns = count as f64 / dur_to_ns!(last_reported.elapsed()) as f64;
                let count_per_s = count_per_ns * NANOS_PER_SEC as f64;

                match period {
                    Period::PreMigration => {
                        println!("{:?} {}: {:.2}",
                                 dur_to_ns!(start.elapsed()),
                                 desc,
                                 count_per_s);
                    }
                    Period::PostMigration => {
                        println!("{:?} {}+: {:.2}",
                                 dur_to_ns!(start.elapsed()),
                                 desc,
                                 count_per_s);
                    }
                }
                stats.record_throughput(period, count_per_s);

                last_reported = time::Instant::now();
                count = 0;
            }
        }
    }

    stats
}

pub fn launch<B: targets::Backend + 'static>(mut target: B,
                                             mut config: RuntimeConfig)
                                             -> (BenchmarkResults, Vec<BenchmarkResults>) {

    // prepopulate
    println!("Connected. Now retrieving putter for prepopulation.");
    let mut putter = target.putter();
    {
        let mut article = putter.article();

        // prepopulate
        println!("Prepopulating with {} articles", config.narticles);
        {
            // let t = putter.transaction().unwrap();
            for i in 0..config.narticles {
                article(i as i64, format!("Article #{}", i));
            }
            // t.commit().unwrap();
        }
        println!("Done with prepopulation");
    }

    // let system settle
    thread::sleep(time::Duration::new(1, 0));
    let start = time::Instant::now();

    // benchmark
    // start putting
    let (np_tx, np_rx): (mpsc::Sender<B::P>, _) = mpsc::channel();
    let mut putter = Some({
        thread::Builder::new().name("put0".to_string()).spawn(move || -> BenchmarkResults {
            let mut vote = putter.vote();
            let mut new_putter = None;
            let mut new_vote = None;
            let init = move || {
                let mut i = 0;
                Box::new(move |uid, aid| -> (bool, Period) {
                    if let Some(migrate_after) = config.migrate_after {
                        if i % 16384 == 0 {
                            if start.elapsed() > migrate_after {
                                // we may have been given a new putter
                                if let Ok(np) = np_rx.try_recv() {
                                    // we have a new putter!
                                    //
                                    // it's unfortunate that we have to use unsafe here...
                                    // hopefully it can go if
                                    // https://github.com/Kimundi/owning-ref-rs/issues/22
                                    // gets resolved. fundamentally, the problem is that the
                                    // compiler doesn't know that we won't overwrite new_putter in
                                    // some subsequent iteration of the loop, which would leave
                                    // new_vote with a dangling pointer to new_putter!
                                    // *we* know that won't happen though, so this is ok
                                    new_putter = Some(np);
                                    let np = new_putter.as_mut().unwrap() as *mut _;
                                    let np: &mut B::P = unsafe { &mut *np };
                                    new_vote = Some(np.vote());
                                }
                            }
                        }
                        i += 1;
                    }

                    if let Some(vote) = new_vote.as_mut() {
                        vote(uid, aid);
                        (true, Period::PostMigration)
                    } else {
                        vote(uid, aid);
                        (true, Period::PreMigration)
                    }
                })
            };
            driver(start, config, init, "PUT".to_string())
        }).unwrap()
    });

    let mut put_stats = None;
    if config.stage {
        println!("Waiting for putter before starting getters");
        match putter.take().unwrap().join() {
            Err(e) => panic!(e),
            Ok(th) => {
                put_stats = Some(th);
            }
        }
        // avoid getters stopping immediately
        config.runtime *= 2;
    }

    // start getters
    println!("Starting {} getters", config.ngetters);
    let (ng_tx, ng_rx): (spmc::Sender<B::G>, _) = spmc::channel();
    let getters = (0..config.ngetters)
        .into_iter()
        .map(|i| (i, target.getter()))
        .map(|(i, getter)| {
            println!("Starting getter #{}", i);
            let ng_rx = ng_rx.clone();
            thread::Builder::new().name(format!("get{}", i)).spawn(move || -> BenchmarkResults {
                let mut get = getter.get();
                let mut new_getter = None;
                let mut new_get = None;
                let init = move || {
                    let mut i = 0;
                    Box::new(move |_, aid| -> (bool, Period) {
                        if let Some(migrate_after) = config.migrate_after {
                            if i % 16384 == 0 {
                                if start.elapsed() > migrate_after {
                                    // we may have been given a new getter
                                    if let Ok(ng) = ng_rx.try_recv() {
                                        // we have a new getter!
                                        // we have to do the same unsafe trick here as for putters
                                        new_getter = Some(ng);
                                        let ng = new_getter.as_mut().unwrap() as *mut _;
                                        let ng: &mut B::G = unsafe { &mut *ng };
                                        new_get = Some(ng.get());
                                    }
                                }
                            }
                            i += 1;
                        }

                        if let Some(get) = new_get.as_mut() {
                            (get(aid).is_ok(), Period::PostMigration)
                        } else {
                            (get(aid).is_ok(), Period::PreMigration)
                        }
                    })
                };
                driver(start, config, init, format!("GET{}", i))
            }).unwrap()
        })
        .collect::<Vec<_>>();
    println!("Started {} getters", getters.len());

    // get ready to perform a migration
    if let Some(migrate_after) = config.migrate_after {
        thread::sleep(migrate_after);
        println!("Starting migration");
        let mig_start = time::Instant::now();
        let (new_put, new_gets) = target.migrate(config.ngetters);
        let mig_duration = dur_to_ns!(mig_start.elapsed()) as f64 / 1_000_000_000.0;
        println!("Migration completed in {:.4}s", mig_duration);
        assert_eq!(new_gets.len(), config.ngetters);
        np_tx.send(new_put).unwrap();
        for ng in new_gets {
            ng_tx.send(ng).unwrap();
        }
        println!("All threads notified of migration completion");
    }

    // clean
    if let Some(putter) = putter {
        // is putter also running?
        match putter.join() {
            Err(e) => panic!(e),
            Ok(th) => {
                assert!(put_stats.is_none());
                put_stats = Some(th);
            }
        }
    }
    let mut get_stats = Vec::with_capacity(getters.len());
    for g in getters {
        match g.join() {
            Err(e) => panic!(e),
            Ok(th) => get_stats.push(th),
        }
    }
    (put_stats.unwrap(), get_stats)
}
