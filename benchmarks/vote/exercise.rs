use common::{Writer, ArticleResult, Reader, Period};

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

use std::sync::mpsc;
use std::thread;
use std::time;

use rand;
use rand::Rng as StdRng;
use hdrsample::Histogram;
use hdrsample::iterators::{HistogramIterator, recorded};

#[derive(Clone, Copy)]
pub struct RuntimeConfig {
    narticles: isize,
    runtime: time::Duration,
    cdf: bool,
    migrate_after: Option<time::Duration>,
}

impl RuntimeConfig {
    pub fn new(narticles: isize, runtime: time::Duration) -> Self {
        RuntimeConfig {
            narticles: narticles,
            runtime: runtime,
            cdf: true,
            migrate_after: None,
        }
    }

    pub fn produce_cdf(&mut self, yes: bool) {
        self.cdf = yes;
    }

    pub fn perform_migration_at(&mut self, t: time::Duration) {
        self.migrate_after = Some(t);
    }
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

    pub fn cdf_percentiles(&self) -> Option<HistogramIterator<u64, recorded::Iter<u64>>> {
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
                desc: &str)
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

    {
        let mut f = init();
        while start.elapsed() < config.runtime {
            let uid: i64 = t_rng.gen();

            // what article to vote for/retrieve?
            let aid = t_rng.gen_range(0, config.narticles) as i64;

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

pub fn launch_writer<W: Writer + 'static>(mut writer: W,
                                          mut config: RuntimeConfig,
                                          ready: Option<mpsc::SyncSender<()>>)
                                          -> BenchmarkResults {

    // prepopulate
    println!("Prepopulating with {} articles", config.narticles);
    {
        // let t = putter.transaction().unwrap();
        for i in 0..config.narticles {
            writer.make_article(i as i64, format!("Article #{}", i));
        }
        // t.commit().unwrap();
    }
    println!("Done with prepopulation");

    // let system settle
    thread::sleep(time::Duration::new(1, 0));
    drop(ready);
    let start = time::Instant::now();

    let mut post = false;
    let mut migrate_done = None;
    let init = move || {
        Box::new(move |uid, aid| -> (bool, Period) {
            if let Some(migrate_after) = config.migrate_after {
                if start.elapsed() > migrate_after {
                    migrate_done = Some(writer.migrate());
                    config.migrate_after.take(); // so we don't migrate again
                }
            }

            if migrate_done.is_some() {
                match migrate_done.as_mut().unwrap().try_recv() {
                    Err(mpsc::TryRecvError::Empty) => {}
                    _ => {
                        migrate_done = None;
                        post = true;
                    }
                }
            }

            writer.vote(uid, aid);
            if post {
                (true, Period::PostMigration)
            } else {
                (true, Period::PreMigration)
            }
        })
    };

    driver(start, config, init, "PUT")
}

pub fn launch_reader<R: Reader + 'static>(mut reader: R,
                                          config: RuntimeConfig)
                                          -> BenchmarkResults {

    println!("Starting reader");
    let init = move || {
        Box::new(move |_, aid| -> (bool, Period) {
            match reader.get(aid) {
                (ArticleResult::Error, period) => (false, period),
                (_, period) => (true, period),
            }
        })
    };

    driver(time::Instant::now(), config, init, "GET")
}
