use common::{Writer, Reader, ArticleResult, Period, RuntimeConfig, Distribution};

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

use std::sync;
use std::thread;
use std::time;

use rand;
use rand::Rng as StdRng;
use hdrsample::{Histogram, RecordError};
use hdrsample::iterators::{HistogramIterator, recorded};
use zipf::ZipfDistribution;

pub struct BenchmarkResult {
    throughputs: Vec<f64>,
    samples: Option<(Histogram<u64>, Histogram<u64>)>,
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
        self.samples = Some((
            Histogram::<u64>::new_with_bounds(10, 10000000, 4).unwrap(),
            Histogram::<u64>::new_with_bounds(10, 10000000, 4).unwrap(),
        ));
    }

    pub fn avg_throughput(&self) -> f64 {
        let s: f64 = self.throughputs.iter().sum();
        s / self.throughputs.len() as f64
    }

    pub fn cdf_percentiles(
        &self,
    ) -> Option<
        (
            HistogramIterator<u64, recorded::Iter<u64>>,
            HistogramIterator<u64, recorded::Iter<u64>>,
        ),
    > {
        self.samples
            .as_ref()
            .map(|&(ref r, ref w)| (r.iter_recorded(), w.iter_recorded()))
    }

    #[allow(dead_code)]
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

    fn record_latency(&mut self, read: bool, p: Period, value: u64) -> Result<(), RecordError> {
        if let Some((ref mut r_samples, ref mut w_samples)) = self.pick(p).samples {
            if read {
                r_samples.record(value)
            } else {
                w_samples.record(value)
            }
        } else {
            Ok(())
        }
    }

    fn record_throughput(&mut self, p: Period, value: f64) {
        self.pick(p).throughputs.push(value)
    }
}

fn driver<R, W>(
    mut config: RuntimeConfig,
    mut r: Option<R>,
    mut w: Option<W>,
    start: sync::Arc<sync::Barrier>,
) -> BenchmarkResults
where
    R: Reader,
    W: Writer,
{
    let mut stats = BenchmarkResults::default();
    if config.cdf {
        stats.keep_cdf();
    }

    {
        // random article ids with distribution. we pre-generate these to avoid overhead at
        // runtime. note that we don't use Iterator::cycle, since it buffers by cloning, which
        // means it might also do vector resizing.
        let mut i = 0;
        let randomness: Vec<i64> = {
            let n = 1_000_000 * config.runtime.as_ref().unwrap().as_secs();
            println!(
                "Generating ~{}M random numbers; this'll take a few seconds...",
                n / 1_000_000
            );
            match config.distribution {
                Distribution::Uniform => {
                    let mut u = rand::thread_rng();
                    (0..n)
                        .map(|_| u.gen_range(0, config.narticles) as i64)
                        .collect()
                }
                Distribution::Zipf(e) => {
                    let mut z =
                        ZipfDistribution::new(rand::thread_rng(), config.narticles as usize, e)
                            .unwrap();
                    (0..n)
                        .map(|_| z.gen_range(0, config.narticles) as i64)
                        .collect()
                }
            }
        };

        start.wait();

        let mut read = config.mix.does_read();
        let mut count = 0usize;
        let start = time::Instant::now();
        let mut last_reported = start;
        let report_every = time::Duration::from_millis(200);
        let mut batch: Vec<_> = (0..config.mix.max_batch_size())
            .into_iter()
            .map(|i| (i as i64, i as i64))
            .collect();
        let runtime = config.runtime.unwrap();
        while start.elapsed() < runtime {
            let bs = config.mix.batch_size_for(read);

            // construct ids for the next batch
            // TODO: eliminate uid from read now that they're separate?
            for &mut (_, ref mut aid) in batch.iter_mut().take(bs) {
                *aid = randomness[i];
                i = (i + 1) % randomness.len();
            }

            let (register, period) = if config.cdf {
                let t = time::Instant::now();
                let (reg, period) = if read {
                    do_read(r.as_mut().unwrap(), &mut config, &start, &batch[..bs])
                } else {
                    (true, w.as_mut().unwrap().vote(&batch[..bs]))
                };
                let t = (dur_to_ns!(t.elapsed()) / 1000) as u64;
                if stats.record_latency(read, period, t).is_err() {
                    let desc = if read { "GET" } else { "PUT" };
                    println!("failed to record slow {} ({}μs)", desc, t);
                }
                (reg, period)
            } else if read {
                do_read(r.as_mut().unwrap(), &mut config, &start, &batch[..bs])
            } else {
                (true, w.as_mut().unwrap().vote(&batch[..bs]))
            };
            if register {
                count += bs;
            }

            // check if we should report
            if last_reported.elapsed() > report_every {
                let count_per_ns = count as f64 / dur_to_ns!(last_reported.elapsed()) as f64;
                let count_per_s = count_per_ns * NANOS_PER_SEC as f64;

                let desc = if config.mix.is_mixed() {
                    "MIX"
                } else {
                    if read {
                        "GET"
                    } else {
                        "PUT"
                    }
                };
                match period {
                    Period::PreMigration => {
                        if config.verbose {
                            println!(
                                "{:?} {}: {:.2}",
                                dur_to_ns!(start.elapsed()),
                                desc,
                                count_per_s
                            );
                        }
                    }
                    Period::PostMigration => {
                        if config.verbose {
                            println!(
                                "{:?} {}+: {:.2}",
                                dur_to_ns!(start.elapsed()),
                                desc,
                                count_per_s
                            );
                        }
                    }
                }
                stats.record_throughput(period, count_per_s);

                last_reported = time::Instant::now();
                count = 0;
            }

            if config.mix.is_mixed() {
                read = !read;
            }
        }
    }

    stats
}

pub fn prep_writer<W: Writer>(writer: &mut W, config: &RuntimeConfig) {

    // prepopulate
    if !config.should_reuse() {
        println!("Prepopulating with {} articles", config.narticles);
        let pop_batch_size = 100;
        assert_eq!(config.narticles % pop_batch_size, 0);
        for i in 0..config.narticles / pop_batch_size {
            let reali = pop_batch_size * i;
            writer.make_articles(
                (reali..reali + pop_batch_size).map(|i| (i as i64, format!("Article #{}", i))),
            );
        }
        println!("Done with prepopulation");
    }

    // let system settle
    thread::sleep(time::Duration::new(1, 0));
}

fn do_read<R: Reader>(
    reader: &mut R,
    _: &RuntimeConfig,
    _: &time::Instant,
    ids: &[(i64, i64)],
) -> (bool, Period) {
    match reader.get(ids) {
        (Err(_), period) => (false, period),
        (_, period) => (true, period),
    }
}

pub fn launch<R: Reader, W: Writer>(
    reader: Option<R>,
    mut writer: Option<W>,
    config: RuntimeConfig,
    ready: Option<sync::Arc<sync::Barrier>>,
    start: sync::Arc<sync::Barrier>,
) -> BenchmarkResults {
    if let Some(ref mut writer) = writer {
        prep_writer(writer, &config);
        ready.map(|b| b.wait());

    }
    if config.runtime.is_none() {
        return BenchmarkResults::default();
    }
    driver(config, reader, writer, start)
}

#[allow(dead_code)]
pub fn launch_mix<T>(inner: T, config: RuntimeConfig) -> BenchmarkResults
where
    T: Reader + Writer,
{
    use std::rc::Rc;
    use std::cell::RefCell;
    let inner = Rc::new(RefCell::new(inner));
    launch(
        Some(inner.clone()),
        Some(inner),
        config,
        None,
        sync::Arc::new(sync::Barrier::new(1)),
    )
}

#[allow(dead_code)]
pub struct NullClient;
impl Reader for NullClient {
    fn get(&mut self, _: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        unreachable!()
    }
}
impl Writer for NullClient {
    fn make_articles<I>(&mut self, _: I)
    where
        I: Iterator<Item = (i64, String)>,
        I: ExactSizeIterator,
    {
        unreachable!()
    }

    fn vote(&mut self, _: &[(i64, i64)]) -> Period {
        unreachable!()
    }
}
