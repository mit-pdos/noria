use targets;
use targets::{Putter, Getter};

use std::thread;
use std::time;
use std;

use rand;
use rand::Rng as StdRng;
use randomkit::{Rng, Sample};
use randomkit::dist::{Uniform, Zipf};
use hdrsample::Histogram;

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
        }
    }

    pub fn put_then_get(&mut self) {
        self.stage = true;
    }

    pub fn produce_cdf(&mut self, yes: bool) {
        self.cdf = yes;
    }

    pub fn set_distribution(&mut self, d: Distribution) {
        self.distribution = d;
    }
}

pub fn launch<B: targets::Backend + 'static>(mut target: B,
                                             mut config: RuntimeConfig)
                                             -> (Vec<f64>, Vec<Vec<f64>>) {

    // prepopulate
    println!("Connected. Now retrieving putter for prepopulation.");
    let mut putter = Box::new(target.putter());
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
    let mut putter = Some({
        thread::spawn(move || -> Vec<f64> {
            let mut count = 0usize;
            let mut samples = Histogram::<u64>::new_with_bounds(1, 100000, 3).unwrap();
            let mut last_reported = start;
            let mut throughputs = Vec::new();

            let mut t_rng = rand::thread_rng();
            let mut v_rng = Rng::from_seed(42);
            let zipf_dist = Zipf::new(1.07).unwrap();

            {
                let mut vote = putter.vote();
                while start.elapsed() < config.runtime {
                    let uniform_dist = Uniform::new(1.0, config.narticles as f64).unwrap();
                    let vote_user = t_rng.gen::<i64>();
                    let vote_rnd_id = match config.distribution {
                        Distribution::Uniform => uniform_dist.sample(&mut v_rng) as isize,
                        Distribution::Zipf => zipf_dist.sample(&mut v_rng) as isize,
                    };
                    let vote_rnd_id = std::cmp::min(vote_rnd_id, config.narticles - 1) as i64;
                    assert!(vote_rnd_id > 0);

                    if config.cdf {
                        let t = time::Instant::now();
                        vote(vote_user, vote_rnd_id);
                        let t = (dur_to_ns!(t.elapsed()) / 1000) as i64;
                        if samples.record(t).is_err() {
                            println!("failed to record slow put ({}ns)", t);
                        }
                    } else {
                        vote(vote_user, vote_rnd_id);
                    }
                    count += 1;

                    // check if we should report
                    if last_reported.elapsed() > time::Duration::from_secs(1) {
                        let ts = last_reported.elapsed();
                        let throughput = count as f64 /
                                         (ts.as_secs() as f64 +
                                          ts.subsec_nanos() as f64 / 1_000_000_000f64);
                        println!("{:?} PUT: {:.2}", dur_to_ns!(start.elapsed()), throughput);
                        throughputs.push(throughput);

                        last_reported = time::Instant::now();
                        count = 0;
                    }
                }
            }

            if config.cdf {
                for (v, p, _, _) in samples.iter_percentiles(1) {
                    println!("percentile PUT {:.2} {:.2}", v, p);
                }
            }
            throughputs
        })
    });

    let mut put_throughput = Vec::new();
    if config.stage {
        println!("Waiting for putter before starting getters");
        match putter.take().unwrap().join() {
            Err(e) => panic!(e),
            Ok(th) => {
                put_throughput.extend(th);
            }
        }
        // avoid getters stopping immediately
        config.runtime *= 2;
    }

    // start getters
    println!("Starting {} getters", config.ngetters);
    let getters = (0..config.ngetters)
        .into_iter()
        .map(|i| (i, Box::new(target.getter())))
        .map(|(i, g)| {
            println!("Starting getter #{}", i);
            thread::spawn(move || -> Vec<f64> {
                let mut count = 0 as u64;
                let mut samples = Histogram::<u64>::new_with_bounds(1, 100000, 3).unwrap();
                let mut last_reported = start;
                let mut throughputs = Vec::new();

                let mut v_rng = Rng::from_seed(42);

                let zipf_dist = Zipf::new(1.07).unwrap();
                let uniform_dist = Uniform::new(1.0, config.narticles as f64).unwrap();

                {
                    let mut get = g.get();
                    while start.elapsed() < config.runtime {
                        // what article to vote for?
                        let id = match config.distribution {
                            Distribution::Uniform => uniform_dist.sample(&mut v_rng) as isize,
                            Distribution::Zipf => zipf_dist.sample(&mut v_rng) as isize,
                        };
                        let id = std::cmp::min(id, config.narticles - 1) as i64;

                        let t = time::Instant::now();
                        if let Some(..) = get(id) {
                            if config.cdf {
                                samples += (dur_to_ns!(t.elapsed()) / 1000) as i64;
                            }
                            count += 1;
                        }

                        if last_reported.elapsed() > time::Duration::from_secs(1) {
                            let ts = last_reported.elapsed();
                            let throughput = count as f64 /
                                             (ts.as_secs() as f64 +
                                              ts.subsec_nanos() as f64 / 1_000_000_000f64);
                            println!("{:?} GET{}: {:.2}",
                                     dur_to_ns!(start.elapsed()),
                                     i,
                                     throughput);
                            throughputs.push(throughput);

                            last_reported = time::Instant::now();
                            count = 0;
                        }
                    }
                }

                if config.cdf {
                    for (v, p, _, _) in samples.iter_percentiles(1) {
                        println!("percentile GET{} {:.2} {:.2}", i, v, p);
                    }
                }
                throughputs
            })
        })
        .collect::<Vec<_>>();
    println!("Started {} getters", getters.len());

    // clean
    if let Some(putter) = putter {
        // is putter also running?
        match putter.join() {
            Err(e) => panic!(e),
            Ok(th) => {
                put_throughput.extend(th);
            }
        }
    }
    let mut get_throughputs = Vec::new();
    for g in getters {
        match g.join() {
            Err(e) => panic!(e),
            Ok(th) => get_throughputs.push(th),
        }
    }
    (put_throughput, get_throughputs)
}
