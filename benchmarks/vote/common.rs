use std::sync::mpsc;
use std::thread;
use std::time;

macro_rules! dur_to_ns {
    ($d:expr) => {{
        const NANOS_PER_SEC: u64 = 1_000_000_000;
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

pub trait MigrationHandle: Send {
    fn execute(&mut self);
}

impl MigrationHandle for () {
    fn execute(&mut self) {
        unimplemented!()
    }
}

pub trait Writer {
    type Migrator: MigrationHandle + 'static;

    fn make_articles<I>(&mut self, articles: I)
        where I: Iterator<Item = (i64, String)>,
              I: ExactSizeIterator;

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period;

    fn prepare_migration(&mut self) -> Self::Migrator {
        unimplemented!()
    }

    fn migrate(&mut self) -> mpsc::Receiver<()> {
        use std::time;
        let (tx, rx) = mpsc::sync_channel(0);
        println!("Starting migration");
        let mig_start = time::Instant::now();
        let mut handle = self.prepare_migration();
        thread::Builder::new()
            .name("migrator".to_string())
            .spawn(move || {
                       handle.execute();
                       let mig_duration = dur_to_ns!(mig_start.elapsed()) as f64 / 1_000_000_000.0;
                       println!("Migration completed in {:.4}s", mig_duration);
                       drop(tx);
                   })
            .unwrap();
        rx
    }
}

pub enum ArticleResult {
    Article { id: i64, title: String, votes: i64 },
    NoSuchArticle,
}

#[derive(Clone, Copy)]
pub enum Period {
    PreMigration,
    PostMigration,
}

pub trait Reader {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period);
}

#[derive(Clone, Copy)]
pub enum Distribution {
    Uniform,
    Zipf(f64),
}

use std::str::FromStr;
impl FromStr for Distribution {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "uniform" {
            Ok(Distribution::Uniform)
        } else if s.starts_with("zipf:") {
            let s = s.trim_left_matches("zipf:");
            str::parse::<f64>(s)
                .map(|exp| Distribution::Zipf(exp))
                .map_err(|e| {
                             use std::error::Error;
                             e.description().to_string()
                         })
        } else {
            Err(format!("unknown distribution '{}'", s))
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeConfig {
    pub narticles: isize,
    pub runtime: Option<time::Duration>,
    pub distribution: Distribution,
    pub cdf: bool,
    pub batch_size: usize,
    pub migrate_after: Option<time::Duration>,
    pub reuse: bool,
    pub verbose: bool,
}

impl RuntimeConfig {
    pub fn new(narticles: isize, runtime: Option<time::Duration>) -> Self {
        RuntimeConfig {
            narticles: narticles,
            runtime: runtime,
            distribution: Distribution::Uniform,
            batch_size: 1,
            cdf: true,
            migrate_after: None,
            reuse: false,
            verbose: false,
        }
    }

    #[allow(dead_code)]
    pub fn set_reuse(&mut self, reuse: bool) {
        self.reuse = reuse;
    }

    pub fn should_reuse(&self) -> bool {
        self.reuse
    }

    #[allow(dead_code)]
    pub fn use_distribution(&mut self, d: Distribution) {
        self.distribution = d;
    }

    #[allow(dead_code)]
    pub fn use_batching(&mut self, batch_size: usize) {
        if batch_size == 0 {
            self.batch_size = 1;
        } else {
            self.batch_size = batch_size;
        }
    }

    #[allow(dead_code)]
    pub fn set_verbose(&mut self, yes: bool) {
        self.verbose = yes;
    }

    pub fn produce_cdf(&mut self, yes: bool) {
        self.cdf = yes;
    }

    pub fn perform_migration_at(&mut self, t: time::Duration) {
        self.migrate_after = Some(t);
    }
}
