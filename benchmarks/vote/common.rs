use std::sync::mpsc;
use std::thread;
use std::time;

#[allow(dead_code)]
pub const NANOS_PER_SEC: u64 = 1_000_000_000;
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

use std::rc::Rc;
use std::cell::RefCell;
impl<T> Writer for Rc<RefCell<T>>
    where T: Writer
{
    type Migrator = T::Migrator;
    fn make_articles<I>(&mut self, articles: I)
        where I: Iterator<Item = (i64, String)>,
              I: ExactSizeIterator
    {
        self.borrow_mut().make_articles(articles)
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        self.borrow_mut().vote(ids)
    }

    fn prepare_migration(&mut self) -> Self::Migrator {
        self.borrow_mut().prepare_migration()
    }

    fn migrate(&mut self) -> mpsc::Receiver<()> {
        self.borrow_mut().migrate()
    }
}
impl<T> Reader for Rc<RefCell<T>>
    where T: Reader
{
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        self.borrow_mut().get(ids)
    }
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
pub enum Mix {
    Write(usize),
    Read(usize),
    #[allow(dead_code)]
    RW(usize, usize),
}

impl Mix {
    pub fn batch_size_for(&self, read: bool) -> usize {
        if read {
            self.read_size().unwrap()
        } else {
            self.write_size().unwrap()
        }
    }

    pub fn read_size(&self) -> Option<usize> {
        match *self {
            Mix::Read(bs) => Some(bs),
            Mix::RW(bs, _) => Some(bs),
            _ => None,
        }
    }

    pub fn write_size(&self) -> Option<usize> {
        match *self {
            Mix::Write(bs) => Some(bs),
            Mix::RW(_, bs) => Some(bs),
            _ => None,
        }
    }

    pub fn max_batch_size(&self) -> usize {
        match *self {
            Mix::Write(bs) => bs,
            Mix::Read(bs) => bs,
            Mix::RW(bs, _) => bs,
        }
    }

    pub fn is_mixed(&self) -> bool {
        if let Mix::RW(..) = *self { true } else { false }
    }

    pub fn does_read(&self) -> bool {
        if let Mix::Write(..) = *self {
            false
        } else {
            true
        }
    }

    #[allow(dead_code)]
    pub fn does_write(&self) -> bool {
        if let Mix::Read(..) = *self {
            false
        } else {
            true
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeConfig {
    pub narticles: isize,
    pub runtime: Option<time::Duration>,
    pub distribution: Distribution,
    pub cdf: bool,
    pub mix: Mix,
    pub migrate_after: Option<time::Duration>,
    pub reuse: bool,
    pub verbose: bool,
}

impl RuntimeConfig {
    pub fn new(narticles: isize, mix: Mix, runtime: Option<time::Duration>) -> Self {
        RuntimeConfig {
            narticles: narticles,
            runtime: runtime,
            distribution: Distribution::Uniform,
            mix: mix,
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
    pub fn set_verbose(&mut self, yes: bool) {
        self.verbose = yes;
    }

    pub fn produce_cdf(&mut self, yes: bool) {
        self.cdf = yes;
    }

    #[allow(dead_code)]
    pub fn perform_migration_at(&mut self, t: time::Duration) {
        self.migrate_after = Some(t);
    }
}
