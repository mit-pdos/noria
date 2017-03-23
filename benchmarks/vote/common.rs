use std::sync::mpsc;
use std::thread;

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

    fn make_article(&mut self, article_id: i64, title: String);
    fn vote(&mut self, user_id: i64, article_id: i64) -> Period;

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
    Error,
}

#[derive(Clone, Copy)]
pub enum Period {
    PreMigration,
    PostMigration,
}

pub trait Reader {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period);
}
