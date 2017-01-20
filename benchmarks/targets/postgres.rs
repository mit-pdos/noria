use r2d2;
use postgres;
use r2d2_postgres::{SslMode, PostgresConnectionManager};

use targets::Backend;
use targets::Putter;
use targets::Getter;

type PCM = PostgresConnectionManager;
type PC = r2d2::PooledConnection<PCM>;

pub fn make(dbn: &str, getters: usize) -> r2d2::Pool<PCM> {
    use std::time;
    use postgres::IntoConnectParams;

    let dbn = format!("postgresql://{}", dbn);
    let params = dbn.into_connect_params().unwrap();

    // Check whether database already exists, or whether we need to create it
    let mut check_new_params = params.clone();
    check_new_params.database = Some(String::from("postgres"));
    let db = params.database.clone().unwrap_or_else(|| String::from("soup_bench"));
    let x = postgres::Connection::connect(check_new_params, postgres::SslMode::None).unwrap();
    if x.execute("SELECT datname FROM pg_database WHERE datname=$1", &[&db]).unwrap() != 0 {
        x.execute(format!("DROP DATABASE \"{}\"", &db).as_str(), &[]).unwrap();
    }
    x.execute(format!("CREATE DATABASE \"{}\"", &db).as_str(), &[]).unwrap();
    x.finish().unwrap();

    // Construct a DB pool connected to the soup database
    let config = r2d2::Config::builder()
        .error_handler(Box::new(r2d2::LoggingErrorHandler))
        .pool_size((getters + 1) as u32 /* putter */)
        .connection_timeout(time::Duration::new(1000, 0))
        .build();

    let pool = r2d2::Pool::new(config,
                               PostgresConnectionManager::new(params, SslMode::None).unwrap())
        .unwrap();

    let conn = pool.get().unwrap();

    // create tables
    conn.execute("CREATE TABLE art (id bigint, title varchar(255), votes bigint)",
                 &[])
        .unwrap();
    conn.execute("CREATE TABLE vt (u bigint, id bigint)", &[]).unwrap();

    // create indices
    conn.execute("CREATE INDEX ON art (id)", &[]).unwrap();
    conn.execute("CREATE INDEX ON vt (id)", &[]).unwrap();

    pool
}

impl Backend for r2d2::Pool<PCM> {
    type P = PC;
    type G = PC;

    fn getter(&mut self) -> Self::G {
        self.clone().get().unwrap()
    }

    fn putter(&mut self) -> Self::P {
        self.clone().get().unwrap()
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for PC {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let prep = self.prepare("INSERT INTO art (id, title, votes) VALUES ($1, $2, 0)").unwrap();
        Box::new(move |id, title| { prep.execute(&[&id, &title]).unwrap(); })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        let pv = self.prepare("INSERT INTO vt (u, id) VALUES ($1, $2)").unwrap();
        let pa = self.prepare("UPDATE art SET votes = votes + 1 WHERE id = $1").unwrap();
        Box::new(move |user, id| {
            pv.execute(&[&user, &id]).unwrap();
            pa.execute(&[&id]).unwrap();
        })
    }
}

impl Getter for PC {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        let prep = self.prepare("SELECT id, title, votes FROM art WHERE id = $1").unwrap();
        Box::new(move |id| {
            for row in prep.query(&[&id]).unwrap().iter() {
                return Some((row.get(0), row.get(1), row.get(2)));
            }
            None
        })
    }
}
