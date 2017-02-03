use mysql;
use r2d2;
use r2d2_mysql::MysqlConnectionManager;

use targets::Backend;
use targets::Putter;
use targets::Getter;

type MCM = MysqlConnectionManager;
type PC = r2d2::PooledConnection<MCM>;

pub fn make(dbn: &str, getters: usize) -> r2d2::Pool<MCM> {
    use std::time;
    use mysql::Opts;
    use r2d2_mysql::CreateManager;

    let dbn = format!("mysql://{}", dbn);
    let opts = Opts::from_url(&dbn).unwrap();
    let db = opts.get_db_name().as_ref().unwrap().clone();
    println!("DB: {}", db);

    // Check whether database already exists, or whether we need to create it
    let x = mysql::Pool::new(opts).unwrap();
    if x.prep_exec(format!("USE {}", db), ()).is_ok() {
        x.prep_exec(format!("DROP DATABASE \"{}\"", &db).as_str(), ()).unwrap();
    }
    x.prep_exec(format!("CREATE DATABASE \"{}\"", &db).as_str(), ()).unwrap();
    drop(x);

    // Construct a DB pool connected to the soup database
    let config = r2d2::Config::builder()
        .error_handler(Box::new(r2d2::LoggingErrorHandler))
        .pool_size((getters + 1) as u32 /* putter */)
        .connection_timeout(time::Duration::new(1000, 0))
        .build();

    let pool = r2d2::Pool::new(config, MysqlConnectionManager::new(dbn.as_str()).unwrap()).unwrap();

    let mut conn = pool.get().unwrap();

    // create tables
    conn.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint)",
                   ())
        .unwrap();
    conn.prep_exec("CREATE TABLE vt (u bigint, id bigint)", ()).unwrap();

    // create indices
    conn.prep_exec("CREATE INDEX artid_idx ON art (id)", ()).unwrap();
    conn.prep_exec("CREATE INDEX vtid_idx ON vt (id)", ()).unwrap();

    pool
}

impl Backend for r2d2::Pool<MCM> {
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
        let mut prep = self.prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)")
            .unwrap();
        Box::new(move |id, title| {
            prep.execute(params!{"id" => id, "title" => &title}).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        //let mut pv = self.prepare("INSERT INTO vt (u, id) VALUES (:user, :id)").unwrap();
        let mut pa = self.prepare("UPDATE art SET votes = votes + 1 WHERE id = :id").unwrap();
        Box::new(move |user, id| {
            //pv.execute(params!{"user" => &user, "id" => &id}).unwrap();
            pa.execute(params!{"id" => &id}).unwrap();
        })
    }
}

impl Getter for PC {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        let mut prep = self.prepare("SELECT id, title, votes FROM art WHERE id = :id").unwrap();
        Box::new(move |id| {
            for row in prep.execute(params!{"id" => &id}).unwrap() {
                let mut rr = row.unwrap();
                return Ok(Some((rr.get(0).unwrap(), rr.get(1).unwrap(), rr.get(2).unwrap())));
            }
            Ok(None)
        })
    }
}
