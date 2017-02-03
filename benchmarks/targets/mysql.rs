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
    // we need to do this dance to avoid using the DB early (which will crash us if it doesn't
    // exist)
    let db = &dbn[dbn.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&dbn[0..dbn.rfind("/").unwrap()]).unwrap();

    // Check whether database already exists, or whether we need to create it
    let mut x = mysql::Pool::new(opts).unwrap().get_conn().unwrap();
    if x.query(format!("USE {}", db)).is_ok() {
        x.query(format!("DROP DATABASE {}", &db).as_str()).unwrap();
    }
    x.query(format!("CREATE DATABASE {}", &db).as_str()).unwrap();
    drop(x);

    // Construct a DB pool connected to the soup database
    let config = r2d2::Config::builder()
        .error_handler(Box::new(r2d2::LoggingErrorHandler))
        .pool_size((getters + 2) as u32 /* putter */)
        .connection_timeout(time::Duration::new(1000, 0))
        .build();

    let pool = r2d2::Pool::new(config, MysqlConnectionManager::new(dbn.as_str()).unwrap()).unwrap();

    let mut conn = pool.get().unwrap();

    // allow larger in-memory tables (4 GB)
    conn.prep_exec("SET max_heap_table_size = 4294967296", ()).unwrap();

    // create tables with indices
    conn.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint, INDEX USING \
                    HASH(id)) ENGINE = MEMORY",
                   ())
        .unwrap();
    conn.prep_exec("CREATE TABLE vt (u bigint, id bigint, INDEX USING HASH(id)) ENGINE = MEMORY",
                   ())
        .unwrap();

    pool
}

impl Backend for r2d2::Pool<MCM> {
    // need two connections here because the "mysql" crate only allows one outstanding prepared
    // statement per connection.
    type P = (PC, PC);
    type G = PC;

    fn getter(&mut self) -> Self::G {
        self.clone().get().unwrap()
    }

    fn putter(&mut self) -> Self::P {
        (self.clone().get().unwrap(), self.clone().get().unwrap())
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for (PC, PC) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let mut prep = self.0
            .prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)")
            .unwrap();
        Box::new(move |id, title| {
            prep.execute(params!{"id" => id, "title" => &title}).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        let mut pv = self.0.prepare("INSERT INTO vt (u, id) VALUES (:user, :id)").unwrap();
        let mut pa = self.1.prepare("UPDATE art SET votes = votes + 1 WHERE id = :id").unwrap();
        Box::new(move |user, id| {
            pv.execute(params!{"user" => &user, "id" => &id}).unwrap();
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
