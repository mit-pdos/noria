use memcached;
use memcached::proto::{Operation, ProtoType};
use mysql;
use r2d2;
use r2d2_mysql::MysqlConnectionManager;

pub struct Memcache(memcached::Client);
unsafe impl Send for Memcache {}

use std::ops::{Deref, DerefMut};
impl Deref for Memcache {
    type Target = memcached::Client;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Memcache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

use targets::Backend;
use targets::Putter;
use targets::Getter;

type MCM = MysqlConnectionManager;
type PC = r2d2::PooledConnection<MCM>;

pub fn make(memcached_dbn: &str,
            mysql_dbn: &str,
            getters: usize)
            -> (Vec<Memcache>, r2d2::Pool<MCM>) {
    use std::time;
    use mysql::Opts;
    use r2d2_mysql::CreateManager;

    let memcached_handles = (0..(getters + 1))
        .into_iter()
        .map(|_| {
            Memcache(memcached::Client::connect(&[(&format!("tcp://{}", memcached_dbn), 1)],
                                                ProtoType::Binary)
                .unwrap())
        })
        .collect::<Vec<_>>();

    let mysql_dbn = format!("mysql://{}", mysql_dbn);
    // we need to do this dance to avoid using the DB early (which will crash us if it doesn't
    // exist)
    let db = &mysql_dbn[mysql_dbn.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&mysql_dbn[0..mysql_dbn.rfind("/").unwrap()]).unwrap();

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

    let mysql_pool = r2d2::Pool::new(config,
                                     MysqlConnectionManager::new(mysql_dbn.as_str()).unwrap())
        .unwrap();

    let mut conn = mysql_pool.get().unwrap();

    // allow larger in-memory tables (4 GB)
    conn.prep_exec("SET max_heap_table_size = 4294967296", ()).unwrap();

    // create tables with indices
    conn.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint, \
                    PRIMARY KEY USING HASH (id)) ENGINE = MEMORY",
                   ())
        .unwrap();
    conn.prep_exec("CREATE TABLE vt (u bigint, id bigint, PRIMARY KEY USING HASH (u, id), \
                    KEY id (id)) ENGINE = MEMORY",
                   ())
        .unwrap();

    (memcached_handles, mysql_pool)
}

impl Backend for (Vec<Memcache>, r2d2::Pool<MCM>) {
    type P = (Memcache, PC);
    type G = (Memcache, PC);

    fn getter(&mut self) -> Self::G {
        // return (memcached handle, MySQL connection)
        (self.0.pop().unwrap(), self.1.clone().get().unwrap())
    }

    fn putter(&mut self) -> Self::P {
        (self.0.pop().unwrap(), self.1.clone().get().unwrap())
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for (Memcache, PC) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let mut prep = self.1
            .prepare("INSERT INTO art (id, title) VALUES (:id, :title)")
            .unwrap();
        let ref mut memd = self.0;
        Box::new(move |id, title| {
            prep.execute(params!{"id" => id, "title" => &title}).unwrap();
            drop(memd.set(format!("article_{}_vc", id).as_bytes(),
                          format!("{};{};0", id, title).as_bytes(),
                          0,
                          0));
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        let mut pv = self.1.prepare("INSERT INTO vt (u, id) VALUES (:user, :id)").unwrap();
        let ref mut memd = self.0;
        Box::new(move |user, id| {
            // DB insert
            pv.execute(params!{"user" => user, "id" => id}).unwrap();
            // memcached invalidate: we use a hack with a short (1s) lifetime here because the
            // `memcached` crate does not expose `delete()`.
            drop(memd.set(format!("article_{}_vc", id).as_bytes(), b"0", 1, 0));
        })
    }
}

impl Getter for (Memcache, PC) {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        let mut prep = self.1
            .prepare("SELECT art.id, title, COUNT(vt.u) as votes FROM art, vt
                      WHERE art.id = vt.id AND art.id = :id
                      GROUP BY vt.id, title")
            .unwrap();
        let ref mut memd = self.0;
        Box::new(move |id| {
            let cached = memd.get(format!("article_{}_vc", id).as_bytes());

            let mut handle_miss = |id: i64| -> Result<Option<(i64, String, i64)>, ()> {
                for row in prep.execute(params!{"id" => &id}).unwrap() {
                    let mut rr = row.unwrap();
                    let id = rr.get(0).unwrap();
                    let title = rr.get(1).unwrap();
                    let vc = rr.get(2).unwrap();
                    drop(memd.set(format!("article_{}_vc", id).as_bytes(),
                                  format!("{};{};{}", id, title, vc).as_bytes(),
                                  0,
                                  0));
                    return Ok(Some((id, title, vc)));
                }
                Ok(None)
            };

            match cached {
                Ok(data) => {
                    let s = String::from_utf8_lossy(&data.0[..]);
                    // we may see the temporary "invalidation" write here, so if we do, handle it
                    // as a miss
                    if s == "0" {
                        handle_miss(id)
                    } else {
                        let mut parts = s.split(";");
                        Ok(Some((parts.next().unwrap().parse().unwrap(),
                                 String::from(parts.next().unwrap()),
                                 parts.next().unwrap().parse().unwrap())))
                    }
                }
                Err(_) => handle_miss(id),
            }
        })
    }
}
