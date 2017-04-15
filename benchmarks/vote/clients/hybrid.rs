use memcached;
use memcached::proto::{Operation, MultiOperation, ProtoType};
use mysql::{self, OptsBuilder};

use common::{Writer, Reader, ArticleResult, Period, RuntimeConfig};

pub struct Pool {
    sql: mysql::Pool,
    mc: Option<memcached::Client>,
}

pub struct W {
    conn: mysql::PooledConn,
    mc: memcached::Client,
}

pub fn setup(mysql_dbn: &str, memcached_dbn: &str, write: bool, config: &RuntimeConfig) -> Pool {
    use mysql::Opts;

    let mc = memcached::Client::connect(&[(&format!("tcp://{}", memcached_dbn), 1)],
                                        ProtoType::Binary)
            .unwrap();

    let addr = format!("mysql://{}", mysql_dbn);
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();

    if write && !config.should_reuse() {
        // clear the db (note that we strip of /db so we get default)
        let mut opts = OptsBuilder::from_opts(opts.clone());
        opts.db_name(Some(db));
        // allow larger in-memory tables (4 GB)
        opts.init(vec!["SET max_heap_table_size = 4294967296;",
                       "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"]);

        let pool = mysql::Pool::new_manual(1, 4, opts).unwrap();
        let mut conn = pool.get_conn().unwrap();
        if conn.query(format!("USE {}", db)).is_ok() {
            conn.query(format!("DROP DATABASE {}", &db).as_str())
                .unwrap();
        }
        conn.query(format!("CREATE DATABASE {}", &db).as_str())
            .unwrap();
        conn.query(format!("USE {}", db)).unwrap();

        drop(conn);

        // create tables with indices
        pool.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint, \
                        PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
        pool.prep_exec("CREATE TABLE vt (u bigint, id bigint, KEY id (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
    }

    let mut opts = OptsBuilder::from_opts(opts.clone());
    opts.db_name(Some(db));
    // allow larger in-memory tables (4 GB)
    opts.init(vec!["SET max_heap_table_size = 4294967296;",
                   "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"]);

    Pool {
        sql: mysql::Pool::new_manual(1, 4, opts).unwrap(),
        mc: Some(mc),
    }
}

pub fn make_writer(pool: &mut Pool) -> W {
    let mc = pool.mc.take().unwrap();
    let pool = &pool.sql;
    W {
        conn: pool.get_conn().unwrap(),
        mc: mc,
    }
}

impl Writer for W {
    type Migrator = ();

    fn make_articles<I>(&mut self, articles: I)
        where I: ExactSizeIterator,
              I: Iterator<Item = (i64, String)>
    {
        let articles: Vec<_> = articles
            .map(|(article_id, title)| {
                     let init = format!("{};{};0", article_id, title);
                     (article_id, title, format!("article_{}_vc", article_id), init)
                 })
            .collect();

        {
            let mut vals = Vec::with_capacity(articles.len());
            let args: Vec<_> = articles
                .iter()
                .flat_map(|&(ref aid, ref title, _, _)| {
                              vals.push("(?, ?, 0)");
                              vec![aid as &_, title as &_]
                          })
                .collect();
            let vals = vals.join(", ");
            self.conn
                .prep_exec(format!("INSERT INTO art (id, title, votes) VALUES {}", vals),
                           &args[..])
                .unwrap();
        }

        use std::collections::BTreeMap;
        let mut m = BTreeMap::new();
        for &(_, _, ref key, ref init) in &articles {
            m.insert(key.as_bytes(), (init.as_bytes(), 0, 0));
        }
        self.mc.set_multi(m).unwrap();
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        use memcached::proto::MultiOperation;

        let values = ids.iter()
            .map(|&(ref u, ref a)| format!("({}, {})", u, a))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("INSERT INTO vt (u, id) VALUES {};", values);

        self.conn.query(query).unwrap();

        let keys: Vec<_> = ids.iter()
            .map(|&(_, a)| format!("article_{}_vc", a))
            .collect();
        drop(self.mc
                 .delete_multi(keys.iter()
                                   .map(String::as_bytes)
                                   .collect::<Vec<_>>()
                                   .as_slice()));

        Period::PreMigration
    }
}

pub struct R {
    conn: mysql::PooledConn,
    mc: memcached::Client,
}

pub fn make_reader(pool: &mut Pool) -> R {
    let mc = pool.mc.take().unwrap();
    let pool = &pool.sql;
    R {
        conn: pool.get_conn().unwrap(),
        mc: mc,
    }
}

impl Reader for R {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        use memcached::proto::MultiOperation;
        use std::str;

        let keys: Vec<_> = ids.iter()
            .map(|&(_, ref article_id)| format!("article_{}_vc", article_id))
            .collect();
        let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();

        let vals = match self.mc.get_multi(&keys[..]) {
            Err(_) => return (Err(()), Period::PreMigration),
            Ok(v) => v,
        };

        let mut res = Vec::new();
        let hits: Vec<_> = keys.iter().filter(|k| vals.contains_key(**k)).collect();
        for k in hits {
            let s = String::from_utf8_lossy(vals.get(*k).unwrap().0.as_slice());
            let mut parts = s.split(";");
            res.push(ArticleResult::Article {
                         id: parts.next().unwrap().parse().unwrap(),
                         title: String::from(parts.next().unwrap()),
                         votes: parts.next().unwrap().parse().unwrap(),
                     });
        }

        let missed: Vec<_> = keys.iter()
            .filter(|k| !vals.contains_key(**k))
            .collect();
        if !missed.is_empty() {
            // missed, we must go to the database
            let qstring = missed
                .into_iter()
                .map(|k| {
                         format!("SELECT art.id, title, COUNT(vt.u) as votes FROM art, vt
                      WHERE art.id = vt.id AND art.id = {}
                      GROUP BY vt.id, title",
                                 str::from_utf8(&k[8..k.len() - 3]).unwrap())
                     })
                .collect::<Vec<_>>()
                .join(" UNION ");
            for row in self.conn.query(qstring).unwrap() {
                let mut rr = row.unwrap();
                let id = rr.get(0).unwrap();
                let title = rr.get(1).unwrap();
                let vc = rr.get(2).unwrap();
                drop(self.mc
                         .set(format!("article_{}_vc", id).as_bytes(),
                              format!("{};{};{}", id, title, vc).as_bytes(),
                              0,
                              0));
                res.push(ArticleResult::Article {
                             id: id,
                             title: title,
                             votes: vc,
                         });
            }
        }

        (Ok(res), Period::PreMigration)
    }
}
