use mysql::{self, OptsBuilder};

use common::{Writer, Reader, ArticleResult, Period};

pub struct W<'a> {
    a_prep: mysql::conn::Stmt<'a>,
    conn: mysql::PooledConn,
}

pub fn setup(addr: &str, write: bool) -> mysql::Pool {
    use mysql::Opts;

    let addr = format!("mysql://{}", addr);
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();

    if write {
        // clear the db (note that we strip of /db so we get default)
        let mut opts = OptsBuilder::from_opts(opts.clone());
        opts.db_name(Some(db));
        opts.init(vec!["SET max_heap_table_size = 4294967296;"]);
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

    // now we connect for real
    let mut opts = OptsBuilder::from_opts(opts);
    opts.db_name(Some(db));
    opts.init(vec!["SET max_heap_table_size = 4294967296;"]);
    mysql::Pool::new_manual(1, 4, opts).unwrap()
}

pub fn make_writer<'a>(pool: &'a mysql::Pool) -> W<'a> {
    W {
        a_prep: pool.prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)")
            .unwrap(),
        conn: pool.get_conn().unwrap(),
    }
}

impl<'a> Writer for W<'a> {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        self.a_prep
            .execute(params!{"id" => article_id, "title" => &title})
            .unwrap();
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let values = ids.iter()
            .map(|&(ref u, ref a)| format!("({}, {})", u, a))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("INSERT INTO vt (u, id) VALUES {}; ", values);

        self.conn.query(query).unwrap();

        for &(_, ref a) in ids {
            self.conn.prep_exec("UPDATE art SET votes = votes + 1 WHERE id = ?;", vec![a]).unwrap();
        }

        Period::PreMigration
    }
}

pub fn make_reader(pool: &mysql::Pool) -> mysql::PooledConn {
    pool.get_conn().unwrap()
}

impl Reader for mysql::PooledConn {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let query = ids.iter()
            .map(|&(_, ref article_id)| {
                     format!("SELECT id, title, votes FROM art WHERE id = {}; ",
                             article_id)
                 })
            .collect::<Vec<_>>()
            .join("");

        let mut res = Vec::new();
        let mut qresult = self.query(query).unwrap();
        while qresult.more_results_exists() {
            for row in qresult.by_ref() {
                let mut rr = row.unwrap();
                res.push(ArticleResult::Article {
                             id: rr.get(0).unwrap(),
                             title: rr.get(1).unwrap(),
                             votes: rr.get(2).unwrap(),
                         });
            }
        }
        assert_eq!(res.len(), ids.len());

        (Ok(res), Period::PreMigration)
    }
}
