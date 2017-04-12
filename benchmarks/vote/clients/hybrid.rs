use memcached;
use memcached::proto::{Operation, ProtoType};
use mysql::{self, OptsBuilder};

use common::{Writer, Reader, ArticleResult, Period};

pub struct Pool {
    sql: mysql::Pool,
    mc: Option<memcached::Client>,
}

pub struct W<'a> {
    a_prep: mysql::conn::Stmt<'a>,
    v_prep: mysql::conn::Stmt<'a>,
    conn: mysql::PooledConn,
    mc: memcached::Client,
}

pub fn setup(mysql_dbn: &str, memcached_dbn: &str, write: bool) -> Pool {
    use mysql::Opts;

    let mc = memcached::Client::connect(&[(&format!("tcp://{}", memcached_dbn), 1)],
                                        ProtoType::Binary)
            .unwrap();

    let addr = format!("mysql://{}", mysql_dbn);
    if write {
        // clear the db (note that we strip of /db so we get default)
        let db = &addr[addr.rfind("/").unwrap() + 1..];
        let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();
        let mut opts = OptsBuilder::from_opts(opts);
        opts.db_name(Some(db));
        opts.init(vec!["SET max_heap_table_size = 4294967296;"]);

        let pool = mysql::Pool::new(opts).unwrap();
        let mut conn = pool.get_conn().unwrap();
        if conn.query(format!("USE {}", db)).is_ok() {
            conn.query(format!("DROP DATABASE {}", &db).as_str()).unwrap();
        }
        conn.query(format!("CREATE DATABASE {}", &db).as_str()).unwrap();

        // allow larger in-memory tables (4 GB)

        // create tables with indices
        pool.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint, \
                        PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
        pool.prep_exec("CREATE TABLE vt (u bigint, id bigint, KEY id (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
    }

    Pool {
        sql: mysql::Pool::new(Opts::from_url(&addr).unwrap()).unwrap(),
        mc: Some(mc),
    }
}

pub fn make_writer<'a>(pool: &'a mut Pool) -> W<'a> {
    let mc = pool.mc.take().unwrap();
    let pool = &pool.sql;
    W {
        a_prep: pool.prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)").unwrap(),
        v_prep: pool.prepare("INSERT INTO vt (u, id) VALUES (:user, :id)").unwrap(),
        mc: mc,
    }
}

impl<'a> Writer for W<'a> {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        self.a_prep.execute(params!{"id" => article_id, "title" => &title}).unwrap();
        drop(self.mc.set(format!("article_{}_vc", article_id).as_bytes(),
                         format!("{};{};0", article_id, title).as_bytes(),
                         0,
                         0));
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        for &(user_id, article_id) in ids {
            self.v_prep.execute(params!{"user" => &user_id, "id" => &article_id}).unwrap();
            drop(self.mc.delete(format!("article_{}_vc", article_id).as_bytes()));
        }
        Period::PreMigration
    }
}

pub struct R<'a> {
    prep: mysql::conn::Stmt<'a>,
    mc: memcached::Client,
}

pub fn make_reader<'a>(pool: &'a mut Pool) -> R<'a> {
    let mc = pool.mc.take().unwrap();
    let pool = &pool.sql;
    R {
        prep: pool.prepare("SELECT art.id, title, COUNT(vt.u) as votes FROM art, vt
                      WHERE art.id = vt.id AND art.id = :id
                      GROUP BY vt.id, title")
            .unwrap(),
        mc: mc,
    }
}

impl<'a> Reader for R<'a> {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let res = ids.iter()
            .map(|&(_, article_id)| {
                // rustfmt
                match self.mc.get(format!("article_{}_vc", article_id).as_bytes()) {
                    Ok(data) => {
                        let s = String::from_utf8_lossy(&data.0[..]);
                        let mut parts = s.split(";");
                        ArticleResult::Article {
                            id: parts.next()
                                .unwrap()
                                .parse()
                                .unwrap(),
                            title: String::from(parts.next().unwrap()),
                            votes: parts.next()
                                .unwrap()
                                .parse()
                                .unwrap(),
                        }
                    }
                    Err(_) => {
                        for row in self.prep.execute(params!{"id" => &article_id}).unwrap() {
                            let mut rr = row.unwrap();
                            let id = rr.get(0).unwrap();
                            let title = rr.get(1).unwrap();
                            let vc = rr.get(2).unwrap();
                            drop(self.mc.set(format!("article_{}_vc", id).as_bytes(),
                                             format!("{};{};{}", id, title, vc).as_bytes(),
                                             0,
                                             0));
                            return ArticleResult::Article {
                                       id: id,
                                       title: title,
                                       votes: vc,
                                   };
                        }
                        ArticleResult::NoSuchArticle
                    }
                }
            })
            .collect();

        (Ok(res), Period::PreMigration)
    }
}
