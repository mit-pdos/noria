use mysql;

use common::{Writer, Reader, ArticleResult, Period};

pub struct W<'a> {
    a_prep: mysql::conn::Stmt<'a>,
    v_prep: mysql::conn::Stmt<'a>,
    vc_prep: mysql::conn::Stmt<'a>,
}

pub fn setup(addr: &str) -> mysql::Pool {
    use mysql::Opts;

    let addr = format!("mysql://{}", addr);
    // we need to do this dance to avoid using the DB early (which will crash us if it doesn't
    // exist)
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();

    // Check whether database already exists, or whether we need to create it
    let pool = mysql::Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    if conn.query(format!("USE {}", db)).is_ok() {
        conn.query(format!("DROP DATABASE {}", &db).as_str()).unwrap();
    }
    conn.query(format!("CREATE DATABASE {}", &db).as_str()).unwrap();

    // allow larger in-memory tables (4 GB)
    pool.prep_exec("SET max_heap_table_size = 4294967296", ()).unwrap();

    // create tables with indices
    pool.prep_exec("CREATE TABLE art (id bigint, title varchar(255), votes bigint, \
                    PRIMARY KEY USING HASH (id)) ENGINE = MEMORY",
                   ())
        .unwrap();
    pool.prep_exec("CREATE TABLE vt (u bigint, id bigint, PRIMARY KEY USING HASH (u, id), \
                    KEY id (id)) ENGINE = MEMORY",
                   ())
        .unwrap();

    pool
}

pub fn make_writer<'a>(pool: &'a mysql::Pool) -> W<'a> {
    W {
        a_prep: pool.prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)").unwrap(),
        v_prep: pool.prepare("INSERT INTO vt (u, id) VALUES (:user, :id)").unwrap(),
        vc_prep: pool.prepare("UPDATE art SET votes = votes + 1 WHERE id = :id").unwrap(),
    }
}

impl<'a> Writer for W<'a> {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        self.a_prep.execute(params!{"id" => article_id, "title" => &title}).unwrap();
    }
    fn vote(&mut self, user_id: i64, article_id: i64) -> Period {
        self.v_prep.execute(params!{"user" => &user_id, "id" => &article_id}).unwrap();
        self.vc_prep.execute(params!{"id" => &article_id}).unwrap();
        Period::PreMigration
    }
}

pub fn make_reader<'a>(pool: &'a mysql::Pool) -> mysql::conn::Stmt<'a> {
    pool.prepare("SELECT id, title, votes FROM art WHERE id = :id").unwrap()
}

impl<'a> Reader for mysql::conn::Stmt<'a> {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period) {
        let mut res = ArticleResult::NoSuchArticle;
        for row in self.execute(params!{"id" => &article_id}).unwrap() {
            let mut rr = row.unwrap();
            res = ArticleResult::Article {
                id: rr.get(0).unwrap(),
                title: rr.get(1).unwrap(),
                votes: rr.get(2).unwrap(),
            };
            break;
        }
        (res, Period::PreMigration)
    }
}
