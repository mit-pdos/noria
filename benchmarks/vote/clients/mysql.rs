use mysql;

use common::{Writer, Reader, ArticleResult, Period};

pub struct W<'a> {
    a_prep: mysql::conn::Stmt<'a>,
    v_prep: mysql::conn::Stmt<'a>,
    vc_prep: mysql::conn::Stmt<'a>,
}

pub fn setup(addr: &str, write: bool) -> mysql::Pool {
    use mysql::Opts;

    let addr = format!("mysql://{}", addr);
    if write {
        // clear the db (note that we strip of /db so we get default)
        let db = &addr[addr.rfind("/").unwrap() + 1..];
        let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();
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
    }

    // now we connect for real
    mysql::Pool::new(Opts::from_url(&addr).unwrap()).unwrap()
}

pub fn make_writer<'a>(pool: &'a mysql::Pool, batch_size: usize) -> W<'a> {
    let mut vc_qstring = String::new();
    let mut vote_qstring = String::new();
    for i in 1..batch_size + 1 {
        vote_qstring.push_str(&format!("INSERT INTO vt (u, id) VALUES (:user{}, :id{}); ", i, i));
        vc_qstring.push_str(&format!("UPDATE art SET votes = votes + 1 WHERE id = :id{}; ", i));
    }
    W {
        a_prep: pool.prepare("INSERT INTO art (id, title, votes) VALUES (:id, :title, 0)")
            .unwrap(),
        v_prep: pool.prepare(vote_qstring).unwrap(),
        vc_prep: pool.prepare(vc_qstring).unwrap(),
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
        let v_params: Vec<_> = ids.iter()
            .fold(Vec::new(), |mut acc, &(ref u, ref a)| {
                acc.push(u as &_);
                acc.push(a as &_);
                acc
            });
        let vc_params: Vec<_> = ids.iter().map(|&(_, ref a)| a as &_).collect();
        self.v_prep.execute(v_params).unwrap();
        self.vc_prep.execute(vc_params).unwrap();
        Period::PreMigration
    }
}

pub fn make_reader<'a>(pool: &'a mysql::Pool, batch_size: usize) -> mysql::conn::Stmt<'a> {
    let mut qstring = String::new();
    for i in 1..batch_size + 1 {
        qstring.push_str(&format!("SELECT id, title, votes FROM art WHERE id = :id{} ", i));
    }
    pool.prepare(qstring).unwrap()
}

impl<'a> Reader for mysql::conn::Stmt<'a> {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let keys: Vec<_> = ids.iter()
            .map(|&(_, ref article_id)| article_id as &_)
            .collect();

        let mut res = Vec::new();

        for row in self.execute(keys).unwrap() {
            let mut rr = row.unwrap();
            res.push(ArticleResult::Article {
                         id: rr.get(0).unwrap(),
                         title: rr.get(1).unwrap(),
                         votes: rr.get(2).unwrap(),
                     });
        }

        (Ok(res), Period::PreMigration)
    }
}
