use postgres;

use common::{Writer, Reader, ArticleResult, Period};

pub fn setup(addr: &str, write: bool) -> postgres::Connection {
    use postgres::IntoConnectParams;

    let dbn = format!("postgresql://{}", addr);
    let params = dbn.into_connect_params().unwrap();

    // Check whether database already exists, or whether we need to create it
    if write {
        let mut check_new_params = params.clone();
        check_new_params.database = Some(String::from("postgres"));
        let db = params.database.clone().unwrap_or_else(|| String::from("soup_bench"));
        let x = postgres::Connection::connect(check_new_params, postgres::SslMode::None).unwrap();
        if x.execute("SELECT datname FROM pg_database WHERE datname=$1", &[&db]).unwrap() != 0 {
            x.execute(format!("DROP DATABASE \"{}\"", &db).as_str(), &[]).unwrap();
        }
        x.execute(format!("CREATE DATABASE \"{}\"", &db).as_str(), &[]).unwrap();

        // create tables
        let x = postgres::Connection::connect(params.clone(), postgres::SslMode::None).unwrap();
        x.execute("CREATE TABLE art (id bigint, title varchar(255), votes bigint)",
                     &[])
            .unwrap();
        x.execute("CREATE TABLE vt (u bigint, id bigint)", &[]).unwrap();

        // create indices
        x.execute("CREATE INDEX ON art (id)", &[]).unwrap();
        x.execute("CREATE INDEX ON vt (id)", &[]).unwrap();
    }

    postgres::Connection::connect(params, postgres::SslMode::None).unwrap()
}

pub struct W<'a> {
    a_prep: postgres::stmt::Statement<'a>,
    v_prep: postgres::stmt::Statement<'a>,
    vc_prep: postgres::stmt::Statement<'a>,
}

pub fn make_writer<'a>(conn: &'a postgres::Connection) -> W<'a> {
    W {
        a_prep: conn.prepare("INSERT INTO art (id, title, votes) VALUES ($1, $2, 0)").unwrap(),
        v_prep: conn.prepare("INSERT INTO vt (u, id) VALUES ($1, $2)").unwrap(),
        vc_prep: conn.prepare("UPDATE art SET votes = votes + 1 WHERE id = $1").unwrap(),
    }
}

impl<'a> Writer for W<'a> {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        self.a_prep.execute(&[&article_id, &title]).unwrap();
    }
    fn vote(&mut self, user_id: i64, article_id: i64) -> Period {
        self.v_prep.execute(&[&user_id, &article_id]).unwrap();
        self.vc_prep.execute(&[&article_id]).unwrap();
        Period::PreMigration
    }
}

pub fn make_reader<'a>(conn: &'a postgres::Connection) -> postgres::stmt::Statement<'a> {
    conn.prepare("SELECT id, title, votes FROM art WHERE id = $1").unwrap()
}

impl<'a> Reader for postgres::stmt::Statement<'a> {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period) {
        let mut res = ArticleResult::NoSuchArticle;
        for row in &self.query(&[&article_id]).unwrap() {
            res = ArticleResult::Article {
                id: row.get(0),
                title: row.get(1),
                votes: row.get(2),
            };
            break;
        }
        (res, Period::PreMigration)
    }
}
