use futures::Future;
use futures_state_stream::StateStream;
use tiberius;
use tokio_core::reactor;

use common::{Writer, Reader, ArticleResult, Period};

pub struct Client {
    conn: Option<tiberius::SqlConnection<Box<tiberius::BoxableIo>>>,
    core: reactor::Core,
}

// safe because the core will remain on the same core as the client
unsafe impl Send for Client {}

fn mkc(addr: &str) -> Client {
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let cfg_string = &addr[0..addr.rfind("/").unwrap()];

    let mut core = reactor::Core::new().unwrap();
    let fc = tiberius::SqlConnection::connect(core.handle(), cfg_string).and_then(|conn| {
        conn.simple_exec(format!("USE {}; \
                                      SET NUMERIC_ROUNDABORT OFF; \
                                      SET ANSI_PADDING, ANSI_WARNINGS, \
                                      CONCAT_NULL_YIELDS_NULL, ARITHABORT, \
                                      QUOTED_IDENTIFIER, ANSI_NULLS ON;",
                                 db))
            .and_then(|r| r)
            .collect()
    });
    match core.run(fc) {
        Ok((_, conn)) => {
            return Client {
                conn: Some(conn),
                core: core,
            }
        }
        Err(_) => panic!("Failed to connect to SQL server"),
    }
}

pub fn make_writer(addr: &str) -> W {
    let mut core = reactor::Core::new().unwrap();

    let cfg_string = &addr[0..addr.rfind("/").unwrap()];
    let db = &addr[addr.rfind("/").unwrap() + 1..];

    // Check whether database already exists, or whether we need to create it
    let fut = tiberius::SqlConnection::connect(core.handle(), cfg_string)
        .and_then(|conn| {
            conn.simple_exec(format!("DROP DATABASE {};", db))
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec(format!("CREATE DATABASE {};", db))
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec(format!("USE {}; \
                                      SET NUMERIC_ROUNDABORT OFF; \
                                      SET ANSI_PADDING, ANSI_WARNINGS, \
                                      CONCAT_NULL_YIELDS_NULL, ARITHABORT, \
                                      QUOTED_IDENTIFIER, ANSI_NULLS ON; ",
                                     db))
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec("CREATE TABLE art (
                             id bigint PRIMARY KEY NONCLUSTERED,
                             title varchar(255),
                             votes bigint
                             );")
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec("CREATE TABLE vt (
                             u bigint,
                             id bigint,
                             PRIMARY KEY NONCLUSTERED (u, id)
                             );")
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec("CREATE VIEW dbo.awvc WITH SCHEMABINDING AS
                                SELECT art.id, art.title, COUNT_BIG(*) AS votes
                                FROM dbo.art AS art, dbo.vt AS vt
                                WHERE art.id = vt.id
                                GROUP BY art.id, art.title;")
                .and_then(|r| r)
                .collect()
        })
        .and_then(|(_, conn)| {
            conn.simple_exec("CREATE UNIQUE CLUSTERED INDEX ix ON dbo.awvc (id);")
                .and_then(|r| r)
                .collect()
        });

    core.run(fut).unwrap();
    drop(core);

    let client = mkc(addr);
    let a_prep = client.conn
        .as_ref()
        .unwrap()
        .prepare("INSERT INTO art (id, title, votes) VALUES (@P1, @P2, 0);");
    let v_prep = client.conn
        .as_ref()
        .unwrap()
        .prepare("INSERT INTO vt (u, id) VALUES (@P1, @P2);");
    W {
        client: client,
        a_prep: a_prep,
        v_prep: v_prep,
    }
}

pub struct W {
    client: Client,
    a_prep: tiberius::stmt::Statement,
    v_prep: tiberius::stmt::Statement,
}

// all our methods are &mut
unsafe impl Sync for W {}
unsafe impl Send for W {}

pub fn make_reader(addr: &str) -> R {
    let client = mkc(addr);
    let prep = client.conn
        .as_ref()
        .unwrap()
        .prepare("SELECT id, title, votes FROM awvc WITH (NOEXPAND) WHERE id = @P1;");
    R {
        client: client,
        prep: prep,
    }
}

pub struct R {
    client: Client,
    prep: tiberius::stmt::Statement,
}

// all our methods are &mut
unsafe impl Sync for R {}
unsafe impl Send for R {}

impl Writer for W {
    type Migrator = ();
    fn make_article(&mut self, article_id: i64, title: String) {
        let fut = self.client
            .conn
            .take()
            .unwrap()
            .exec(&self.a_prep, &[&article_id, &title.as_str()])
            .and_then(|r| r)
            .collect();
        let (_, conn) = self.client.core.run(fut).unwrap();
        self.client.conn = Some(conn);
    }

    fn vote(&mut self, user_id: i64, article_id: i64) -> Period {
        let fut = self.client
            .conn
            .take()
            .unwrap()
            .exec(&self.v_prep, &[&user_id, &article_id])
            .and_then(|r| r)
            .collect();
        let (_, conn) = self.client.core.run(fut).unwrap();
        self.client.conn = Some(conn);
        Period::PreMigration
    }
}

impl Reader for R {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period) {
        use tiberius::stmt::ResultStreamExt;

        let mut res = ArticleResult::NoSuchArticle;
        {
            let fut = self.client
                .conn
                .take()
                .unwrap()
                .query(&self.prep, &[&article_id])
                .for_each_row(|ref row| {
                    let aid: i64 = row.get(0);
                    let title: &str = row.get(1);
                    let votes: i64 = row.get(2);
                    res = ArticleResult::Article {
                        id: aid,
                        title: String::from(title),
                        votes: votes,
                    };
                    Ok(())
                });
            let conn = self.client.core.run(fut).unwrap();
            self.client.conn = Some(conn);
        }
        (res, Period::PreMigration)
    }
}
