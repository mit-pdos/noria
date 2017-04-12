use futures::{Future, Stream};
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

pub fn make_writer(addr: &str, batch_size: usize) -> W {
    let mut core = reactor::Core::new().unwrap();

    let cfg_string = &addr[0..addr.rfind("/").unwrap()];
    let db = &addr[addr.rfind("/").unwrap() + 1..];

    // Check whether database already exists, or whether we need to create it
    let fut = tiberius::SqlConnection::connect(core.handle(), cfg_string)
        .and_then(|conn| {
                      conn.simple_exec(format!("DROP DATABASE {};", db)).and_then(|r| r).collect()
                  })
        .and_then(|(_, conn)| {
                      conn.simple_exec(format!("CREATE DATABASE {};", db)).and_then(|r| r).collect()
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

    let mut vote_qstring = String::new();
    for i in 0..batch_size {
        vote_qstring.push_str(&format!("INSERT INTO vt (u, id) VALUES (@P{}, @P{}); ",
                                       i * 2,
                                       i * 2 + 1));
    }

    let v_prep = client.conn.as_ref().unwrap().prepare(vote_qstring);
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

pub fn make_reader(addr: &str, batch_size: usize) -> R {
    let client = mkc(addr);

    let mut qstring = String::new();
    for i in 1..batch_size + 1 {
        qstring.push_str(&format!("SELECT id, title, votes FROM awvc WITH (NOEXPAND) WHERE id = @P{};", i));
    }

    let prep = client.conn.as_ref().unwrap().prepare(qstring);
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
        let (_, conn) = self.client
            .core
            .run(fut)
            .unwrap();
        self.client.conn = Some(conn);
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let data = ids.iter()
            .fold(Vec::new(), |mut acc, &(ref u, ref a)| {
                acc.push(u as &_);
                acc.push(a as &_);
                acc
            });
        let fut = self.client
            .conn
            .take()
            .unwrap()
            .exec(&self.v_prep, data.as_slice())
            .and_then(|r| r)
            .collect();
        let (_, conn) = self.client.core.run(fut).unwrap();
        self.client.conn = Some(conn);
        Period::PreMigration
    }
}

impl Reader for R {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        use tiberius::stmt::ResultStreamExt;

        let mut res = Vec::new();
        let conn;
        // scope needed so that the compiler realizes that `fut` goes out of scope, thus returning
        // the borrow of `res`
        {
            let data = ids.iter()
                .fold(Vec::new(), |mut acc, &(_, ref a)| {
                    acc.push(a as &_);
                    acc
                });
            let fut = self.client
                .conn
                .take()
                .unwrap()
                .query(&self.prep, data.as_slice())
                .for_each(|qs| {
                    let q_res: Vec<ArticleResult> = qs.wait()
                        .fold(Vec::new(), |mut acc,
                               row: Result<tiberius::query::QueryRow, tiberius::TdsError>| {
                            let row = row.unwrap();
                            let aid: i64 = row.get(0);
                            let title: &str = row.get(1);
                            let votes: i64 = row.get(2);
                            acc.push(ArticleResult::Article {
                                         id: aid,
                                         title: String::from(title),
                                         votes: votes,
                                     });
                            acc
                        });
                    res.extend(q_res);
                    Ok(())
                });
            conn = self.client.core.run(fut).unwrap();
        }
        self.client.conn = Some(conn);
        (Ok(res), Period::PreMigration)
    }
}
