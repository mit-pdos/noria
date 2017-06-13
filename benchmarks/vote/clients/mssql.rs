use futures::{Future, Stream};
use futures_state_stream::StateStream;
use tiberius;
use tokio_core::reactor;

use common::{Writer, Reader, ArticleResult, Period, RuntimeConfig};

pub struct Client {
    conn: Option<tiberius::SqlConnection<Box<tiberius::BoxableIo>>>,
    core: reactor::Core,
}

fn mkc(addr: &str) -> Client {
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let cfg_string = &addr[0..addr.rfind("/").unwrap()];

    let mut core = reactor::Core::new().unwrap();
    let fc = tiberius::SqlConnection::connect(core.handle(), cfg_string).and_then(|conn| {
        conn.simple_exec(format!(
            "USE {}; \
                                      SET NUMERIC_ROUNDABORT OFF; \
                                      SET ANSI_PADDING, ANSI_WARNINGS, \
                                      CONCAT_NULL_YIELDS_NULL, ARITHABORT, \
                                      QUOTED_IDENTIFIER, ANSI_NULLS ON; \
                                      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            db
        )).and_then(|r| r)
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

pub fn make(addr: &str, config: &RuntimeConfig) -> RW {
    let mut core = reactor::Core::new().unwrap();

    let cfg_string = &addr[0..addr.rfind("/").unwrap()];
    let db = &addr[addr.rfind("/").unwrap() + 1..];

    let fixconn = |conn: tiberius::SqlConnection<Box<tiberius::BoxableIo>>| {
        conn.simple_exec(format!(
            "USE {}; \
                                      SET NUMERIC_ROUNDABORT OFF; \
                                      SET ANSI_PADDING, ANSI_WARNINGS, \
                                      CONCAT_NULL_YIELDS_NULL, ARITHABORT, \
                                      QUOTED_IDENTIFIER, ANSI_NULLS ON; \
                                      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            db
        )).and_then(|r| r)
            .collect()
    };

    // Check whether database already exists, or whether we need to create it
    let fut = tiberius::SqlConnection::connect(core.handle(), cfg_string);
    if config.mix.does_write() && !config.should_reuse() {
        let fut = fut.and_then(|conn| {
            conn.simple_exec(format!("DROP DATABASE {};", db))
                .and_then(|r| r)
                .collect()
        }).and_then(|(_, conn)| {
                conn.simple_exec(format!("CREATE DATABASE {};", db))
                    .and_then(|r| r)
                    .collect()
            })
            .and_then(|(_, conn)| fixconn(conn))
            .and_then(|(_, conn)| {
                conn.simple_exec(
                    "CREATE TABLE art (
                             id bigint PRIMARY KEY NONCLUSTERED,
                             title varchar(16)
                             );",
                ).and_then(|r| r)
                    .collect()
            })
            .and_then(|(_, conn)| {
                conn.simple_exec(
                    "CREATE TABLE vt (
                             u bigint,
                             id bigint index vt_article_idx
                             );",
                ).and_then(|r| r)
                    .collect()
            })
            .and_then(|(_, conn)| {
                conn.simple_exec(
                    "CREATE VIEW dbo.awvc WITH SCHEMABINDING AS
                                SELECT art.id, art.title, COUNT_BIG(*) AS votes
                                FROM dbo.art AS art, dbo.vt AS vt
                                WHERE art.id = vt.id
                                GROUP BY art.id, art.title;",
                ).and_then(|r| r)
                    .collect()
            })
            .and_then(|(_, conn)| {
                conn.simple_exec("CREATE UNIQUE CLUSTERED INDEX ix ON dbo.awvc (id);")
                    .and_then(|r| r)
                    .collect()
            });
        core.run(fut).unwrap();
    } else {
        core.run(fut.and_then(fixconn)).unwrap();
    }

    drop(core);

    let client = mkc(addr);
    let vals = (1..config.mix.write_size().unwrap_or(1) + 1)
        .map(|i| format!("(@P{}, @P{})", i * 2 - 1, i * 2))
        .collect::<Vec<_>>()
        .join(", ");
    let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
    let v_prep = client.conn.as_ref().unwrap().prepare(vote_qstring);

    let qstring = (1..config.mix.read_size().unwrap_or(1) + 1)
        .map(|i| {
            format!(
                "SELECT id, title, votes FROM awvc WITH (NOEXPAND) WHERE id = @P{}",
                i
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ");
    let prep = client.conn.as_ref().unwrap().prepare(qstring);

    RW {
        client: client,
        v_prep: v_prep,
        prep: prep,
    }
}

pub struct RW {
    client: Client,
    v_prep: tiberius::stmt::Statement,
    prep: tiberius::stmt::Statement,
}

impl Writer for RW {
    type Migrator = ();
    fn make_articles<I>(&mut self, articles: I)
    where
        I: Iterator<Item = (i64, String)>,
        I: ExactSizeIterator,
    {
        let articles: Vec<_> = articles.collect();
        let narticles = articles.len();
        let vals = (1..narticles + 1)
            .map(|i| format!("(@P{}, @P{})", i * 2 - 1, i * 2))
            .collect::<Vec<_>>()
            .join(", ");

        let qstring = format!("INSERT INTO art (id, title) VALUES {}", vals);
        let a1_prep = self.client.conn.as_ref().unwrap().prepare(qstring);
        let qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
        let a2_prep = self.client.conn.as_ref().unwrap().prepare(qstring);

        let articles: Vec<_> = articles
            .iter()
            .map(|&(article_id, ref title)| (0, article_id, title.as_str()))
            .collect();
        let mut a1_vals = Vec::with_capacity(narticles * 2);
        let mut a2_vals = Vec::with_capacity(narticles * 2);
        for &(ref zero, ref article_id, ref title) in &articles {
            a1_vals.push(article_id as &_);
            a1_vals.push(title as &_);
            a2_vals.push(zero as &_);
            a2_vals.push(article_id as &_);
        }

        let fut = self.client
            .conn
            .take()
            .unwrap()
            .exec(&a1_prep, &a1_vals[..])
            .and_then(|r| r)
            .collect();
        let (_, conn) = self.client.core.run(fut).unwrap();
        let fut = conn.exec(&a2_prep, &a2_vals[..]).and_then(|r| r).collect();
        let (_, conn) = self.client.core.run(fut).unwrap();
        self.client.conn = Some(conn);
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let data = ids.iter().fold(Vec::new(), |mut acc, &(ref u, ref a)| {
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

impl Reader for RW {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let mut res = Vec::new();
        let conn;
        // scope needed so that the compiler realizes that `fut` goes out of scope, thus returning
        // the borrow of `res`
        {
            let data: Vec<_> = ids.iter().map(|&(_, ref a)| a as &_).collect();
            let fut = self.client
                .conn
                .take()
                .unwrap()
                .query(&self.prep, data.as_slice())
                .for_each(|qs| {
                    let q_res: Vec<ArticleResult> = qs.wait()
                        .map(|row: Result<
                            tiberius::query::QueryRow,
                            tiberius::TdsError,
                        >| {
                            let row = row.unwrap();
                            let aid: i64 = row.get(0);
                            let title: &str = row.get(1);
                            let votes: i64 = row.get(2);
                            ArticleResult::Article {
                                id: aid,
                                title: String::from(title),
                                votes: votes,
                            }
                        })
                        .collect();
                    res.extend(q_res);
                    Ok(())
                });
            conn = self.client.core.run(fut).unwrap();
        }
        self.client.conn = Some(conn);
        (Ok(res), Period::PreMigration)
    }
}
