use futures::Future;
use futures_state_stream::StateStream;
use tiberius;
use tokio_core::reactor;

use clap;
use std::time;

use clients::{Parameters, VoteClient};

pub(crate) struct Client {
    conn: Conn,

    write_stmt: tiberius::stmt::Statement,
    read_stmt: tiberius::stmt::Statement,

    write_size: usize,
    read_size: usize,
}
// safe (?) because every Handle associated with Core is also sent
unsafe impl Send for Client {}

struct Conn {
    conn: Option<tiberius::SqlConnection<Box<tiberius::BoxableIo>>>,
    core: reactor::Core,
}

impl Conn {
    fn new(addr: &str, db: &str) -> Conn {
        let mut core = reactor::Core::new().unwrap();
        let fc = tiberius::SqlConnection::connect(core.handle(), addr).and_then(|conn| {
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
                return Conn {
                    conn: Some(conn),
                    core: core,
                }
            }
            Err(_) => panic!("Failed to connect to SQL server"),
        }
    }
}

pub(crate) struct Conf {
    write_size: usize,
    read_size: usize,
    addr: String,
    db: String,
}

impl VoteClient for Client {
    type Constructor = Conf;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self::Constructor {
        let addr = args.value_of("address").unwrap();
        let db = args.value_of("database").unwrap();

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
        let mut core = reactor::Core::new().unwrap();
        let fut = tiberius::SqlConnection::connect(core.handle(), addr);
        if params.prime {
            // drop database if possible
            let x = core.run(fut.and_then(|conn| {
                conn.simple_exec(format!("DROP DATABASE {};", db))
                    .and_then(|r| r)
                    .collect()
            }));
            // we don't care if dropping failed
            drop(x);

            // we need to connect again because there's no way to recover the conn if drop fails
            let fut = tiberius::SqlConnection::connect(core.handle(), addr);
            let fut = fut.and_then(|conn| {
                conn.simple_exec(format!("CREATE DATABASE {};", db))
                    .and_then(|r| r)
                    .collect()
            }).and_then(|(_, conn)| fixconn(conn))
                .and_then(|(_, conn)| {
                    conn.simple_exec(
                        "CREATE TABLE art (
                             id bigint NOT NULL PRIMARY KEY NONCLUSTERED,
                             title varchar(16) NOT NULL
                             );",
                    ).and_then(|r| r)
                        .collect()
                })
                .and_then(|(_, conn)| {
                    conn.simple_exec(
                        "CREATE TABLE vt (
                             u bigint NOT NULL,
                             id bigint NOT NULL index vt_article_idx
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
            let mut conn = core.run(fut).unwrap().1;

            // prepop
            let mut aid = 0;
            assert_eq!(params.articles % params.max_batch_size, 0);
            for _ in 0..params.articles / params.max_batch_size {
                use tiberius::stmt::ResultStreamExt;

                let mut sql = String::new();
                sql.push_str("INSERT INTO art (id, title) VALUES ");
                for i in 0..params.max_batch_size {
                    if i != 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'Article #{}')", aid + i, aid + i));
                }
                conn = core.run(conn.exec(sql, &[]).single()).unwrap().1;

                let mut sql = String::new();
                sql.push_str("INSERT INTO vt (u, id) VALUES ");
                for i in 0..params.max_batch_size {
                    if i != 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("(0, {})", aid + i));
                }
                conn = core.run(conn.exec(sql, &[]).single()).unwrap().1;

                aid += params.max_batch_size;
            }
        } else {
            core.run(fut.and_then(fixconn)).unwrap();
        }

        drop(core);

        Conf {
            write_size: 2,
            read_size: 16,
            addr: addr.to_string(),
            db: db.to_string(),
        }
    }

    fn from(cnf: &mut Self::Constructor) -> Self {
        let conn = Conn::new(&cnf.addr, &cnf.db);

        let vote_qstring = (0..cnf.write_size)
            .map(|i| format!("begin try\n\
                             INSERT INTO vt (u, id) VALUES (0, @P{})\n\
                             end try\n\
                             begin catch\n\
                             end catch", i + 1))
            .collect::<Vec<_>>()
            .join("; ");
        let vote_qstring = format!("begin transaction\n\
                                   {}\n\
                                   commit", vote_qstring);
        let w = conn.conn.as_ref().unwrap().prepare(vote_qstring);

        let vals = (0..cnf.read_size)
            .map(|i| format!("@P{}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        // NOTE: this is sort of unfair with skewed ids, since every row is only returned once
        let qstring = format!(
            "SELECT id, title, votes FROM awvc WITH (NOEXPAND) WHERE id IN ({})",
            vals
        );
        let r = conn.conn.as_ref().unwrap().prepare(qstring);

        Client {
            conn: conn,

            write_stmt: w,
            read_stmt: r,

            read_size: cnf.read_size,
            write_size: cnf.write_size,
        }
    }

    fn handle_writes(&mut self, ids: &[(time::Instant, i32)]) {
        let missing = self.write_size - (ids.len() % self.write_size);
        let ids = ids.iter()
            .map(|&(_, ref a)| a as &_)
            .chain((0..missing).map(|_| &None::<bool> as &_))
            .collect::<Vec<_>>();

        for chunk in ids.chunks(self.write_size) {
            let fut = self.conn
                .conn
                .take()
                .unwrap()
                .exec(&self.write_stmt, chunk)
                .and_then(|r| r)
                .collect();
            let (_, conn) = self.conn.core.run(fut).unwrap();
            self.conn.conn = Some(conn);
        }
    }

    fn handle_reads(&mut self, ids: &[(time::Instant, i32)]) {
        let missing = self.read_size - (ids.len() % self.read_size);
        let ids = ids.iter()
            .map(|&(_, ref a)| a as &_)
            .chain((0..missing).map(|_| &None::<bool> as &_))
            .collect::<Vec<_>>();

        let mut rows = 0;
        for chunk in ids.chunks(self.read_size) {
            // scope needed so that the compiler realizes that `fut` goes out of scope, thus returning
            // the borrow of `res`
            let conn = {
                use tiberius::stmt::ResultStreamExt;
                let fut = self.conn
                    .conn
                    .take()
                    .unwrap()
                    .query(&self.read_stmt, chunk)
                    .for_each_row(|_| {
                        rows += 1;
                        Ok(())
                    });
                self.conn.core.run(fut).unwrap()
            };
            self.conn.conn = Some(conn);
        }

        // <= because IN() collapses duplicates
        assert!(rows <= ids.len() - missing);
    }
}
