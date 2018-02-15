use futures::Future;
use futures_state_stream::StateStream;
use tiberius;
use tokio_core::reactor;

use clap;

use clients::{Parameters, VoteClient};

pub(crate) struct Client {
    conn: Conn,
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
            let bs = 1000;
            assert_eq!(params.articles % bs, 0);
            for _ in 0..params.articles / bs {
                use tiberius::stmt::ResultStreamExt;

                let mut sql = String::new();
                sql.push_str("INSERT INTO art (id, title) VALUES ");
                for i in 0..bs {
                    if i != 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'Article #{}')", aid + i, aid + i));
                }
                conn = core.run(conn.exec(sql, &[]).single()).unwrap().1;

                let mut sql = String::new();
                sql.push_str("INSERT INTO vt (u, id) VALUES ");
                for i in 0..bs {
                    if i != 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("(0, {})", aid + i));
                }
                conn = core.run(conn.exec(sql, &[]).single()).unwrap().1;

                aid += bs;
            }
        } else {
            core.run(fut.and_then(fixconn)).unwrap();
        }

        drop(core);

        Conf {
            addr: addr.to_string(),
            db: db.to_string(),
        }
    }

    fn from(cnf: &mut Self::Constructor) -> Self {
        Client {
            conn: Conn::new(&cnf.addr, &cnf.db),
        }
    }

    fn handle_writes(&mut self, ids: &[i32]) {
        let ids = ids.into_iter().map(|a| a as &_).collect::<Vec<_>>();

        let vote_qstring = (0..ids.len())
            .map(|i| format!("(0, @P{})", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vote_qstring);

        let fut = self.conn
            .conn
            .take()
            .unwrap()
            .exec(vote_qstring, &ids)
            .and_then(|r| r)
            .collect();
        let (_, conn) = self.conn.core.run(fut).unwrap();
        self.conn.conn = Some(conn);
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        // this is going to seem a little stupid, but bear with me
        // mssql has a bug where its performance drops off a cliff
        // for queries with many parameters. so, if we have many
        // parameter, we need to issue them in sub-batches.
        //
        // to figure out the size of the sub-batches, we find all
        // the factors of the batch size, and pick the greatest one
        // that's less than the performance cliff (~224 params).
        //
        // if we can't factor the batch size, or if the common factor
        // is small, we issue batches of size threshold and then pay
        // the extra cost of doing another query that isn't prepared
        // (i.e., it will have a different # of parameters).
        let nids = ids.len();
        let threshold = 200;
        if nids > threshold {
            let sbs = 'find: loop {
                // there's no reason to try factors that would produce overly large batches
                let mut f = ::std::cmp::max(nids / threshold, 2);
                // also no reason to go past sqrt, since we've already the inverses of those.
                // well, that's only kind of true. could be a good batch size there, but it would
                // likely be much smaller than threshold, so we'd end up not using it anyway.
                let mut end = ::std::cmp::min(threshold, (nids as f64).sqrt().floor() as usize);
                let step = if nids % 2 == 1 {
                    // odd numbers can't have even factors
                    if nids % 2 == 1 && f % 2 == 0 {
                        f += 1;
                    }
                    2
                } else {
                    1
                };

                while f < end {
                    if nids % f == 0 {
                        let other_f = nids / f;
                        if other_f < threshold {
                            break 'find nids / f;
                        }
                    }
                    f += step;
                }
                break 1;
            };

            // if the chosen batch size is small, then we have to do extra RTTs
            // that's bad. we have to weigh that against the cost of an extra
            // prepare. let's say that 1 RTT = 1 prepare, so:
            let min_batches = (nids + threshold - 1) / threshold;
            let sbs_batches = nids / sbs;

            let mut i = 0;
            if sbs_batches > min_batches + 1 {
                // batch size doesn't divide nicely, so just issue
                // large batches + one overflow batch.
                while i < nids {
                    let end = ::std::cmp::min(i + threshold, nids);
                    self.handle_reads(&ids[i..end]);
                    i += end;
                }
            } else {
                // read in batches of size sbs
                while i < nids {
                    self.handle_reads(&ids[i..(i + sbs)]);
                    i += sbs;
                }
            }
            return;
        }

        let ids = ids.into_iter().map(|a| a as &_).collect::<Vec<_>>();
        let vals = (0..ids.len())
            .map(|i| format!("@P{}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let qstring = format!(
            "SELECT id, title, votes FROM awvc WITH (NOEXPAND) WHERE id IN ({})",
            vals
        );

        // scope needed so that the compiler realizes that `fut` goes out of scope, thus returning
        // the borrow of `res`
        let mut rows = 0;
        let conn = {
            use tiberius::stmt::ResultStreamExt;
            let fut = self.conn
                .conn
                .take()
                .unwrap()
                .query(qstring, &ids)
                .for_each_row(|_| {
                    rows += 1;
                    Ok(())
                });
            self.conn.core.run(fut).unwrap()
        };
        self.conn.conn = Some(conn);

        // <= because IN() collapses duplicates
        assert!(rows <= ids.len());
    }
}
