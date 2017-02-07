use futures::Future;
use futures_state_stream::StateStream;
use tiberius;
use tokio_core::reactor;

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub struct MssqlTarget {
    dbn: String,
    db: String,
}

pub fn make(dbn: &str, _: usize) -> MssqlTarget {
    let mut core = reactor::Core::new().unwrap();

    let cfg_string = &dbn[0..dbn.rfind("/").unwrap()];
    let db = &dbn[dbn.rfind("/").unwrap() + 1..];

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
            conn.simple_exec(format!("USE {};", db))
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
                             id bigint
                             );")
                .and_then(|r| r)
                .collect()
        });

    core.run(fut).unwrap();
    drop(core);

    MssqlTarget {
        dbn: String::from(cfg_string),
        db: String::from(db),
    }
}

pub struct Client {
    conn: Option<tiberius::SqlConnection<Box<tiberius::BoxableIo>>>,
    core: reactor::Core,
}

// safe because the core will remain on the same core as the client
unsafe impl Send for Client {}

impl MssqlTarget {
    fn mkc(&self) -> Client {
        let mut core = reactor::Core::new().unwrap();

        let fc = tiberius::SqlConnection::connect(core.handle(), self.dbn.as_str())
            .and_then(|conn| {
                conn.simple_exec(format!("USE {}", self.db.as_str()))
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
}

impl Backend for MssqlTarget {
    type P = Client;
    type G = Client;

    fn getter(&mut self) -> Self::G {
        self.mkc()
    }

    fn putter(&mut self) -> Self::P {
        self.mkc()
    }

    fn migrate(&mut self, _: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for Client {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let prep = self.conn
            .as_ref()
            .unwrap()
            .prepare("INSERT INTO art (id, title, votes) VALUES (@P1, @P2, 0);");
        Box::new(move |id, title| {
            let fut = self.conn
                .take()
                .unwrap()
                .exec(&prep, &[&id, &title.as_str()])
                .and_then(|r| r)
                .collect();
            let (_, conn) = self.core.run(fut).unwrap();
            self.conn = Some(conn);
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        let pv = self.conn
            .as_ref()
            .unwrap()
            .prepare("INSERT INTO vt (u, id) VALUES (@P1, @P2);");
        let pa = self.conn
            .as_ref()
            .unwrap()
            .prepare("UPDATE art SET votes = votes + 1 WHERE id = @P1;");
        Box::new(move |user, id| {
            let fut = self.conn
                .take()
                .unwrap()
                .exec(&pv, &[&user, &id])
                .and_then(|r| r)
                .collect();
            let (_, conn) = self.core.run(fut).unwrap();
            let fut = conn.exec(&pa, &[&id])
                .and_then(|r| r)
                .collect();
            let (_, conn) = self.core.run(fut).unwrap();
            self.conn = Some(conn);
        })
    }
}

impl Getter for Client {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        use tiberius::stmt::ResultStreamExt;

        let prep = self.conn
            .as_ref()
            .unwrap()
            .prepare("SELECT id, title, votes FROM art WHERE id = @P1;");
        Box::new(move |id| {
            let mut res = None;
            {
                let fut = self.conn
                    .take()
                    .unwrap()
                    .query(&prep, &[&id])
                    .for_each_row(|ref row| {
                        let aid: i64 = row.get(0);
                        let title: &str = row.get(1);
                        let votes: i64 = row.get(2);
                        res = Some((aid, String::from(title), votes));
                        Ok(())
                    });
                let conn = self.core.run(fut).unwrap();
                self.conn = Some(conn);
            }
            Ok(res)
        })
    }
}
