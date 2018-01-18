use mysql::{self, Opts, OptsBuilder};

use clap;
use std::time;

use clients::{Parameters, VoteClient};

pub(crate) struct Client {
    write1_stmt: mysql::Stmt<'static>,
    write2_stmt: mysql::Stmt<'static>,
    read_stmt: mysql::Stmt<'static>,

    write_size: usize,
    read_size: usize,
}

pub(crate) struct Conf {
    write_size: usize,
    read_size: usize,
    opts: Opts,
}

impl VoteClient for Client {
    type Constructor = Conf;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self::Constructor {
        let addr = args.value_of("address").unwrap();
        let addr = format!("mysql://{}", addr);
        let db = args.value_of("database").unwrap();
        let opts = Opts::from_url(&addr).unwrap();

        if params.prime {
            let mut opts = OptsBuilder::from_opts(opts.clone());
            opts.db_name(None::<&str>);
            opts.init(vec![
                "SET max_heap_table_size = 4294967296;",
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            ]);
            let mut conn = mysql::Conn::new(opts).unwrap();
            if conn.query(format!("USE {}", db)).is_ok() {
                conn.query(format!("DROP DATABASE {}", &db).as_str())
                    .unwrap();
            }
            conn.query(format!("CREATE DATABASE {}", &db).as_str())
                .unwrap();

            // create tables with indices
            conn.query(format!("USE {}", db)).unwrap();
            conn.prep_exec(
                "CREATE TABLE art (id bigint not null, title varchar(16) not null, votes bigint not null, \
                 PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();
            conn.prep_exec(
                "CREATE TABLE vt (u bigint not null, id bigint not null, KEY id (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();

            // prepop
            let mut aid = 0;
            assert_eq!(params.articles % params.max_batch_size, 0);
            for _ in 0..params.articles / params.max_batch_size {
                let mut sql = String::new();
                sql.push_str("INSERT INTO art (id, title, votes) VALUES ");
                for i in 0..params.max_batch_size {
                    if i != 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str("(");
                    sql.push_str(&format!("{}, 'Article #{}'", aid, aid));
                    sql.push_str(", 0)");
                    aid += 1;
                }
                conn.query(sql).unwrap();
            }
        }

        // now we connect for real
        let mut opts = OptsBuilder::from_opts(opts);
        opts.db_name(Some(db));
        opts.init(vec![
            "SET max_heap_table_size = 4294967296;",
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
        ]);

        Conf {
            write_size: 2,
            read_size: 32,
            opts: opts.into(),
        }
    }

    fn from(cnf: &mut Self::Constructor) -> Self {
        let mut conns = mysql::Pool::new_manual(3, 3, cnf.opts.clone()).unwrap();
        conns.use_cache(false);

        let vals = (0..cnf.write_size)
            .map(|_| "(0, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let vote_qstring = format!("INSERT IGNORE INTO vt (u, id) VALUES {}", vals);
        let w1 = conns.prepare(vote_qstring).unwrap();

        let vals = (0..cnf.write_size)
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        // NOTE: this is *not* correct for duplicate ids
        let vote_qstring = format!(
            "UPDATE IGNORE art SET votes = votes + 1 WHERE id IN({})",
            vals
        );
        let w2 = conns.prepare(vote_qstring).unwrap();

        let vals = (0..cnf.read_size)
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        // NOTE: this is sort of unfair with skewed ids, since every row is only returned once
        let qstring = format!("SELECT id, title, votes FROM art WHERE id IN ({})", vals);
        let r = conns.prepare(qstring).unwrap();

        Client {
            write1_stmt: w1,
            write2_stmt: w2,
            read_stmt: r,

            read_size: cnf.read_size,
            write_size: cnf.write_size,
        }
    }

    fn handle_writes(&mut self, ids: &[(time::Instant, usize)]) {
        let missing = self.write_size - (ids.len() % self.write_size);
        let ids = ids.iter()
            .map(|&(_, ref a)| a as &_)
            .chain((0..missing).map(|_| &mysql::Value::NULL as &_))
            .collect::<Vec<_>>();

        for chunk in ids.chunks(self.write_size) {
            // register votes
            self.write1_stmt.execute(&chunk[..]).unwrap();

            // update vote counts
            self.write2_stmt.execute(&chunk[..]).unwrap();
        }
    }

    fn handle_reads(&mut self, ids: &[(time::Instant, usize)]) {
        let missing = self.read_size - (ids.len() % self.read_size);
        let ids = ids.iter()
            .map(|&(_, ref a)| a as &_)
            .chain((0..missing).map(|_| &mysql::Value::NULL as &_))
            .collect::<Vec<_>>();

        let mut rows = 0;
        for chunk in ids.chunks(self.read_size) {
            let mut qresult = self.read_stmt.execute(&chunk[..]).unwrap();
            while qresult.more_results_exists() {
                for row in qresult.by_ref() {
                    row.unwrap();
                    rows += 1;
                }
            }
        }

        // <= because IN() collapses duplicates
        assert!(rows <= ids.len() - missing);
    }
}
