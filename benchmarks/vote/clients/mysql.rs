use mysql::{self, Opts, OptsBuilder};

use clap;
use std::time;

use VoteClient;

pub(crate) struct Client {
    write1_stmt: mysql::Stmt<'static>,
    write2_stmt: mysql::Stmt<'static>,
    read_stmt: mysql::Stmt<'static>,
}

impl VoteClient for Client {
    type Constructor = Opts;

    fn new(args: &clap::ArgMatches, articles: usize) -> Self::Constructor {
        let addr = args.value_of("address").unwrap();
        let addr = format!("mysql://{}", addr);
        let db = args.value_of("database").unwrap();
        let opts = Opts::from_url(&addr).unwrap();

        if args.is_present("prime") {
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
                "CREATE TABLE art (id bigint, title varchar(16), votes bigint, \
                 PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();
            conn.prep_exec(
                "CREATE TABLE vt (u bigint, id bigint, KEY id (id)) ENGINE = MEMORY;",
                (),
            ).unwrap();

            // prepop
            let mut sql = String::new();
            sql.push_str("INSERT INTO art (id, title, votes) VALUES ");
            for i in 0..articles {
                if i != 0 {
                    sql.push_str(", ");
                }
                sql.push_str("(");
                sql.push_str(&format!("{}, \"Article #{}\"", i, i));
                sql.push_str(", 0)");
            }
            conn.query(sql).unwrap();
        }

        // now we connect for real
        let mut opts = OptsBuilder::from_opts(opts);
        opts.db_name(Some(db));
        opts.init(vec![
            "SET max_heap_table_size = 4294967296;",
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
        ]);
        opts.into()
    }

    fn from(opts: &mut Self::Constructor) -> Self {
        let mut conns = mysql::Pool::new_manual(3, 3, opts.clone()).unwrap();
        conns.use_cache(false);

        let write_size = 1;
        let read_size = 1;

        let vals = (1..write_size + 1)
            .map(|_| "(0, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
        let w1 = conns.prepare(vote_qstring).unwrap();

        let vals = (1..write_size + 1)
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN({})", vals);
        let w2 = conns.prepare(vote_qstring).unwrap();

        let qstring = (1..read_size + 1)
            .map(|_| "SELECT id, title, votes FROM art WHERE id = ?")
            .collect::<Vec<_>>()
            .join(" UNION ");
        let r = conns.prepare(qstring).unwrap();

        Client {
            write1_stmt: w1,
            write2_stmt: w2,
            read_stmt: r,
        }
    }

    fn handle_writes(&mut self, ids: &[(time::Instant, usize)]) {
        let ids = ids.iter().map(|&(_, ref a)| a as &_).collect::<Vec<_>>();

        // register votes
        self.write1_stmt.execute(&ids[..]).unwrap();

        // update vote counts
        self.write2_stmt.execute(&ids[..]).unwrap();
    }

    fn handle_reads(&mut self, ids: &[(time::Instant, usize)]) {
        let ids = ids.iter().map(|&(_, ref a)| a as &_).collect::<Vec<_>>();

        let mut rows = 0;
        let mut qresult = self.read_stmt.execute(&ids[..]).unwrap();
        while qresult.more_results_exists() {
            for row in qresult.by_ref() {
                row.unwrap();
                rows += 1;
            }
        }
        assert_eq!(rows, ids.len());
    }
}
