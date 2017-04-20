use mysql::{self, OptsBuilder};

use common::{Writer, Reader, ArticleResult, Period, RuntimeConfig};

pub struct RW<'a> {
    v_prep_1: mysql::conn::Stmt<'a>,
    v_prep_2: mysql::conn::Stmt<'a>,
    r_prep: mysql::conn::Stmt<'a>,
    pool: &'a mysql::Pool,
}

pub fn setup(addr: &str, config: &RuntimeConfig) -> mysql::Pool {
    use mysql::Opts;

    let addr = format!("mysql://{}", addr);
    let db = &addr[addr.rfind("/").unwrap() + 1..];
    let opts = Opts::from_url(&addr[0..addr.rfind("/").unwrap()]).unwrap();

    if config.mix.does_write() && !config.should_reuse() {
        // clear the db (note that we strip of /db so we get default)
        let mut opts = OptsBuilder::from_opts(opts.clone());
        opts.db_name(Some(db));
        opts.init(vec!["SET max_heap_table_size = 4294967296;",
                       "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"]);
        let pool = mysql::Pool::new_manual(1, 4, opts).unwrap();
        let mut conn = pool.get_conn().unwrap();
        if conn.query(format!("USE {}", db)).is_ok() {
            conn.query(format!("DROP DATABASE {}", &db).as_str())
                .unwrap();
        }
        conn.query(format!("CREATE DATABASE {}", &db).as_str())
            .unwrap();
        conn.query(format!("USE {}", db)).unwrap();

        drop(conn);

        // create tables with indices
        pool.prep_exec("CREATE TABLE art (id bigint, title varchar(16), votes bigint, \
                        PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
        pool.prep_exec("CREATE TABLE vt (u bigint, id bigint, KEY id (id)) ENGINE = MEMORY;",
                       ())
            .unwrap();
    }

    // now we connect for real
    let mut opts = OptsBuilder::from_opts(opts);
    opts.db_name(Some(db));
    opts.init(vec!["SET max_heap_table_size = 4294967296;",
                   "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"]);
    mysql::Pool::new_manual(1, 4, opts).unwrap()
}

pub fn make<'a>(pool: &'a mysql::Pool, config: &RuntimeConfig) -> RW<'a> {
    let vals = (1..config.mix.write_size().unwrap_or(1) + 1)
        .map(|_| "(?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
    let v_prep_1 = pool.prepare(vote_qstring).unwrap();

    let vals = (1..config.mix.write_size().unwrap_or(1) + 1)
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN({})", vals);
    let v_prep_2 = pool.prepare(vote_qstring).unwrap();

    let qstring = (1..config.mix.read_size().unwrap_or(1) + 1)
        .map(|_| "SELECT id, title, votes FROM art WHERE id = ?")
        .collect::<Vec<_>>()
        .join(" UNION ");
    let r_prep = pool.prepare(qstring).unwrap();

    RW {
        v_prep_1,
        v_prep_2,
        r_prep,
        pool,
    }
}

impl<'a> Writer for RW<'a> {
    type Migrator = ();
    fn make_articles<I>(&mut self, articles: I)
        where I: Iterator<Item = (i64, String)>,
              I: ExactSizeIterator
    {
        let mut vals = Vec::with_capacity(articles.len());
        let args: Vec<_> = articles
            .map(|(aid, title)| {
                     vals.push("(?, ?, 0)");
                     (aid, title)
                 })
            .collect();
        let args: Vec<_> = args.iter()
            .flat_map(|&(ref aid, ref title)| vec![aid as &_, title as &_])
            .collect();
        let vals = vals.join(", ");
        self.pool
            .prep_exec(format!("INSERT INTO art (id, title, votes) VALUES {}", vals),
                       &args[..])
            .unwrap();
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        // register votes
        let values_1 = ids.iter()
            .flat_map(|&(ref u, ref a)| vec![u as &_, a as &_])
            .collect::<Vec<_>>();
        self.v_prep_1.execute(&values_1[..]).unwrap();

        // update vote counts
        let values_2 = ids.iter()
            .map(|&(_, ref a)| a as &_)
            .collect::<Vec<_>>();
        self.v_prep_2.execute(&values_2[..]).unwrap();

        Period::PreMigration
    }
}

impl<'a> Reader for RW<'a> {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let ids: Vec<_> = ids.iter().map(|&(_, ref a)| a as &_).collect();

        let mut res = Vec::new();
        let mut qresult = self.r_prep.execute(&ids[..]).unwrap();
        while qresult.more_results_exists() {
            for row in qresult.by_ref() {
                let mut rr = row.unwrap();
                res.push(ArticleResult::Article {
                             id: rr.get(0).unwrap(),
                             title: rr.get(1).unwrap(),
                             votes: rr.get(2).unwrap(),
                         });
            }
        }
        (Ok(res), Period::PreMigration)
    }
}
