#[macro_use]
extern crate clap;

extern crate mysql;

use mysql as my;

struct Backend {
    pool: mysql::Pool,
}

impl Backend {
    fn new(addr: &str) -> Backend {
        Backend {
            pool: my::Pool::new_manual(1, 1, addr).unwrap(),
        }
    }

    fn populate(&self, nusers: i32, nclasses: i32, nposts: i32) {
        unimplemented!();
    }

    fn create_connection(&self, db: &str) {
        let mut conn = self.pool.get_conn().unwrap();
        if conn.query(format!("USE {}", db)).is_ok() {
            conn.query(format!("DROP DATABASE {}", &db).as_str())
                .unwrap();
        }

        conn.query(format!("CREATE DATABASE {}", &db).as_str())
            .unwrap();
        conn.query(format!("USE {}", db)).unwrap();

        drop(conn);
    }

    fn create_tables(&self) {
        self.pool.prep_exec(
            "CREATE TABLE Post ( \
              p_id int(11) NOT NULL auto_increment, \
              p_cid int(11) NOT NULL, \
              p_author int(11) NOT NULL, \
              p_content varchar(258) NOT NULL, \
              p_private tinyint(1) NOT NULL default '0', \
              PRIMARY KEY (p_id), \
              UNIQUE KEY p_id (p_id), \
              KEY p_cid (p_cid), \
              KEY p_author (p_author) \
            ) ENGINE=MEMORY;",
            (),
        ).unwrap();

        self.pool.prep_exec(
            "CREATE TABLE User ( \
              u_id int(11) NOT NULL auto_increment, \
              PRIMARY KEY  (u_id), \
              UNIQUE KEY u_id (u_id) \
            ) ENGINE=MEMORY;",
            (),
        ).unwrap();

        self.pool.prep_exec(
            "CREATE TABLE Class ( \
              c_id int(11) NOT NULL auto_increment, \
              PRIMARY KEY  (c_id), \
              UNIQUE KEY c_id (c_id) \
            ) ENGINE=MEMORY;",
            (),
        ).unwrap();

        self.pool.prep_exec(
            "CREATE TABLE Role ( \
              r_uid int(11) NOT NULL, \
              r_cid int(11) NOT NULL, \
              r_role tinyint(1) NOT NULL default '0', \
              PRIMARY KEY  (r_uid), \
              UNIQUE KEY r_uid (r_uid), \
              KEY r_cid (r_cid) \
            ) ENGINE=MEMORY;",
            (),
        ).unwrap();

    }

}


fn main() {
    use clap::{App, Arg};

    let args = App::new("piazza-mysql")
        .version("0.1")
        .about("Benchmarks a forum like application with security policies using MySql")
        .arg(
            Arg::with_name("dbname")
                .required(true),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .default_value("1000")
                .help("Number of users in the db"),
        )
        .arg(
            Arg::with_name("nclasses")
                .short("c")
                .default_value("100")
                .help("Number of classes in the db"),
        )
        .arg(
            Arg::with_name("nposts")
                .short("p")
                .default_value("100000")
                .help("Number of posts in the db"),
        )
        .get_matches();

    let dbn = args.value_of("dbname").unwrap();
    let nusers = value_t_or_exit!(args, "nusers", i32);
    let nclasses = value_t_or_exit!(args, "nclasses", i32);
    let nposts = value_t_or_exit!(args, "nposts", i32);

    let backend = Backend::new(dbn);

    let db = &dbn[dbn.rfind("/").unwrap() + 1..];
    backend.create_connection(db);
    backend.create_tables();

    backend.populate(nusers, nclasses, nposts);

}