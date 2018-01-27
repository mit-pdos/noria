#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    Netsoup {
        workers: usize,
        readers: usize,
        shards: Option<usize>,
    },
    Mssql,
    Mysql,
    Memcached,
}

impl Backend {
    pub(crate) fn multiclient_name(&self) -> &'static str {
        match *self {
            Backend::Netsoup { .. } => "netsoup",
            Backend::Mssql { .. } => "mssql",
            Backend::Mysql { .. } => "mysql",
            Backend::Memcached { .. } => "memcached",
        }
    }

    pub(crate) fn systemd_name(&self) -> Option<&'static str> {
        match *self {
            Backend::Memcached => Some("memcached"),
            Backend::Mysql => Some("mysqld"),
            Backend::Mssql => Some("mssql-server"),
            Backend::Netsoup { .. } => None,
        }
    }

    pub(crate) fn uniq_name(&self) -> String {
        match *self {
            Backend::Netsoup {
                readers,
                workers,
                shards,
            } => format!("netsoup_{}r_{}w_{}s", readers, workers, shards.unwrap_or(0)),
            Backend::Memcached | Backend::Mysql | Backend::Mssql => {
                self.multiclient_name().to_string()
            }
        }
    }

    pub(crate) fn port(&self) -> u16 {
        match *self {
            Backend::Netsoup { .. } => unreachable!(),
            Backend::Memcached => 11211,
            Backend::Mysql => 3306,
            Backend::Mssql => 1433,
        }
    }
}
