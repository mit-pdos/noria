use clap;

pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) articles: usize,
}

pub(crate) trait VoteClientConstructor {
    type Instance: VoteClient;
    fn new(&Parameters, &clap::ArgMatches) -> Self;
    fn make(&mut self) -> Self::Instance;
    fn spawns_threads() -> bool {
        false
    }
}

pub(crate) trait VoteClient {
    fn handle_reads(&mut self, requests: &[i32]);
    fn handle_writes(&mut self, requests: &[i32]);
}

pub(crate) mod hybrid;
pub(crate) mod localsoup;
pub(crate) mod memcached;
pub(crate) mod mssql;
pub(crate) mod mysql;
pub(crate) mod netsoup;
