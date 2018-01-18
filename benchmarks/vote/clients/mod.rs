use clap;
use std::time;

pub(crate) struct Parameters {
    pub(crate) prime: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) ratio: u32,
    pub(crate) articles: usize,
}

pub(crate) trait VoteClient {
    type Constructor;

    fn new(&Parameters, &clap::ArgMatches) -> Self::Constructor;
    fn from(&mut Self::Constructor) -> Self;
    fn handle_reads(&mut self, requests: &[(time::Instant, i32)]);
    fn handle_writes(&mut self, requests: &[(time::Instant, i32)]);

    fn spawns_threads() -> bool {
        false
    }
}

pub(crate) mod localsoup;
pub(crate) mod netsoup;
pub(crate) mod mysql;
