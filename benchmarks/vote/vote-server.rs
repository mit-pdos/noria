#![feature(conservative_impl_trait, plugin)]
#![plugin(tarpc_plugins)]

extern crate clap;

extern crate distributary;
extern crate tarpc;

extern crate slog;
extern crate slog_term;

mod graph;

use distributary::srv;
use tarpc::util::FirstSocketAddr;

fn main() {
    use clap::{Arg, App};
    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for different storage \
                backends.")
        .arg(Arg::with_name("ADDR")
                 .index(1)
                 .help("Address and port to listen on")
                 .required(true))
        .get_matches();

    let addr = args.value_of("ADDR").unwrap();
    println!("Attempting to start soup on {}", addr);
    let g = graph::make(false);

    // start processing
    // TODO: what about the node indices?
    srv::run(g.graph, addr.first_socket_addr());
}
