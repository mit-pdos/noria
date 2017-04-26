
#[macro_use]
extern crate clap;
extern crate distributary;
extern crate mio;

use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use distributary::WorkerDaemon;

pub fn main() {
    use clap::{Arg, App};
    let args = App::new("soup-daemon")
        .version("0.1")
        .arg(Arg::with_name("port")
                 .short("p")
                 .long("port")
                 .default_value("1025")
                 .help("port to listen on"))
        .get_matches();

    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let port = value_t_or_exit!(args, "port", u16);
    WorkerDaemon::start(SocketAddr::new(ip, port));
}
