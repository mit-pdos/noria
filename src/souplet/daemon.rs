
#[macro_use]
extern crate clap;
extern crate distributary;
extern crate mio;

use std::net::SocketAddr;

use distributary::SoupletDaemon;

pub fn main() {
    use clap::{Arg, App};
    let args = App::new("soup-daemon")
        .version("0.1")
        .arg(Arg::with_name("port")
                 .short("p")
                 .long("port")
                 .default_value("1026")
                 .help("port to listen on"))
        .arg(Arg::with_name("ip")
                 .long("ip")
                 .default_value("127.0.0.1")
                 .help("port to listen on"))
        .get_matches();

    let ip = args.value_of("ip").unwrap().parse().unwrap();
    let port = value_t_or_exit!(args, "port", u16);
    SoupletDaemon::start(SocketAddr::new(ip, port));
}
