extern crate noria;

use noria::ControllerHandle;


fn main() {
    println!("Get contrller handle");
    let mut srv = ControllerHandle::from_zk("127.0.0.1:2181/lobsters").unwrap();
    println!("Getting comments view");
    let mut comments = srv.view("comment_votes").unwrap();
    println!("Testing comments table");
    let aid = 1;
    let search = comments.lookup(&[aid.into()], true).unwrap();
}

