use std::fs::File;
use std::sync;

use distributary::jdl;

use targets::Backend;
use targets::soup::SoupTarget;

pub fn make(dbn: &str, _: usize) -> Box<Backend> {
    let file = match File::open(&dbn) {
        Err(msg) => panic!("{}", msg),
        Ok(f) => f,
    };

    let mut g = jdl::parse(file);

    let vote = g.lookup_node("vote");
    let article = g.lookup_node("article");
    let awvc = g.lookup_node("awvc");

    let (mut put, mut get) = g.run(10);

    Box::new(SoupTarget {
        vote: put.remove(&vote),
        article: put.remove(&article),
        end: sync::Arc::new(get.remove(&awvc).unwrap()),
        _g: g, // so it's not dropped and waits for threads
    })
}
