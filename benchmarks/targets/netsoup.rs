use std::collections::HashMap;

use distributary::srv;
use distributary::{FlowGraph, new, Query, Base, Aggregation, Joiner, DataType};
use shortcut;
use tarpc;

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub struct SoupTarget<D: tarpc::transport::Dialer> {
    vote: usize,
    article: usize,
    end: usize,
    addr: String,
    _srv: tarpc::ServeHandle<D>, // so the server won't quit
}

pub fn make(addr: &str, _: usize) -> Box<Backend> {
    // set up graph
    let mut g = FlowGraph::new();

    // add article base node
    let article = g.incorporate(new("article", &["id", "title"], true, Base {}), vec![]);

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}), vec![]);

    // add vote count
    let q = Query::new(&[true, true], Vec::new());
    let vc = g.incorporate(new("vc",
                               &["id", "votes"],
                               true,
                               Aggregation::COUNT.new(vote, 0, 2)),
                           vec![(q, vote)]);

    // add final join
    let mut join = HashMap::new();
    // if article joins against vote count, query and join using article's first field
    join.insert(article, vec![(article, vec![0]), (vc, vec![0])]);
    // if vote count joins against article, also query and join on the first field
    join.insert(vc, vec![(vc, vec![0]), (article, vec![0])]);
    // emit first and second field from article (id + title)
    // and second field from right (votes)
    let emit = vec![(article, 0), (article, 1), (vc, 1)];
    let j = Joiner::new(emit, join);
    // query to article/vc should select all fields, and query on id
    let q = Query::new(&[true, true],
                       vec![shortcut::Condition {
                            column: 0,
                            cmp:
                                shortcut::Comparison::Equal(shortcut::Value::Const(DataType::None)),
                        }]);
    let end = g.incorporate(new("awvc", &["id", "title", "votes"], true, j),
                            vec![(q.clone(), article), (q, vc)]);


    // start processing
    Box::new(SoupTarget {
        vote: vote.index(),
        article: article.index(),
        end: end.index(),
        addr: addr.to_owned(),
        _srv: srv::run(g, addr),
    })
}

impl<D: tarpc::transport::Dialer> Backend for SoupTarget<D> {
    fn getter(&mut self) -> Box<Getter> {
        Box::new((srv::ext::Client::new(&self.addr).unwrap(), self.end))
    }

    fn putter(&mut self) -> Box<Putter> {
        Box::new((srv::ext::Client::new(&self.addr).unwrap(), self.vote, self.article))
    }
}

impl Putter for (srv::ext::Client, usize, usize) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            self.0.insert(self.2, vec![id.into(), title.into()]).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.0.insert(self.1, vec![user.into(), id.into()]).unwrap();
        })
    }
}

impl Getter for (srv::ext::Client, usize) {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            for row in self.0.query(self.1, Some(vec![Some(id.into())])).unwrap().into_iter() {
                match row[1] {
                    DataType::Text(ref s) => {
                        return Some((row[0].clone().into(), (**s).clone(), row[2].clone().into()));
                    }
                    _ => unreachable!(),
                }
            }
            None
        })
    }
}
