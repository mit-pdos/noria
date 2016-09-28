use std::collections::HashMap;
use std::sync;

use distributary::{FlowGraph, new, Query, Base, Aggregation, Joiner, DataType};
use clocked_dispatch;
use shortcut;

use targets::Backend;
use targets::Putter;
use targets::Getter;

type Put = clocked_dispatch::ClockedSender<Vec<DataType>>;
type Get = Box<Fn(Option<Query>) -> Vec<Vec<DataType>> + Send + Sync>;
type FG<U> = FlowGraph<Query, U, Vec<DataType>>;

pub struct SoupTarget<U: Send + Clone> {
    vote: Option<Put>,
    article: Option<Put>,
    end: sync::Arc<Get>,
    _g: FG<U>,
}

pub fn make(_: &str, _: usize) -> Box<Backend> {
    // set up graph
    let mut g = FlowGraph::new();

    // add article base node
    let article = g.incorporate(new("article", &["id", "title"], true, Base {}));

    // add vote base table
    let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));

    // add vote count
    let vc = g.incorporate(new("votecount", &["id", "votes"], true, Aggregation::COUNT.new(vote, 0)));

    // add final join using first field from article and first from vc
    let mut join = HashMap::new();
    join.insert(article, vec![1, 0]);
    join.insert(vc, vec![1, 0]);
    // emit first and second field from article (id + title)
    // and second field from right (votes)
    let emit = vec![(article, 0), (article, 1), (vc, 1)];
    let j = Joiner::new(emit, join);
    let end = g.incorporate(new("awvc", &["id", "title", "votes"], true, j));

    // start processing
    let (mut put, mut get) = g.run(10);

    Box::new(SoupTarget {
        vote: put.remove(&vote),
        article: put.remove(&article),
        end: sync::Arc::new(get.remove(&end).unwrap()),
        _g: g, // so it's not dropped and waits for threads
    })
}

impl<U: Send + Clone> Backend for SoupTarget<U> {
    fn getter(&mut self) -> Box<Getter> {
        Box::new(self.end.clone())
    }

    fn putter(&mut self) -> Box<Putter> {
        Box::new((self.vote.take().unwrap(), self.article.take().unwrap()))
    }
}

impl Putter for (Put, Put) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            self.1.send(vec![id.into(), title.into()]);
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.0.send(vec![user.into(), id.into()]);
        })
    }
}

impl Getter for sync::Arc<Get> {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            let q = Query::new(&[true, true, true],
                               vec![shortcut::Condition {
                             column: 0,
                             cmp:
                                 shortcut::Comparison::Equal(shortcut::Value::Const(id.into())),
                         }]);
            for row in self(Some(q)).into_iter() {
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
