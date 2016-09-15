use std::collections::HashMap;
use std::sync;

use distributary;
use distributary::{FlowGraph, new, Query, Base, Aggregation, Joiner, DataType};
use clocked_dispatch;
use shortcut;

use Backend;
use Putter;
use Getter;

type Put = clocked_dispatch::ClockedSender<Vec<DataType>>;
type Get = Box<Fn(Option<Query>) -> Vec<Vec<DataType>> + Send + Sync>;
type FG = FlowGraph<Query,
                    distributary::Update,
                    Vec<distributary::DataType>,
                    Vec<shortcut::Value<distributary::DataType>>>;

pub struct SoupTarget {
    vote: Option<Put>,
    article: Option<Put>,
    end: sync::Arc<Get>,
    _g: FG,
}

pub fn make(_: &str, _: usize) -> Box<Backend> {
    // set up graph
    let mut g = FlowGraph::new();

    // add article base node
    let article = g.incorporate(new(&["id", "title"], true, Base {}), vec![]);

    // add vote base table
    let vote = g.incorporate(new(&["user", "id"], true, Base {}), vec![]);

    // add vote count
    let q = Query::new(&[true, true], Vec::new());
    let vc = g.incorporate(new(&["id", "votes"], true, Aggregation::COUNT.new(vote, 0, 2)),
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
    let end = g.incorporate(new(&["id", "title", "votes"], true, j),
                            vec![(q.clone(), article), (q, vc)]);


    // start processing
    let (mut put, mut get) = g.run(10);

    Box::new(SoupTarget {
        vote: put.remove(&vote),
        article: put.remove(&article),
        end: sync::Arc::new(get.remove(&end).unwrap()),
        _g: g, // so it's not dropped and waits for threads
    })
}

impl Backend for SoupTarget {
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
