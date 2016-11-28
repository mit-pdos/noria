use std::sync;

use distributary::{Blender, Query, Base, Aggregation, JoinBuilder, DataType};
use shortcut;

use targets::Backend;
use targets::Putter;
use targets::Getter;

type Put = Box<Fn(Vec<DataType>) + Send + 'static>;
type Get = Box<Fn(Option<&Query>) -> Vec<Vec<DataType>> + Send + Sync>;

pub struct SoupTarget {
    vote: Option<Put>,
    article: Option<Put>,
    end: sync::Arc<Option<Get>>,
    _g: Blender,
}

pub fn make(_: &str, _: usize) -> Box<Backend> {
    // set up graph
    let mut g = Blender::new();

    let article;
    let vote;
    let vc;
    let end;
    let (mut put, mut get) = {
        // migrate
        let mut mig = g.start_migration();

        // add article base node
        article = mig.add_ingredient("article", &["id", "title"], Base {});

        // add vote base table
        vote = mig.add_ingredient("vote", &["user", "id"], Base {});

        // add vote count
        vc = mig.add_ingredient("votecount",
                                &["id", "votes"],
                                Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join using first field from article and first from vc
        let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
            .from(article, vec![1, 0])
            .join(vc, vec![1, 0]);
        end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        // let d = mig.add_domain();
        // mig.assign_domain(article, d);
        // mig.assign_domain(end, d);
        // mig.assign_domain(vc, d);

        // start processing
        mig.commit()
    };

    Box::new(SoupTarget {
        vote: put.remove(&vote),
        article: put.remove(&article),
        end: sync::Arc::new(get.remove(&end)),
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
        Box::new(move |id, title| { self.1(vec![id.into(), title.into()]); })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| { self.0(vec![user.into(), id.into()]); })
    }
}

impl Getter for sync::Arc<Option<Get>> {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            if let Some(ref g) = *self.as_ref() {
                let q = Query::new(&[true, true, true],
                                   vec![shortcut::Condition {
                             column: 0,
                             cmp:
                                 shortcut::Comparison::Equal(shortcut::Value::new(DataType::from(id))),
                         }]);
                for row in g(Some(&q)) {
                    match row[1] {
                        DataType::Text(ref s) => {
                            return Some((row[0].clone().into(),
                                         (**s).clone(),
                                         row[2].clone().into()));
                        }
                        _ => unreachable!(),
                    }
                }
            }
            None
        })
    }
}
