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
    let (mut put, endq) = {
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

        let endq = if cfg!(feature = "rtm") {
            let d = mig.add_domain();
            mig.assign_domain(article, d);
            mig.assign_domain(vote, d);
            mig.assign_domain(end, d);
            mig.assign_domain(vc, d);
            None
        } else {
            Some(mig.maintain(end))
        };

        // start processing
        (mig.commit(), endq)
    };

    Box::new(SoupTarget {
        vote: put.remove(&vote),
        article: put.remove(&article),
        end: sync::Arc::new(endq),
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

                g(Some(&q)).into_iter().next().map(|row| {
                    // we only care about the first result
                    let mut row = row.into_iter();
                    let id: i64 = row.next().unwrap().into();
                    let title: String = row.next().unwrap().into();
                    let count: i64 = row.next().unwrap().into();
                    (id, title, count)
                })
            } else {
                use std::time::Duration;
                use std::thread;
                // avoid spinning
                thread::sleep(Duration::from_secs(1));
                None
            }
        })
    }
}
