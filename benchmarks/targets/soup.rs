use std::sync;

use distributary::{Blender, Base, Aggregation, JoinBuilder, DataType, NodeAddress};

use targets::Backend;
use targets::Putter;
use targets::Getter;

type Put = Box<Fn(Vec<DataType>) + Send + 'static>;
type Get = Box<Fn(&DataType) -> Vec<Vec<DataType>> + Send + Sync>;

pub struct SoupTarget {
    vci: NodeAddress,
    articlei: NodeAddress,

    vote: Option<Put>,
    article: Option<Put>,
    end: sync::Arc<Option<Get>>,
    _g: Blender,
}

pub fn make(_: &str, _: usize) -> SoupTarget {
    // set up graph
    let mut g = Blender::new();

    let article;
    let vote;
    let vc;
    let end;
    let (mut put, articlei, vci, endq) = {
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

        if cfg!(feature = "no_domains") {
            let d = mig.add_domain();
            mig.assign_domain(article, d);
            mig.assign_domain(vote, d);
            mig.assign_domain(end, d);
            mig.assign_domain(vc, d);
        } else {
            // let's try to be clever about this.
            //
            // article and the join should certainly be together, since article is dormant after
            // setup, this is purely a win. plus it avoids duplicating the article state
            //
            // NOTE: this domain will become dormant once migration has finished, which is good!
            let ad = mig.add_domain();
            mig.assign_domain(article, ad);
            mig.assign_domain(end, ad);
            // vote and votecount may as well be together since that's where the most number of
            // puts will flow.
            let vd = mig.add_domain();
            mig.assign_domain(vote, vd);
            mig.assign_domain(vc, vd);
            // the real question is whether these *two* domains should be joined.
            // it's not entirely clear. for now, let's keep them separate to allow the aggregation
            // and the join to occur in parallel.
        }

        let endq = if cfg!(feature = "rtm") {
            None
        } else {
            Some(mig.maintain(end, 0))
        };

        // start processing
        (mig.commit(), article, vc, endq)
    };

    SoupTarget {
        articlei: articlei,
        vci: vci,
        vote: put.remove(&vote).map(|x| x.0),
        article: put.remove(&article).map(|x| x.0),
        end: sync::Arc::new(endq),
        _g: g, // so it's not dropped and waits for threads
    }
}

impl Backend for SoupTarget {
    type P = (Put, Option<Put>);
    type G = sync::Arc<Option<Get>>;

    fn getter(&mut self) -> Self::G {
        self.end.clone()
    }

    fn putter(&mut self) -> Self::P {
        (self.vote.take().unwrap(), Some(self.article.take().unwrap()))
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        let (put, newendq) = {
            // migrate
            let mut mig = self._g.start_migration();

            let domain = mig.add_domain();

            // add new "ratings" base table
            let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base {});

            // add sum of ratings
            let rs = mig.add_ingredient("rsum",
                                        &["id", "total"],
                                        Aggregation::SUM.over(rating, 2, &[1]));

            // join vote count and rsum (and in theory, sum them)
            let j = JoinBuilder::new(vec![(rs, 0), (rs, 1), (self.vci, 1)])
                .from(rs, vec![1, 0])
                .join(self.vci, vec![1, 0]);
            let total = mig.add_ingredient("total", &["id", "ratings", "votes"], j);

            mig.assign_domain(rating, domain);
            mig.assign_domain(rs, domain);
            mig.assign_domain(total, domain);

            // finally, produce end result
            let j = JoinBuilder::new(vec![(self.articlei, 0),
                                          (self.articlei, 1),
                                          (total, 1),
                                          (total, 2)])
                .from(self.articlei, vec![1, 0])
                .join(total, vec![1, 0, 0]);
            let newend = mig.add_ingredient("awr", &["id", "title", "ratings", "votes"], j);
            let newendq = mig.maintain(newend, 0);

            // start processing
            (mig.commit().remove(&rating).unwrap().0, newendq)
        };

        let newendq = sync::Arc::new(Some(newendq));
        ((put, None), (0..ngetters).into_iter().map(|_| newendq.clone()).collect())
    }
}

impl Putter for (Put, Option<Put>) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let articles = self.1.as_mut().expect("article putter is only available before migrations");
        Box::new(move |id, title| { articles(vec![id.into(), title.into()]); })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        if self.1.is_some() {
            // pre-migration
            Box::new(move |user, id| { self.0(vec![user.into(), id.into()]); })
        } else {
            // post-migration
            Box::new(move |user, id| { self.0(vec![user.into(), id.into(), 5.into()]); })
        }
    }
}

impl Getter for sync::Arc<Option<Get>> {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            if let Some(ref g) = *self.as_ref() {
                let id = id.into();
                g(&id).into_iter().next().map(|row| {
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
