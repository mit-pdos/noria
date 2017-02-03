use std::sync;

use distributary::{Blender, Base, Aggregation, JoinBuilder, Union, DataType, NodeAddress, Mutator};

use targets::Backend;
use targets::Putter;
use targets::Getter;

use std::collections::HashMap;

type Put = Box<Fn(Vec<DataType>) + Send + 'static>;
type Get = Box<Fn(&DataType) -> Result<Vec<Vec<DataType>>, ()> + Send + Sync>;

pub struct SoupTarget {
    vci: NodeAddress,
    articlei: NodeAddress,

    vote: Mutator,
    article: Mutator,
    end: sync::Arc<Get>,
    _g: Blender,
}

pub fn make(_: &str, _: usize) -> SoupTarget {
    // set up graph
    let mut g = Blender::new();

    let article;
    let vote;
    let vc;
    let end;
    let (articlei, vci, endq) = {
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

        let endq = mig.maintain(end, 0);

        // start processing
        mig.commit();
        (article, vc, endq)
    };

    SoupTarget {
        articlei: articlei,
        vci: vci,
        vote: g.get_mutator(vote),
        article: g.get_mutator(article),
        end: sync::Arc::new(endq),
        _g: g, // so it's not dropped and waits for threads
    }
}

impl Backend for SoupTarget {
    type P = (Put, Option<Put>);
    type G = sync::Arc<Get>;

    fn getter(&mut self) -> Self::G {
        self.end.clone()
    }

    fn putter(&mut self) -> Self::P {
        let v = self.vote.clone();
        let vote = Box::new(move |u: Vec<DataType>|{
            v.put(u)
        });

        let a = self.article.clone();
        let article = Box::new(move |u: Vec<DataType>|{
            a.put(u)
        });

        (vote, Some(article))
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        let (rating, newendq) = {
            // migrate
            let mut mig = self._g.start_migration();

            // add new "ratings" base table
            let rating = mig.add_ingredient("rating", &["user", "id", "stars"], Base {});

            // add sum of ratings
            let rs = mig.add_ingredient("rsum",
                                        &["id", "total"],
                                        Aggregation::SUM.over(rating, 2, &[1]));

            // take a union of vote count and rsum
            let mut emits = HashMap::new();
            emits.insert(rs, vec![0, 1]);
            emits.insert(self.vci, vec![0, 1]);
            let u = Union::new(emits);
            let both = mig.add_ingredient("both", &["id", "value"], u);

            // sum them by article id
            let total = mig.add_ingredient("total",
                                           &["id", "total"],
                                           Aggregation::SUM.over(both, 1, &[0]));

            // finally, produce end result
            let j = JoinBuilder::new(vec![(self.articlei, 0), (self.articlei, 1), (total, 1)])
                .from(self.articlei, vec![1, 0])
                .join(total, vec![1, 0]);
            let newend = mig.add_ingredient("awr", &["id", "title", "score"], j);
            let newendq = mig.maintain(newend, 0);

            // we want ratings, rsum, and the union to be in the same domain,
            // because only rsum is really costly
            let domain = mig.add_domain();
            mig.assign_domain(rating, domain);
            mig.assign_domain(rs, domain);
            mig.assign_domain(both, domain);

            // and then we want the total sum and the join in the same domain,
            // to avoid duplicating the total state
            let domain = mig.add_domain();
            mig.assign_domain(total, domain);
            mig.assign_domain(newend, domain);

            // start processing
            mig.commit();
            (rating, newendq)
        };

        let mutator = self._g.get_mutator(rating);
        let put = Box::new(move |u: Vec<DataType>| {
            mutator.put(u);
        });


        let newendq = sync::Arc::new(newendq);
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

impl Getter for sync::Arc<Get> {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        Box::new(move |id| {
            let id = id.into();
            self(&id).map(|g| {
                g.into_iter().next().map(|row| {
                    // we only care about the first result
                    let mut row = row.into_iter();
                    let id: i64 = row.next().unwrap().into();
                    let title: String = row.next().unwrap().into();
                    let count: i64 = row.next().unwrap().into();
                    (id, title, count)
                })
            })
        })
    }
}
