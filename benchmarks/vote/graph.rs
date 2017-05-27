use distributary::{Blender, Base, BaseDurabilityLevel, Aggregation, Join, JoinType, NodeAddress};
use distributary;

pub struct Graph {
    pub vote: NodeAddress,
    pub article: NodeAddress,
    pub vc: NodeAddress,
    pub end: NodeAddress,
    pub graph: Blender,
}

pub fn make(log: bool, transactions: bool, durability: Option<BaseDurabilityLevel>) -> Graph {
    // set up graph
    let mut g = Blender::new();
    if log {
        g.log_with(distributary::logger_pls());
    }

    let (article, vote, vc, end) = {
        // migrate
        let mut mig = g.start_migration();

        // add article base node
        let mut b = Base::default();
        if let Some(d) = durability {
            b = b.with_durability(d).delete_log_on_drop();
        }
        let article = if transactions {
            mig.add_transactional_base("article", &["id", "title"], b)
        } else {
            mig.add_ingredient("article", &["id", "title"], b)
        };

        // add vote base table
        let mut b = Base::default().with_key(vec![1]);
        if let Some(d) = durability {
            b = b.with_durability(d).delete_log_on_drop();
        }
        let vote = if transactions {
            mig.add_transactional_base("vote", &["user", "id"], b)
        } else {
            mig.add_ingredient("vote", &["user", "id"], b)
        };

        // add vote count
        let vc = mig.add_ingredient("votecount",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join using first field from article and first from vc
        use distributary::JoinSource::*;
        let j = Join::new(article, vc, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        mig.maintain(end, 0);

        // start processing
        mig.commit();
        (article, vote, vc, end)
    };

    Graph {
        vote: vote.into(),
        article: article.into(),
        vc: vc.into(),
        end: end.into(),
        graph: g,
    }
}

impl Graph {
    #[allow(dead_code)]
    pub fn transition(&mut self, stupid: bool, transactions: bool) -> (NodeAddress, NodeAddress) {
        use distributary::{Base, Aggregation, Join, JoinType, Union, Project};
        use std::collections::HashMap;

        // get all the ids since migration will borrow self
        let vc = self.vc;
        let vote = self.vote;
        let article = self.article;

        // migrate
        let mut mig = self.graph.start_migration();

        // add new "ratings" base table
        let b = Base::default().with_key(vec![1]);
        let rating = if transactions {
            mig.add_transactional_base("rating", &["user", "id", "stars"], b)
        } else {
            mig.add_ingredient("rating", &["user", "id", "stars"], b)
        };

        let total = if stupid {
            // project on 1 to votes
            let upgrade =
                mig.add_ingredient("upvote",
                                   &["id", "one"],
                                   Project::new(vote, &[1], Some(vec![1.into()])));

            // take a union of votes and ratings
            let mut emits = HashMap::new();
            emits.insert(rating, vec![1, 2]);
            emits.insert(upgrade, vec![0, 1]);
            let u = Union::new(emits);
            let both = mig.add_ingredient("both", &["id", "value"], u);

            // add sum of combined ratings
            mig.add_ingredient("total",
                               &["id", "total"],
                               Aggregation::SUM.over(both, 1, &[0]))
        } else {
            // add sum of ratings
            let rs = mig.add_ingredient("rsum",
                                        &["id", "total"],
                                        Aggregation::SUM.over(rating, 2, &[1]));

            // take a union of vote count and rsum
            let mut emits = HashMap::new();
            emits.insert(rs, vec![0, 1]);
            emits.insert(vc, vec![0, 1]);
            let u = Union::new(emits);
            let both = mig.add_ingredient("both", &["id", "value"], u);

            // sum them by article id
            mig.add_ingredient("total",
                               &["id", "total"],
                               Aggregation::SUM.over(both, 1, &[0]))
        };

        // finally, produce end result
        use distributary::JoinSource::*;
        let j = Join::new(article, total, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        let newend = mig.add_ingredient("awr", &["id", "title", "score"], j);
        mig.maintain(newend, 0);

        // start processing
        mig.commit();
        (rating, newend)
    }
}
