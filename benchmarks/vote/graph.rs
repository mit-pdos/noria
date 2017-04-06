use distributary::{Blender, Base, Aggregation, Join, JoinType, NodeAddress};

use slog;
use slog_term;
use slog::DrainExt;

pub struct Graph {
    pub vote: NodeAddress,
    pub article: NodeAddress,
    pub vc: NodeAddress,
    pub end: NodeAddress,
    pub graph: Blender,
}

pub fn make() -> Graph {
    // set up graph
    let mut g = Blender::new();
    g.log_with(slog::Logger::root(slog_term::streamer().full().build().fuse(), None));

    let (article, vote, vc, end) = {
        // migrate
        let mut mig = g.start_migration();

        // add article base node
        let article = mig.add_ingredient("article", &["id", "title"], Base::default());

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

        // add vote count
        let vc = mig.add_ingredient("votecount",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join using first field from article and first from vc
        use distributary::JoinSource::*;
        let j = Join::new(article, vc, JoinType::Inner, vec![B(0, 0), L(1), R(1)]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

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
