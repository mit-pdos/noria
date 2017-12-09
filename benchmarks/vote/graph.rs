use time;

use distributary::{self, Blender, ControllerBuilder, NodeIndex, PersistenceParameters};

pub struct Graph {
    setup: Setup,
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: Blender,
}

#[derive(Clone)]
pub struct Setup {
    pub transactions: bool,
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub local: bool,
    pub nworkers: usize,
    pub logging: bool,
    pub randomize_ports: bool,
    pub concurrent_replays: usize,
    pub replay_batch_timeout: time::Duration,
    pub replay_batch_size: usize,
}

impl Setup {
    pub fn new(local: bool, nworkers: usize) -> Self {
        Setup {
            transactions: false,
            stupid: false,
            partial: true,
            sharding: None,
            logging: false,
            randomize_ports: false,
            local,
            nworkers,
            concurrent_replays: 512,
            replay_batch_timeout: time::Duration::from_millis(1),
            replay_batch_size: 32,
        }
    }
}

impl Setup {
    #[allow(dead_code)]
    pub fn enable_logging(mut self) -> Self {
        self.logging = true;
        self
    }

    #[allow(dead_code)]
    pub fn set_max_concurrent_replay(mut self, n: usize) -> Self {
        self.concurrent_replays = n;
        self
    }

    #[allow(dead_code)]
    pub fn set_partial_replay_batch_timeout(mut self, t: time::Duration) -> Self {
        self.replay_batch_timeout = t;
        self
    }

    #[allow(dead_code)]
    pub fn set_partial_replay_batch_size(mut self, n: usize) -> Self {
        self.replay_batch_size = n;
        self
    }

    #[allow(dead_code)]
    pub fn with_transactions(mut self) -> Self {
        self.transactions = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_stupidity(mut self) -> Self {
        self.stupid = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_sharding(mut self, s: usize) -> Self {
        self.sharding = Some(s);
        self
    }

    #[allow(dead_code)]
    pub fn without_partial(mut self) -> Self {
        self.partial = false;
        self
    }

    #[allow(dead_code)]
    pub fn without_sharding(mut self) -> Self {
        self.sharding = None;
        self
    }

    #[allow(dead_code)]
    pub fn with_random_ports(mut self) -> Self {
        self.randomize_ports = true;
        self
    }
}

pub fn make(s: Setup, persistence_params: PersistenceParameters) -> Graph {
    let mut g = ControllerBuilder::default();
    if !s.partial {
        g.disable_partial();
    }
    if let Some(shards) = s.sharding {
        g.enable_sharding(shards);
    }
    g.set_persistence(persistence_params);
    if s.local {
        g.set_local_workers(s.nworkers);
    } else {
        g.set_nworkers(s.nworkers);
    }
    if s.logging {
        g.log_with(distributary::logger_pls());
    }
    if s.randomize_ports {
        g.set_internal_port(0);
        g.set_external_port(0);
    }
    let graph = g.build();

    let recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (id int, user int, PRIMARY KEY(id));

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.id, COUNT(user) AS votes \
                                       FROM Vote GROUP BY Vote.id) AS VoteCount \
                            ON (Article.id = VoteCount.id) WHERE Article.id = ?;";

    graph.install_recipe(recipe.to_owned());
    let inputs = graph.inputs();
    let outputs = graph.outputs();

    if s.logging {
        println!("inputs {:?}", inputs);
        println!("outputs {:?}", outputs);
    }

    Graph {
        vote: inputs["Vote"],
        article: inputs["Article"],
        end: outputs["ArticleWithVoteCount"],
        graph,
        setup: s,
    }
}

impl Graph {
    #[allow(dead_code)]
    pub fn transition(&mut self) -> (NodeIndex, NodeIndex) {
        // TODO(fintelia): Port non-stupid migration to SQL expression.
        let stupid_recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (id int, user int, PRIMARY KEY(id));
               CREATE TABLE Rating (id int, user int, stars int, PRIMARY KEY(id));

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.id, COUNT(user) AS votes \
                                       FROM Vote GROUP BY Vote.id) AS VoteCount \
                            ON (Article.id = VoteCount.id) WHERE Article.id = ?;
               U: SELECT id, stars FROM Rating UNION SELECT id, 1 FROM Vote;
               QUERY ArticleWithScore: SELECT Article.id, title, Total.score AS score \
                            FROM Article \
                            LEFT JOIN (SELECT id, SUM(stars) AS score \
                                       FROM U \
                                       GROUP BY id) AS Total
                            ON (Article.id = Total.id) \
                            WHERE Article.id = ?;";

        let smart_recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (id int, user int, PRIMARY KEY(id));
               CREATE TABLE Rating (id int, user int, stars int, PRIMARY KEY(id));

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.id, COUNT(user) AS votes \
                                       FROM Vote GROUP BY Vote.id) AS VoteCount \
                            ON (Article.id = VoteCount.id) WHERE Article.id = ?;

               RatingSum: SELECT id, SUM(stars) AS score FROM Rating GROUP BY id;
               U: SELECT id, score FROM RatingSum UNION SELECT id, votes FROM VoteCount;
               QUERY ArticleWithScore: SELECT Article.id, title, SUM(U.score) AS score \
                            FROM Article, U \
                            WHERE Article.id = U.id AND Article.id = ? \
                            GROUP BY Article.id;";


        if self.setup.stupid {
            self.graph.install_recipe(stupid_recipe.to_owned());
        } else {
            self.graph.install_recipe(smart_recipe.to_owned());
        }

        let inputs = self.graph.inputs();
        let outputs = self.graph.outputs();
        (inputs["Rating"], outputs["ArticleWithScore"])
        // self.graph.migrate(|mig| {
        //     // add new "ratings" base table
        //     let b = Base::default().with_key(vec![1]);
        //     let rating = if setup.transactions {
        //         mig.add_transactional_base("rating", &["user", "id", "stars"], b)
        //     } else {
        //         mig.add_ingredient("rating", &["user", "id", "stars"], b)
        //     };

        //     let total = if setup.stupid {
        //         // project on 1 to votes
        //         let upgrade = mig.add_ingredient(
        //             "upvote",
        //             &["id", "one"],
        //             Project::new(vote, &[1], Some(vec![1.into()]), None),
        //         );

        //         // take a union of votes and ratings
        //         let mut emits = HashMap::new();
        //         emits.insert(rating, vec![1, 2]);
        //         emits.insert(upgrade, vec![0, 1]);
        //         let u = Union::new(emits);
        //         let both = mig.add_ingredient("both", &["id", "value"], u);

        //         // add sum of combined ratings
        //         mig.add_ingredient(
        //             "total",
        //             &["id", "total"],
        //             Aggregation::SUM.over(both, 1, &[0]),
        //         )
        //     } else {
        //         // add sum of ratings
        //         let rs = mig.add_ingredient(
        //             "rsum",
        //             &["id", "total"],
        //             Aggregation::SUM.over(rating, 2, &[1]),
        //         );

        //         // take a union of vote count and rsum
        //         let mut emits = HashMap::new();
        //         emits.insert(rs, vec![0, 1]);
        //         emits.insert(vc, vec![0, 1]);
        //         let u = Union::new(emits);
        //         let both = mig.add_ingredient("both", &["id", "value"], u);

        //         // sum them by article id
        //         mig.add_ingredient(
        //             "total",
        //             &["id", "total"],
        //             Aggregation::SUM.over(both, 1, &[0]),
        //         )
        //     };

        //     // finally, produce end result
        //     use distributary::JoinSource::*;
        //     let j = Join::new(article, total, JoinType::Left, vec![B(0, 0), L(1), R(1)]);
        //     let newend = mig.add_ingredient("awr", &["id", "title", "score"], j);
        //     mig.maintain(newend, 0);
        //     (rating, newend)
        // })
    }
}
