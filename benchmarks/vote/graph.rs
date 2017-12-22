use time;

use distributary::{self, ControllerBuilder, ControllerHandle, LocalAuthority, NodeIndex,
                   PersistenceParameters};

pub struct Graph {
    setup: Setup,
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: ControllerHandle<LocalAuthority>,
}

pub struct Setup {
    pub transactions: bool,
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub local: bool,
    pub nworkers: usize,
    pub nreaders: usize,
    pub logging: bool,
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
            local,
            nworkers,
            nreaders: 1,
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
    pub fn set_read_threads(mut self, n: usize) -> Self {
        self.nreaders = n;
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
    g.set_local_read_threads(s.nreaders);
    if s.logging {
        g.log_with(distributary::logger_pls());
    }
    let mut graph = g.build_local();

    let recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (id int, user int, PRIMARY KEY(id));

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.id, COUNT(user) AS votes \
                                       FROM Vote GROUP BY Vote.id) AS VoteCount \
                            ON (Article.id = VoteCount.id) WHERE Article.id = ?;";

    graph.install_recipe(recipe.to_owned()).unwrap();
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

        // TODO(malte): ends up with partial-above-full due to compound group by key in post-join
        // aggregation.
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
            self.graph.install_recipe(stupid_recipe.to_owned()).unwrap();
        } else {
            self.graph.install_recipe(smart_recipe.to_owned()).unwrap();
        }

        let inputs = self.graph.inputs();
        let outputs = self.graph.outputs();
        (inputs["Rating"], outputs["ArticleWithScore"])
    }
}
