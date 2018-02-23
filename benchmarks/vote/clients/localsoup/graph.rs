use distributary::{self, ControllerBuilder, ControllerHandle, LocalAuthority, NodeIndex,
                   PersistenceParameters};

pub(crate) const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (id int, user int, PRIMARY KEY(id));

# read queries
QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN (SELECT Vote.id, COUNT(user) AS votes \
                       FROM Vote GROUP BY Vote.id) AS VoteCount \
            ON (Article.id = VoteCount.id) WHERE Article.id = ?;";

pub struct Graph {
    setup: Setup,
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: ControllerHandle<LocalAuthority>,
}

pub struct Setup {
    pub stupid: bool,
    pub partial: bool,
    pub sharding: Option<usize>,
    pub nworkers: usize,
    pub nreaders: usize,
    pub logging: bool,
}

impl Default for Setup {
    fn default() -> Self {
        Setup {
            stupid: false,
            partial: true,
            sharding: None,
            logging: false,
            nworkers: 1,
            nreaders: 1,
        }
    }
}

pub fn make(s: Setup, persistence_params: PersistenceParameters) -> Graph {
    let mut g = ControllerBuilder::default();
    if !s.partial {
        g.disable_partial();
    }
    g.set_sharding(s.sharding);
    g.set_persistence(persistence_params);
    g.set_worker_threads(s.nworkers);
    g.set_read_threads(s.nreaders);
    if s.logging {
        g.log_with(distributary::logger_pls());
    }
    let mut graph = g.build_local();

    graph.install_recipe(RECIPE.to_owned()).unwrap();
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
