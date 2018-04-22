use distributary::{self, ControllerBuilder, ControllerHandle, LocalAuthority, NodeIndex,
                   PersistenceParameters};

pub(crate) const RECIPE: &str = "# base tables
CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
CREATE TABLE Vote (article_id int, user int);

# read queries
QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
            FROM Article \
            LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                       FROM Vote GROUP BY Vote.article_id) AS VoteCount \
            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;";

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
    pub fn transition(&mut self) {
        let stupid_recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (article_id int, user int);
               CREATE TABLE Rating (article_id int, user int, stars int);

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.article_id, COUNT(Vote.user) AS votes \
                                       FROM Vote GROUP BY Vote.article_id) AS VoteCount \
                            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;

               U: SELECT article_id, stars FROM Rating UNION SELECT article_id, 1 AS stars FROM Vote;
               Total: SELECT article_id, SUM(U.stars) AS score \
                           FROM U \
                           GROUP BY article_id;
               QUERY ArticleWithScore: SELECT Article.id, title, Total.score AS score \
                            FROM Article \
                            LEFT JOIN Total ON (Article.id = Total.article_id) \
                            WHERE Article.id = ?;";

        let smart_recipe = "# base tables
               CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
               CREATE TABLE Vote (article_id int, user int);
               CREATE TABLE Rating (article_id int, user int, stars int);

               # read queries
               QUERY ArticleWithVoteCount: SELECT Article.id, title, VoteCount.votes AS votes \
                            FROM Article \
                            LEFT JOIN (SELECT Vote.article_id, COUNT(Vote.user) AS votes \
                                       FROM Vote GROUP BY Vote.article_id) AS VoteCount \
                            ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;

               RatingSum: SELECT article_id, SUM(Rating.stars) AS score FROM Rating GROUP BY article_id;
               U: SELECT article_id, score FROM RatingSum UNION SELECT article_id, votes AS score FROM VoteCount;
               Score: SELECT U.article_id, SUM(U.score) AS score \
                            FROM U GROUP BY U.article_id;
               QUERY ArticleWithScore: SELECT Article.id, title, Score.score \
                            FROM Article LEFT JOIN Score ON (Article.id = Score.article_id) \
                            WHERE Article.id = ?;";

        if self.setup.stupid {
            self.graph.install_recipe(stupid_recipe.to_owned()).unwrap();
        } else {
            self.graph.install_recipe(smart_recipe.to_owned()).unwrap();
        }
    }
}
