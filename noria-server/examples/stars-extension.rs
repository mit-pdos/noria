extern crate noria_server;

use noria_server::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(DISTINCT uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article, VoteCount \
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;";

    let extended_sql = "\
                CREATE TABLE Rating (aid int, uid int, stars int);

                RatingsAndVotes: SELECT Rating.aid, Rating.stars FROM Rating \
                                UNION \
                                SELECT Vote.aid, 5 AS stars FROM Vote;
                TotalCount: SELECT RatingsAndVotes.aid, SUM(RatingsAndVotes.stars) AS stars \
                            FROM RatingsAndVotes \
                            GROUP BY aid;
                QUERY ArticleWithTotalCount: \
                            SELECT Article.aid, title, url, TotalCount.stars AS votes \
                            FROM Article, TotalCount \
                            WHERE Article.aid = TotalCount.aid AND Article.aid = ?;";

    let persistence_params = noria_server::PersistenceParameters::new(
        noria_server::DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    // set up Soup via recipe
    let mut builder = ControllerBuilder::default();

    builder.log_with(noria_server::logger_pls());
    builder.set_persistence(persistence_params);

    let mut blender = builder.build_local().unwrap();
    blender.install_recipe(sql).unwrap();
    blender.extend_recipe(extended_sql).unwrap();
    println!("{}", blender.graphviz().unwrap());

    // Get mutators and getter.
    let mut article = blender.table("Article").unwrap();
    let mut vote = blender.table("Vote").unwrap();
    let mut rating = blender.table("Rating").unwrap();
    //let mut awvc = blender.view("ArticleWithVoteCount").unwrap();
    let mut awtc = blender.view("ArticleWithTotalCount").unwrap();

    println!("Creating article...");
    let aid = 1;
    // Make sure the article exists:
    if awtc.lookup(&[aid.into()], true).unwrap().is_empty() {
        println!("Creating new article...");
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }


    let uid = generate_time();
    vote.insert(vec![aid.into(), uid.into()]).unwrap();
    vote.insert(vec![aid.into(), uid.into()]).unwrap();

    let uid2 = generate_time();
    rating.insert(vec![aid.into(), uid2.into(), 2.into()]).unwrap();

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", awtc.lookup(&[aid.into()], true))
}

fn generate_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
