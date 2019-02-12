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
                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?;
               PreJoin: SELECT Article.aid, title, url, Vote.uid AS uid \
                            FROM Article, Vote \
                            WHERE Article.aid = Vote.aid;
               QUERY CountedVotes: \
                            SELECT aid, title, url, COUNT(DISTINCT uid) AS votes \
                            FROM PreJoin \
                            WHERE aid = ?;";

    let persistence_params = noria_server::PersistenceParameters::new(
        noria_server::DurabilityMode::Permanent,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    // set up Soup via recipe
    let mut builder = ControllerBuilder::default();

    builder.log_with(noria_server::logger_pls());
    builder.disable_partial();
    builder.set_persistence(persistence_params);

    let mut blender = builder.build_local().unwrap();
    blender.install_recipe(sql).unwrap();
    println!("{}", blender.graphviz().unwrap());

    // Get mutators and getter.
    let mut article = blender.table("Article").unwrap();
    let mut vote = blender.table("Vote").unwrap();
    let mut awvc = blender.view("ArticleWithVoteCount").unwrap();
    let mut cv = blender.view("CountedVotes").unwrap();

    let aid = 1;
    // Make sure the article exists:
    if awvc.lookup(&[aid.into()], true).unwrap().is_empty() {
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    // Then create a new vote:
    let uid = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    vote.insert(vec![aid.into(), uid.into()]).unwrap();

    thread::sleep(Duration::from_millis(1000));

    println!("{:#?}", awvc.lookup(&[aid.into()], true));
    println!("{:#?}", cv.lookup(&[aid.into()], true))
}
