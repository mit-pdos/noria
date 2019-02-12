extern crate noria_server;

use noria_server::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Author (aid int, name text);
               CREATE TABLE Vote (aid int, uid int);

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(DISTINCT uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
                QUERY AllTogether: \
                             SELECT Article.aid, title, name, votes \
                             FROM Article
                             JOIN VoteCount ON Article.aid = VoteCount.aid \
                             JOIN Author ON Author.aid = Article.aid \
                             WHERE Article.aid = ?;
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article JOIN VoteCount ON Article.aid = VoteCount.aid \
                            WHERE Article.aid = ?;
               QUERY ArticleWithAuthor: \
                            SELECT Article.aid, title, name \
                            FROM Article JOIN Author ON Article.aid = Author.aid \
                            WHERE Article.aid = ?;
               QUERY AuthorWithVoteCount: \
                            SELECT Author.aid, name, votes \
                            FROM Author JOIN VoteCount ON Author.aid = VoteCount.aid \
                            WHERE Author.aid = ?;
               VotesByAuthor: SELECT name, SUM(votes) AS votes \
                            FROM AuthorWithVoteCount GROUP BY name;
               QUERY AuthorTotalVotes: SELECT * FROM VotesByAuthor \
                            WHERE name = ?;";

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
    let mut author = blender.table("Author").unwrap();
    let mut awvc = blender.view("ArticleWithVoteCount").unwrap();
    let mut awau = blender.view("ArticleWithAuthor").unwrap();
    let mut auwvc = blender.view("AuthorWithVoteCount").unwrap();
    let mut at = blender.view("AllTogether").unwrap();
    let mut autv = blender.view("AuthorTotalVotes").unwrap();

    let aid = 1;
    let name = "Jackie Bredenberg";
    // Make sure the article exists:
    if awvc.lookup(&[aid.into()], true).unwrap().is_empty() {
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article.insert(vec![aid.into(), title.into(), url.into()]).unwrap();
    }
    if awau.lookup(&[aid.into()], true).unwrap().is_empty() {
        author.insert(vec![aid.into(), name.into()]).unwrap();
    }

    // Then create a new vote:
    let uid = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    vote.insert(vec![aid.into(), uid.into()]).unwrap();

    thread::sleep(Duration::from_millis(1000));

    println!("{:#?}", awvc.lookup(&[aid.into()], true));
    println!("{:#?}", awau.lookup(&[aid.into()], true));
    println!("{:#?}", auwvc.lookup(&[aid.into()], true));
    println!("{:#?}", at.lookup(&[aid.into()], true));
    println!("{:#?}", autv.lookup(&[name.into()], true))
}
