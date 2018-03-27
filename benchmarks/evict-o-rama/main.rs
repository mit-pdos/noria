extern crate distributary;

use distributary::ControllerBuilder;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

static NUM_ARTICLES: usize = 10_000;

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));
               CREATE TABLE Vote (aid int, uid int);

               # read queries
               VoteCount: SELECT Vote.aid, COUNT(uid) AS votes \
                            FROM Vote GROUP BY Vote.aid;
               QUERY ArticleWithVoteCount: \
                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
                            FROM Article LEFT JOIN VoteCount \
                                         ON (Article.aid = VoteCount.aid) \
                            WHERE Article.aid = ?;";

    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(1),
        Some(String::from("evictorama")),
    );

    // set up Soup via recipe
    let mut builder = ControllerBuilder::default();
    builder.log_with(distributary::logger_pls());
    builder.set_worker_threads(2);
    builder.set_persistence(persistence_params);
    builder.set_memory_limit(100 * 1024);

    // TODO: This should be removed when the `it_works_with_reads_before_writes`
    // test passes again.
    //builder.disable_partial();

    let mut blender = builder.build_local();
    blender.install_recipe(sql.to_owned()).unwrap();

    // Get mutators and getter.
    let mut article = blender.get_mutator("Article").unwrap();
    let mut vote = blender.get_mutator("Vote").unwrap();
    let mut awvc = blender.get_getter("ArticleWithVoteCount").unwrap();

    println!("Creating articles...");
    for aid in 1..NUM_ARTICLES {
        // Make sure the article exists:
        let title = format!("Article {}", aid);
        let url = "http://pdos.csail.mit.edu";
        article
            .put(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    println!("Reading articles...");
    for aid in 1..NUM_ARTICLES {
        awvc.lookup(&aid.into(), true).unwrap();
    }

    println!("Casting votes...");
    let mut aid = 0;
    loop {
        let uid = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        aid = (aid + 1) % NUM_ARTICLES;
        vote.put(vec![(aid + 1).into(), uid.into()]).unwrap();
    }
}
