extern crate noria_server;

use noria_server::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Article (aid int, title varchar(255), \
                                     url text, PRIMARY KEY(aid));

               # read queries
               FewerColumnsSelected: SELECT aid, title FROM Article;
               QUERY MoreColumnsSelected: \
                            SELECT Article.aid, FewerColumnsSelected.title, Article.url \
                            FROM FewerColumnsSelected, Article \
                            WHERE Article.aid = FewerColumnsSelected.aid AND Article.aid = ?;";

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
    println!("{}", blender.graphviz().unwrap());

    // Get mutators and getter.
    let mut article = blender.table("Article").unwrap();
    let mut fcs = blender.view("MoreColumnsSelected").unwrap();

    let aid = 1;
    // Make sure the article exists:
    if fcs.lookup(&[aid.into()], true).unwrap().is_empty() {
        let title = "test title";
        let url = "http://pdos.csail.mit.edu";
        article
            .insert(vec![aid.into(), title.into(), url.into()])
            .unwrap();
    }

    thread::sleep(Duration::from_millis(1000));

    println!("{:#?}", fcs.lookup(&[aid.into()], true))
}
