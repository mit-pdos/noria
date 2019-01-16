extern crate noria_server;

use noria_server::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
//    VoteCount: SELECT Vote.aid, COUNT(DISTINCT uid) AS votes \
//                            FROM Vote GROUP BY Vote.aid;
//               QUERY ArticleWithVoteCount: \
//                            SELECT Article.aid, title, url, VoteCount.votes AS votes \
//                            FROM Article, VoteCount \
//                            WHERE Article.aid = VoteCount.aid AND Article.aid = ?
    let sql = "# base tables
               CREATE TABLE Diagnoses (zipcode int, diagnosis int, uid int, PRIMARY KEY(uid));

               # read queries
               DiagnosisByLoc: \
                            SELECT Diagnoses.zipcode AS gbk, COUNT(diagnosis) AS patients \
                            FROM Diagnoses GROUP BY Diagnoses.zipcode;
               QUERY DiagnosisCount: \
                            SELECT DiagnosisByLoc.gbk, DiagnosisByLoc.patients \
                            FROM DiagnosisByLoc \
                            WHERE DiagnosisByLoc.gbk = ?;";

    let persistence_params = noria_server::PersistenceParameters::new(
        noria_server::DurabilityMode::DeleteOnExit,
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
    let mut diagnoses = blender.table("Diagnoses").unwrap();
//    let mut diag_by_loc = blender.table("DiagnosisByLoc").unwrap();
    let mut counts = blender.view("DiagnosisCount").unwrap();
    let uid = 1;
    let zip = 1;
    
    if counts.lookup(&[uid.into()], true).unwrap().is_empty() {
        println!("Creating new users...");
        let uid1 = 1;
        let diag1 = 1;
        let zip1 = 1;
        diagnoses
            .insert(vec![uid1.into(), diag1.into(), zip1.into()])
            .unwrap();
    }

    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(1000));

    println!("Reading...");
    println!("{:#?}", counts.lookup(&[uid.into()], true))
}
