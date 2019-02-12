extern crate noria_server;

use noria_server::ControllerBuilder;

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Books (title text, count int, lib_id int, year int);

               # read queries
               QUERY NumBooksByLibrary: SELECT SUM(count) AS count, lib_id \
                                    FROM Books GROUP BY lib_id;
               QUERY NumBooksByYear: SELECT SUM(count) AS count, year \
                                    FROM Books GROUP BY year;
               QUERY NumBooksByBoth: SELECT SUM(count) AS count, lib_id, year \
                                    FROM Books GROUP BY lib_id, year;";

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
    let mut books = blender.table("Books").unwrap();
    let mut both = blender.view("NumBooksByBoth").unwrap();
    let mut year = blender.view("NumBooksByYear").unwrap();
    let mut lib = blender.view("NumBooksByLibrary").unwrap();

    let title1 = "HPMOR";
    let title2 = "BWP";
    let lib_id = 49901;
    if lib.lookup(&[title1.into()], true).unwrap().is_empty() {
        books.insert(vec![title1.into(), 2.into(), lib_id.into(), 2015.into()]).unwrap();
        books.insert(vec![title2.into(), 3.into(), lib_id.into(), 2019.into()]).unwrap();
    }

    thread::sleep(Duration::from_millis(1000));
    println!("{:#?}", lib.lookup(&[lib_id.into()], true));
    println!("{:#?}", both.lookup(&[lib_id.into()], true))
}
