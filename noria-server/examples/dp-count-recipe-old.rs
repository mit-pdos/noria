extern crate noria;

use distributary::ControllerBuilder;

use std::thread;
use std::time::Duration;

fn main() {
    // inline recipe definition
    let sql = "# base tables
               CREATE TABLE Diagnoses (pid int, diagnosis int, zipcode char(5));

               # read queries
               QUERY DiagnosisByLoc: \
                            SELECT COUNT(*) \
                            FROM Diagnoses \
                            WHERE diagnosis = 1 \
                            GROUP BY zipcode;";

    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(1),
        Some(String::from("example")),
        1,
    );

    // set up Soup via recipe
    println!("Setting up Soup.");
    let mut builder = ControllerBuilder::default();
    builder.log_with(distributary::logger_pls());
    builder.set_persistence(persistence_params);

    let mut blender = builder.build_local().unwrap();
    blender.install_recipe(sql).unwrap();
    println!("{}", blender.graphviz().unwrap());

    // Get mutators and getter.
    let mut diagnoses = blender.table("Diagnoses").unwrap();
    let mut counts = blender.view("DiagnosisByLoc").unwrap();
    let uid = 1;
    
    if counts.lookup(&[uid.into()], true).unwrap().is_empty() {
        println!("Creating new users...");
        let uid1 = 1;
        let diag1 = 1;
        let zip1 = "02139";
        diagnoses
            .insert(vec![uid1.into(), diag1.into(), zip1.into()])
            .unwrap();
    }

    // Wait for changes to propagate
    println!("Finished writing! Let's wait for things to propagate...");
    thread::sleep(Duration::from_millis(60000));

    // Read from table
    println!("Reading...");
    println!("{:#?}", counts.lookup(&[1.into()], true));
}
