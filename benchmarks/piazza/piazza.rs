extern crate distributary;

#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;

use distributary::{Blender, Recipe};

pub struct Backend {
    recipe: Option<Recipe>,
    log: slog::Logger,
    g: Blender,
}

impl Backend {
    pub fn new() -> Backend {
        let mut g = Blender::new();
        let log = distributary::logger_pls();
        let blender_log = log.clone();
        g.log_with(blender_log);

        let recipe = Recipe::blank(Some(log.clone()));

        Backend {
            recipe: Some(recipe),
            log: log,
            g: g,
        }
    }

    fn migrate(
        &mut self, 
        schema_file: &str,
        query_file: Option<&str>,
        policy_file: Option<&str>,
    ) -> Result<(), String> {
        use std::fs::File;
        use std::io::Read;
        
        // Start migration
        let mut mig = self.g.start_migration();

        // Read schema file
        let mut sf = File::open(schema_file).unwrap();
        let mut s = String::new();
        sf.read_to_string(&mut s).unwrap();

        let mut rs = s.clone();
        s.clear();

        match query_file {
            None => (),
            Some(qf) => {
                let mut qf = File::open(qf).unwrap();
                qf.read_to_string(&mut s).unwrap();
                rs.push_str("\n");
                rs.push_str(&s);
            }
        }

        let mut p = String::new();
        match policy_file {
            None => (),
            Some(pf) => { 
                let mut pf = File::open(pf).unwrap();
                pf.read_to_string(&mut p).unwrap();
            },
        }

        let new_recipe = Recipe::from_str_with_policy(&rs, Some(&p), Some(self.log.clone()))?;
        let cur_recipe = self.recipe.take().unwrap();
        let updated_recipe = match cur_recipe.replace(new_recipe) {
            Ok(mut recipe) => {
                match recipe.activate(&mut mig, false) {
                    Ok(ar) => {
                        info!(self.log, "{} expressions added", ar.expressions_added);
                        info!(self.log, "{} expressions removed", ar.expressions_removed);
                    }
                    Err(e) => return Err(format!("failed to activate recipe: {}", e)),
                };
                recipe
            }
            Err(e) => return Err(format!("failed to replace recipe: {}", e)),
        };

        mig.commit();
        self.recipe = Some(updated_recipe);
        Ok(())

    }
    
}

fn main() {
    use clap::{Arg, App};
    let args = App::new("piazza")
        .version("0.1")
        .about(
            "Benchmarks Piazza-like application with security policies.",
        )
        .arg(
            Arg::with_name("schema")
                .short("s")
                .required(true)
                .default_value("Benchmarks/piazza/schema.sql")
                .help("Schema file for Piazza application")
        )
        .arg(
            Arg::with_name("queries")
                .short("q")
                .required(true)
                .default_value("Benchmarks/piazza/queries.sql")
                .help("Query file for Piazza application")
        )
        .arg(
            Arg::with_name("policies")
                .short("p")
                .required(true)
                .default_value("Benchmarks/piazza/policies.json")
                .help("Security policies file for Piazza application")
        )
        .get_matches();


    println!("Starting benchmark...");

    // Read arguments
    let sloc = args.value_of("schema").unwrap();
    let qloc = args.value_of("queries").unwrap();
    let ploc = args.value_of("policies").unwrap();

    // Initiliaze backend application
    let mut backend = Backend::new();
    backend.migrate(sloc, Some(qloc), Some(ploc));

    println!("Done with benchmark.");

    println!("{}", backend.g);

}
