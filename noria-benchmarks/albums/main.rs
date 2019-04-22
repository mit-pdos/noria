use clap::value_t_or_exit;
use noria::{Builder, FrontierStrategy, ReuseConfigType};
use slog::{crit, debug, error, info, o, trace, warn, Logger};

fn main() {
    use clap::{App, Arg};
    let args = App::new("albums")
        .version("0.1")
        .about("Test a Facebook-like album visibility security policy.")
        .arg(
            Arg::with_name("graph")
                .short("g")
                .takes_value(true)
                .help("File to dump application's soup graph, if set"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Enable verbose output"),
        )
        .get_matches();
    let verbose = args.occurrences_of("verbose");

    let log = if verbose != 0 {
        noria::logger_pls()
    } else {
        Logger::root(slog::Discard, o!())
    };

    eprintln!("starting up noria");
    let mut g = Builder::default();
    //g.set_reuse(ReuseConfigType::Full);
    g.set_reuse(ReuseConfigType::NoReuse);
    g.set_frontier_strategy(FrontierStrategy::Readers);
    g.set_sharding(None);
    if verbose > 1 {
        g.log_with(log.clone());
    }
    let mut g = g.start_simple().unwrap();

    eprintln!("setting up database schema");
    g.install_recipe(include_str!("schema.sql"))
        .expect("failed to load initial schema");
    eprintln!("adding security policies");
    g.on_worker(|w| w.set_security_config(include_str!("policies.json").to_string()))
        .expect("failed to set policy");
    eprintln!("adding queries");
    g.extend_recipe(include_str!("queries.sql"))
        .expect("failed to load initial schema");

    eprintln!("making some data");
    let mut friends = g.table("Friend").unwrap().into_sync();
    let mut albums = g.table("Album").unwrap().into_sync();
    let mut photos = g.table("Photo").unwrap().into_sync();

    // four users: 1, 2, 3, and 4
    // 1 and 2 are friends, 3 is a friend of 1 but not 2
    // 4 isn't friends with anyone
    //
    // four albums: 1, 2, 3, and 4; one authored by each user
    // 3 is public.
    //
    // there's one photo in each album
    //
    // what should each user be able to see?
    //
    //  - 1 should be able to see albums 1, 2, and 3
    //  - 2 should be able to see albums 1, 2, and 3
    //  - 3 should be able to see albums 1 and 3
    //  - 4 should be able to see albums 3 and 4
    friends
        .perform_all(vec![vec![1.into(), 2.into()], vec![3.into(), 1.into()]])
        .unwrap();
    albums
        .perform_all(vec![
            vec![1.into(), 1.into(), 0.into()],
            vec![2.into(), 2.into(), 0.into()],
            vec![3.into(), 3.into(), 1.into()],
            vec![4.into(), 4.into(), 0.into()],
        ])
        .unwrap();
    photos
        .perform_all(vec![
            vec!["a".into(), 1.into()],
            vec!["b".into(), 2.into()],
            vec!["c".into(), 3.into()],
            vec!["d".into(), 4.into()],
        ])
        .unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));

    if let Some(gloc) = args.value_of("graph") {
        debug!(log, "extracing query graph with two users");
        let gv = g.graphviz().expect("failed to read graphviz");
        std::fs::write(gloc, gv).expect("failed to save graphviz output");
    }

    let mut test = move |uid| {
        let mut view = match uid {
            Some(uid) => g.view(format!("photos_u{}", uid)).unwrap().into_sync(),
            None => g.view("photos").unwrap().into_sync(),
        };
        for aid in 1..=4 {
            eprintln!(
                "album {} as {:?}: {:?}",
                aid,
                uid,
                view.lookup(&[aid.into()], true).unwrap()
            );
        }
    };

    eprintln!("reading without policies");
    test(None);
    eprintln!("reading with policies");
    test(Some(1));
    test(Some(2));
    test(Some(3));
    test(Some(4));
}
