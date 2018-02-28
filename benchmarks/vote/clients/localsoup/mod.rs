use distributary::{self, DataType};
use clap;
use std::time;
use std::thread;

use clients::{Parameters, VoteClient};

pub(crate) mod graph;

pub(crate) struct Client {
    r: distributary::RemoteGetter,
    #[allow(dead_code)]
    w: distributary::Mutator,
}

impl VoteClient for Client {
    type Constructor = graph::Graph;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self::Constructor {
        use distributary::{DurabilityMode, PersistenceParameters};

        assert!(params.prime);

        let nworkers = value_t_or_exit!(args, "workers", usize);
        let read_threads = value_t_or_exit!(args, "readthreads", usize);
        let verbose = args.is_present("verbose");

        let mut persistence = PersistenceParameters::default();
        persistence.mode = if args.is_present("durability") {
            if args.is_present("retain-logs-on-exit") {
                DurabilityMode::Permanent
            } else {
                DurabilityMode::DeleteOnExit
            }
        } else {
            DurabilityMode::MemoryOnly
        };
        persistence.queue_capacity = value_t_or_exit!(args, "write-batch-size", usize);
        persistence.log_prefix = "vote".to_string();

        // setup db
        let mut s = graph::Setup::default();
        s.logging = verbose;
        s.nworkers = nworkers;
        s.nreaders = read_threads;
        s.sharding = match value_t_or_exit!(args, "shards", usize) {
            0 => None,
            x => Some(x),
        };
        s.stupid = args.is_present("stupid");
        let mut g = graph::make(s, persistence);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }
        let mut a = g.graph.get_mutator("Article").unwrap();
        let articles = (0..params.articles)
            .map(|i| vec![(i as i64).into(), format!("Article #{}", i).into()])
            .collect::<Vec<Vec<DataType>>>();
        a.multi_put(articles).unwrap();
        if verbose {
            println!("Done with prepopulation");
        }

        // allow writes to propagate
        thread::sleep(time::Duration::from_secs(1));

        g
    }

    fn from(soup: &mut Self::Constructor) -> Self {
        Client {
            r: soup.graph.get_getter("ArticleWithVoteCount").unwrap(),
            w: soup.graph.get_mutator("Vote").unwrap(),
        }
    }

    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids.into_iter()
            .map(|&article_id| vec![0.into(), (article_id as usize).into()])
            .collect();

        self.w.multi_put(data).unwrap();
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let arg = ids.into_iter()
            .map(|&article_id| (article_id as usize).into())
            .collect();

        let rows = self.r
            .multi_lookup(arg, true)
            .unwrap()
            .into_iter()
            .map(|_rows| {
                // TODO
                //assert_eq!(rows.map(|rows| rows.len()), Ok(1));
            })
            .count();
        assert_eq!(rows, ids.len());
    }

    fn spawns_threads() -> bool {
        true
    }
}
