use clap;
use clients::{Parameters, VoteClient, VoteClientConstructor};
use distributary::{self, DataType};
use std::thread;
use std::time;

pub(crate) mod graph;

pub(crate) struct Client {
    r: distributary::RemoteGetter<distributary::ExclusiveConnection>,
    #[allow(dead_code)]
    w: distributary::Mutator<distributary::ExclusiveConnection>,
}

pub(crate) struct Constructor(graph::Graph);

// this is *only* safe because we make the getters and mutators exclusive, *and* we get them under
// a lock (so the Rcs will still be correct)
unsafe impl Send for Constructor {}

impl VoteClientConstructor for Constructor {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
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
        a.batch_put(
            (0..params.articles).map(|i| vec![(i as i32).into(), format!("Article #{}", i).into()]),
        ).unwrap();
        if verbose {
            println!("Done with prepopulation");
        }

        // allow writes to propagate
        thread::sleep(time::Duration::from_secs(1));

        Constructor(g)
    }

    fn make(&mut self) -> Self::Instance {
        Client {
            r: self.0
                .graph
                .get_getter("ArticleWithVoteCount")
                .unwrap()
                .into_exclusive(),
            w: self.0.graph.get_mutator("Vote").unwrap().into_exclusive(),
        }
    }

    fn spawns_threads() -> bool {
        true
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids.into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        self.w.multi_put(data).unwrap();
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let arg = ids.into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
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
}
