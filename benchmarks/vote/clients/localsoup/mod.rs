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

        let queue_length = value_t_or_exit!(args, "write-batch-size", usize);
        let flush_timeout = time::Duration::from_millis(10);
        let mode = if args.is_present("durability") {
            if args.is_present("retain-logs-on-exit") {
                DurabilityMode::Permanent
            } else {
                DurabilityMode::DeleteOnExit
            }
        } else {
            DurabilityMode::MemoryOnly
        };

        let persistence_params = PersistenceParameters::new(
            mode,
            queue_length,
            flush_timeout,
            Some(String::from("vote")),
        );

        // setup db
        let mut s = graph::Setup::new(true, nworkers);
        s.logging = verbose;
        s.transactions = false;
        s.nreaders = read_threads;
        s.sharding = match value_t_or_exit!(args, "shards", usize) {
            0 => None,
            x => Some(x),
        };
        s.stupid = args.is_present("stupid");
        let mut g = graph::make(s, persistence_params);

        // prepopulate
        if verbose {
            println!("Prepopulating with {} articles", params.articles);
        }
        let mut a = g.graph.get_mutator(g.article).unwrap();
        let pop_batch_size = 100;
        assert_eq!(params.articles % pop_batch_size, 0);
        for i in 0..params.articles / pop_batch_size {
            let reali = pop_batch_size * i;
            let data: Vec<Vec<DataType>> = (reali..reali + pop_batch_size)
                .map(|i| (i as i64, format!("Article #{}", i)))
                .map(|(id, title)| vec![id.into(), title.into()])
                .collect();
            a.multi_put(data).unwrap();
        }
        if verbose {
            println!("Done with prepopulation");
        }

        // allow writes to propagate
        thread::sleep(time::Duration::from_secs(1));

        g
    }

    fn from(soup: &mut Self::Constructor) -> Self {
        Client {
            r: soup.graph.get_getter(soup.end).unwrap(),
            w: soup.graph.get_mutator(soup.vote).unwrap(),
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
