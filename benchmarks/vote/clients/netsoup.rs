use distributary::{self, ControllerHandle, DataType, ZookeeperAuthority};
use clap;
use std::time;
use std::thread;

use clients::{Parameters, VoteClient};
use clients::localsoup::graph::RECIPE;

pub(crate) struct Client {
    r: distributary::RemoteGetter,
    #[allow(dead_code)]
    w: distributary::Mutator,
}

type Handle = ControllerHandle<ZookeeperAuthority>;

fn make_mutator(c: &mut Handle, view: &str) -> distributary::Mutator {
    let view = c.inputs()[view];
    c.get_mutator(view).unwrap()
}

fn make_getter(c: &mut Handle, view: &str) -> distributary::RemoteGetter {
    let view = c.outputs()[view];
    c.get_getter(view).unwrap()
}

impl VoteClient for Client {
    type Constructor = String;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self::Constructor {
        if params.prime {
            // for prepop, we need a mutator
            let mut ch = Handle::new(ZookeeperAuthority::new(args.value_of("zookeeper").unwrap()));

            ch.install_recipe(RECIPE.to_owned()).unwrap();
            let mut m = make_mutator(&mut ch, "Article");

            let pop_batch_size = 100;
            assert_eq!(params.articles % pop_batch_size, 0);
            for i in 0..params.articles / pop_batch_size {
                let reali = pop_batch_size * i;
                let data: Vec<Vec<DataType>> = (reali..reali + pop_batch_size)
                    .map(|i| (i as i64, format!("Article #{}", i)))
                    .map(|(id, title)| vec![id.into(), title.into()])
                    .collect();
                m.multi_put(data).unwrap();
            }

            // allow writes to propagate
            thread::sleep(time::Duration::from_secs(1));
        }

        args.value_of("zookeeper").unwrap().to_string()
    }

    fn from(control: &mut Self::Constructor) -> Self {
        let mut ch = Handle::new(ZookeeperAuthority::new(control));

        Client {
            r: make_getter(&mut ch, "ArticleWithVoteCount"),
            w: make_mutator(&mut ch, "Vote"),
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
}
