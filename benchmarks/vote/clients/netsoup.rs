use distributary::{self, ControllerHandle, DataType, ZookeeperAuthority};
use clap;

use clients::{Parameters, VoteClient};
use clients::localsoup::graph::RECIPE;

pub(crate) struct Client {
    r: distributary::RemoteGetter,
    #[allow(dead_code)]
    w: distributary::Mutator,
}

type Handle = ControllerHandle<ZookeeperAuthority>;

fn make_mutator(c: &mut Handle, view: &str) -> distributary::Mutator {
    c.get_mutator(view).unwrap()
}

fn make_getter(c: &mut Handle, view: &str) -> distributary::RemoteGetter {
    c.get_getter(view).unwrap()
}

impl VoteClient for Client {
    type Constructor = String;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self::Constructor {
        let zk = format!(
            "{}/{}",
            args.value_of("zookeeper").unwrap(),
            args.value_of("deployment").unwrap()
        );

        if params.prime {
            // for prepop, we need a mutator
            let mut ch = Handle::new(ZookeeperAuthority::new(&zk));
            ch.install_recipe(RECIPE.to_owned()).unwrap();
            let mut m = make_mutator(&mut ch, "Article");
            m.batch_put(
                (0..params.articles)
                    .map(|i| vec![(i as i32).into(), format!("Article #{}", i).into()]),
            ).unwrap();
        }

        zk
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
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
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
}
