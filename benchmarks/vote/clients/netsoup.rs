use clap;
use clients::localsoup::graph::RECIPE;
use clients::{Parameters, VoteClient, VoteClientConstructor};
use distributary::{self, ControllerHandle, DataType, ZookeeperAuthority};

pub(crate) struct Client {
    r: distributary::RemoteGetter<distributary::ExclusiveConnection>,
    #[allow(dead_code)]
    w: distributary::Mutator<distributary::ExclusiveConnection>,
}

type Handle = ControllerHandle<ZookeeperAuthority>;

fn make_mutator(
    c: &mut Handle,
    view: &str,
) -> distributary::Mutator<distributary::ExclusiveConnection> {
    c.base(view).unwrap().into_exclusive()
}

fn make_getter(
    c: &mut Handle,
    view: &str,
) -> distributary::RemoteGetter<distributary::ExclusiveConnection> {
    c.view(view).unwrap().into_exclusive()
}

pub(crate) struct Constructor(String);

impl VoteClientConstructor for Constructor {
    type Instance = Client;

    fn new(params: &Parameters, args: &clap::ArgMatches) -> Self {
        let zk = format!(
            "{}/{}",
            args.value_of("zookeeper").unwrap(),
            args.value_of("deployment").unwrap()
        );

        if params.prime {
            // for prepop, we need a mutator
            let mut ch = Handle::new(ZookeeperAuthority::new(&zk));
            ch.install_recipe(RECIPE).unwrap();
            let mut m = make_mutator(&mut ch, "Article");
            let mut id = 0;
            while id < params.articles {
                let end = ::std::cmp::min(id + 1000, params.articles);
                m.batch_put(
                    (id..end)
                        .map(|i| vec![((i + 1) as i32).into(), format!("Article #{}", i).into()]),
                ).unwrap();
                id = end;
            }
        }

        Constructor(zk)
    }

    fn make(&mut self) -> Self::Instance {
        let mut ch = Handle::new(ZookeeperAuthority::new(&self.0));

        Client {
            r: make_getter(&mut ch, "ArticleWithVoteCount"),
            w: make_mutator(&mut ch, "Vote"),
        }
    }
}

impl VoteClient for Client {
    fn handle_writes(&mut self, ids: &[i32]) {
        let data: Vec<Vec<DataType>> = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into(), 0.into()])
            .collect();

        self.w.multi_put(data).unwrap();
    }

    fn handle_reads(&mut self, ids: &[i32]) {
        let arg = ids
            .into_iter()
            .map(|&article_id| vec![(article_id as usize).into()])
            .collect();

        let rows = self
            .r
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
