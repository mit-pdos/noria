use distributary::srv;
use distributary::{Blender, Base, Aggregation, JoinBuilder, DataType};
use tarpc;
use tarpc::util::FirstSocketAddr;
use tarpc::client::future::Connect;
use tokio_core::reactor;

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub struct SoupTarget {
    vote: usize,
    article: usize,
    end: usize,
    addr: ::std::net::SocketAddr,
    _srv: srv::ServerHandle,
}

pub fn make(addr: &str, _: usize) -> SoupTarget {
    // set up graph
    let mut g = Blender::new();

    let (article, vote, end) = {
        let mut mig = g.start_migration();

        // add article base node
        let article = mig.add_ingredient("article", &["id", "title"], Base {});

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base {});

        // add vote count
        let vc = mig.add_ingredient("vc",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join -- joins on first field of each input
        let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
            .from(article, vec![1, 0])
            .join(vc, vec![1, 0]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        mig.maintain(end, 0);
        mig.commit();

        (article, vote, end)
    };

    // start processing
    let srv = srv::run(g, addr.first_socket_addr(), 4);

    SoupTarget {
        vote: vote.into(),
        article: article.into(),
        end: end.into(),
        addr: addr.first_socket_addr(),
        _srv: srv,
    }
}

pub struct Client {
    rpc: srv::ext::FutureClient,
    core: reactor::Core,
}

// safe because the core will remain on the same core as the client
unsafe impl Send for Client {}

impl SoupTarget {
    fn mkc(&self) -> Client {
        use self::srv::ext::FutureClient;
        let mut core = reactor::Core::new().unwrap();
        for _ in 0..3 {
            let options = tarpc::client::Options::default().handle(core.handle());
            match core.run(FutureClient::connect(self.addr, options)) {
                Ok(client) => {
                    return Client {
                        rpc: client,
                        core: core,
                    }
                }
                Err(_) => {
                    use std::thread;
                    use std::time::Duration;
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
        panic!("Failed to connect to netsoup server");
    }
}

impl Backend for SoupTarget {
    type P = (Client, usize, usize);
    type G = (Client, usize);

    fn getter(&mut self) -> Self::G {
        (self.mkc(), self.end)
    }

    fn putter(&mut self) -> Self::P {
        (self.mkc(), self.vote, self.article)
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for (Client, usize, usize) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            self.0.core.run(self.0.rpc.insert(self.2, vec![id.into(), title.into()])).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.0.core.run(self.0.rpc.insert(self.1, vec![user.into(), id.into()])).unwrap();
        })
    }
}

impl Getter for (Client, usize) {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        Box::new(move |id| {
            self.0
                .core
                .run(self.0
                    .rpc
                    .query(self.1, id.into()))
                .map_err(|_| ())
                .map(|rows| {
                    for row in rows.into_iter() {
                        match row[1] {
                            DataType::Text(ref s) => {
                                return Some((row[0].clone().into(),
                                             (**s).clone(),
                                             row[2].clone().into()));
                            }
                            _ => unreachable!(),
                        }
                    }
                    None
                })
        })
    }
}
