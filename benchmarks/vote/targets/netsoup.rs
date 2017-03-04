use distributary::srv;
use distributary::{Blender, Base, Aggregation, JoinBuilder, DataType};
use tarpc;
use tarpc::util::FirstSocketAddr;
use tarpc::client::future::ClientExt;
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
        let article = mig.add_ingredient("article", &["id", "title"], Base::default());

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base::default());

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
    let srv = srv::run(g, addr.first_socket_addr(), 8);

    SoupTarget {
        vote: vote.into(),
        article: article.into(),
        end: end.into(),
        addr: addr.first_socket_addr(),
        _srv: srv,
    }
}

pub struct C(srv::ext::FutureClient, reactor::Core);
use std::ops::{Deref, DerefMut};
impl Deref for C {
    type Target = srv::ext::FutureClient;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for C {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl C {
    pub fn insert(&mut self, view: usize, data: Vec<DataType>) {
        self.1.run(self.0.insert(view, data)).unwrap();
    }
    pub fn query(&mut self,
                 view: usize,
                 key: DataType)
                 -> Result<Vec<Vec<DataType>>, tarpc::Error<()>> {
        self.1.run(self.0.query(view, key))
    }
}
unsafe impl Send for C {}

impl SoupTarget {
    fn mkc(&self) -> C {
        use self::srv::ext::FutureClient;
        let mut core = reactor::Core::new().unwrap();
        for _ in 0..3 {
            use tarpc::client::Options;
            let c = FutureClient::connect(self.addr, Options::default().handle(core.handle()));
            match core.run(c) {
                Ok(client) => {
                    return C(client, core);
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
    type P = (C, usize, usize);
    type G = (C, usize);

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

impl Putter for (C, usize, usize) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| { self.0.insert(self.2, vec![id.into(), title.into()]); })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| { self.0.insert(self.1, vec![user.into(), id.into()]); })
    }
}

impl Getter for (C, usize) {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        Box::new(move |id| {
            self.0
                .query(self.1, id.into())
                .map_err(|_| ())
                .map(|rows| {
                    for row in rows {
                        match row[1] {
                            DataType::TinyText(..) |
                            DataType::Text(..) => {
                                use std::borrow::Cow;
                                let t: Cow<_> = (&row[1]).into();
                                return Some((row[0].clone().into(),
                                             t.to_string(),
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
