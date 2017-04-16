use distributary::srv;
use distributary::DataType;
use tarpc;
use tarpc::util::FirstSocketAddr;
use tarpc::future::client::{ClientExt, Options};
use tokio_core::reactor;
use futures;

use common::{Writer, Reader, ArticleResult, Period};

const ARTICLE_NODE: usize = 1;
const VOTE_NODE: usize = 2;
const END_NODE: usize = 4;

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
    pub fn mput(&mut self, view: usize, data: Vec<Vec<DataType>>) {
        let &mut C(ref client, ref mut core) = self;
        let fut = futures::future::join_all(data.into_iter().map(|row| client.insert(view, row)));
        core.run(fut).unwrap();
    }
    pub fn query(&mut self,
                 view: usize,
                 keys: Vec<DataType>)
                 -> Result<Vec<Vec<Vec<DataType>>>, tarpc::Error<()>> {
        let &mut C(ref client, ref mut core) = self;
        let fut = futures::future::join_all(keys.into_iter().map(|key| client.query(view, key)));
        core.run(fut)
    }
}
unsafe impl Send for C {}

fn mkc(addr: &str) -> C {
    use self::srv::ext::FutureClient;
    let mut core = reactor::Core::new().unwrap();
    for _ in 0..3 {
        let c = FutureClient::connect(addr.first_socket_addr(),
                                      Options::default().handle(core.handle()));
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

pub fn make(addr: &str) -> C {
    mkc(addr)
}

impl Writer for C {
    type Migrator = ();
    fn make_articles<I>(&mut self, articles: I)
        where I: Iterator<Item = (i64, String)>,
              I: ExactSizeIterator
    {
        let articles = articles
            .map(|(aid, title)| vec![aid.into(), title.into()])
            .collect();
        self.mput(ARTICLE_NODE, articles);
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let votes = ids.iter()
            .map(|&(user_id, article_id)| vec![user_id.into(), article_id.into()])
            .collect();
        self.mput(VOTE_NODE, votes);
        Period::PreMigration
    }
}

impl Reader for C {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let aids = ids.iter()
            .map(|&(_, article_id)| article_id.into())
            .collect();
        let res = self.query(END_NODE, aids)
            .map_err(|_| ())
            .map(|rows| {
                assert_eq!(ids.len(), rows.len());
                rows.into_iter()
                    .map(|rows| {
                        // rustfmt
                        match rows.into_iter().next() {
                            Some(row) => {
                                match row[1] {
                                    DataType::TinyText(..) |
                                    DataType::Text(..) => {
                                        use std::borrow::Cow;
                                        let t: Cow<_> = (&row[1]).into();
                                        let count: i64 = match row[2].clone() {
                                            DataType::None => 0,
                                            d => d.into(),
                                        };
                                        ArticleResult::Article {
                                            id: row[0].clone().into(),
                                            title: t.to_string(),
                                            votes: count,
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            None => ArticleResult::NoSuchArticle,
                        }
                    })
                    .collect()
            });
        (res, Period::PreMigration)
    }
}
