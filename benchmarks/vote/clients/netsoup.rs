use distributary::srv;
use distributary::{DataType, Mutator, MutatorBuilder, RemoteGetter, RemoteGetterBuilder};

use common::{ArticleResult, Period, Reader, RuntimeConfig, Writer};

use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use net2::TcpBuilder;
use bufstream::BufStream;
use bincode;
use vec_map::VecMap;

const ARTICLE_NODE: usize = 1;
const VOTE_NODE: usize = 2;
const END_NODE: usize = 4;

pub struct C(BufStream<TcpStream>, VecMap<Mutator>, VecMap<RemoteGetter>);
impl C {
    pub fn mput(&mut self, view: usize, data: Vec<Vec<DataType>>) {
        let stream = &mut self.0;
        let mutator = self.1.entry(view).or_insert_with(|| {
            bincode::serialize_into(
                stream,
                &srv::Method::GetMutatorBuilder { view },
                bincode::Infinite,
            ).unwrap();
            bincode::serialize_into(stream, &srv::Method::Flush, bincode::Infinite).unwrap();
            stream.flush().unwrap();
            let builder: MutatorBuilder =
                bincode::deserialize_from(stream, bincode::Infinite).unwrap();
            builder.build()
        });

        for r in data {
            mutator.put(r).unwrap();
        }
    }

    pub fn query(
        &mut self,
        view: usize,
        keys: Vec<DataType>,
    ) -> Result<Vec<Vec<Vec<DataType>>>, io::Error> {
        let stream = &mut self.0;
        let getter = self.2.entry(view).or_insert_with(|| {
            bincode::serialize_into(
                stream,
                &srv::Method::GetGetterBuilder { view },
                bincode::Infinite,
            ).unwrap();
            bincode::serialize_into(stream, &srv::Method::Flush, bincode::Infinite).unwrap();
            stream.flush().unwrap();
            let builder: RemoteGetterBuilder =
                bincode::deserialize_from(stream, bincode::Infinite).unwrap();
            println!("Got RemoteGetterBuilder: {:?}", builder);
            builder.build()
        });

        Ok(getter.lookup(keys, true).into_iter().map(|rs| rs.unwrap_or_default()).collect())
    }
}

pub fn make(addr: &str, config: &RuntimeConfig) -> C {
    let stream = TcpBuilder::new_v4().unwrap();
    if let Some(ref addr) = config.bind_to {
        stream.bind(addr).unwrap();
    }
    let stream = stream.connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let stream = BufStream::new(stream);
    C(stream, VecMap::new(), VecMap::new())
}

impl Writer for C {
    fn make_articles<I>(&mut self, articles: I)
    where
        I: Iterator<Item = (i64, String)>,
        I: ExactSizeIterator,
    {
        let articles = articles
            .map(|(aid, title)| vec![aid.into(), title.into()])
            .collect();
        self.mput(ARTICLE_NODE, articles);
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let votes = ids.iter()
            .map(|&(user_id, article_id)| {
                vec![user_id.into(), article_id.into()]
            })
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
        let res = self.query(END_NODE, aids).map_err(|_| ()).map(|rows| {
            assert_eq!(ids.len(), rows.len());
            rows.into_iter()
                .map(|rows| {
                    // rustfmt
                    match rows.into_iter().next() {
                        Some(row) => match row[1] {
                            DataType::TinyText(..) | DataType::Text(..) => {
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
                        },
                        None => ArticleResult::NoSuchArticle,
                    }
                })
                .collect()
        });
        (res, Period::PreMigration)
    }
}
