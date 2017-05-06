use distributary::srv;
use distributary::DataType;

use common::{Writer, Reader, ArticleResult, Period};

use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use bufstream::BufStream;
use bincode;

const ARTICLE_NODE: usize = 1;
const VOTE_NODE: usize = 2;
const END_NODE: usize = 4;

pub struct C(BufStream<TcpStream>);
impl C {
    pub fn mput(&mut self, view: usize, mut data: Vec<Vec<DataType>>) {
        let &mut C(ref mut bs) = self;
        let n = data.len();
        let mut method = srv::Method::Insert {
            view,
            args: Vec::new(),
        };
        for r in &mut data {
            if let srv::Method::Insert { ref mut args, .. } = method {
                use std::mem;
                mem::swap(args, r);
            }
            bincode::serialize_into(bs, &method, bincode::Infinite).unwrap();
        }
        bincode::serialize_into(bs, &srv::Method::Flush, bincode::Infinite).unwrap();
        bs.flush().unwrap();
        for _ in 0..n {
            let _: i64 = bincode::deserialize_from(bs, bincode::Infinite).unwrap();
        }
    }

    pub fn query(&mut self,
                 view: usize,
                 mut keys: Vec<DataType>)
                 -> Result<Vec<Vec<Vec<DataType>>>, io::Error> {
        let &mut C(ref mut bs) = self;
        let n = keys.len();
        let mut method = srv::Method::Query {
            view,
            key: DataType::None,
        };
        for q_key in &mut keys {
            if let srv::Method::Query { ref mut key, .. } = method {
                use std::mem;
                mem::swap(key, q_key);
            }
            bincode::serialize_into(bs, &method, bincode::Infinite).unwrap();
        }
        bincode::serialize_into(bs, &srv::Method::Flush, bincode::Infinite).unwrap();
        bs.flush()?;
        Ok((0..n)
               .map(|_| {
                        let result: Result<Vec<Vec<DataType>>, ()> =
                            bincode::deserialize_from(bs, bincode::Infinite).unwrap();
                        result.unwrap_or_default()
                    })
               .collect())
    }
}

pub fn make(addr: &str) -> C {
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let stream = BufStream::new(stream);
    C(stream)
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
