use abomonation::{encode, decode, Abomonation};
use distributary::DataType;

use common::{Writer, Reader, RuntimeConfig, ArticleResult, Period};

use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use net2::TcpBuilder;
use bufstream::BufStream;

const END_NODE: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rpc {
    /// Query the given `view` for all records whose key column matches the given value.
    Query {
        /// The view to query
        view: usize,
        /// The key value to use for the given query's free parameter
        key: i64,
    },

    /// Insert a new `article` record.
    ///
    /// `args` gives the column values for the new record.
    InsertArticle {
        /// Article ID.
        aid: i64,
        /// Article title.
        title: String,
    },

    /// Insert a new `vote` record.
    ///
    /// `args` gives the column values for the new record.
    InsertVote {
        /// Article ID.
        aid: i64,
        /// User ID.
        uid: i64,
    },

    /// Flush any buffered responses.
    Flush,
}

impl Abomonation for Rpc {
    #[inline]
    unsafe fn embalm(&mut self) {
        if let Rpc::Query { mut view, mut key } = *self {
            view.embalm();
            key.embalm();
        }
        if let Rpc::InsertArticle {
            ref mut aid,
            ref mut title,
        } = *self
        {
            aid.embalm();
            title.embalm();
        }
        if let Rpc::InsertVote {
            ref mut aid,
            ref mut uid,
        } = *self
        {
            aid.embalm();
            uid.embalm();
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        if let Rpc::Query { ref view, ref key } = *self {
            view.entomb(bytes);
            key.entomb(bytes);
        }
        if let Rpc::InsertArticle { ref aid, ref title } = *self {
            aid.entomb(bytes);
            title.entomb(bytes);
        }
        if let Rpc::InsertVote { ref aid, ref uid } = *self {
            aid.entomb(bytes);
            uid.entomb(bytes);
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Rpc::Query {
                ref mut view,
                ref mut key,
            } => {
                view.exhume(bytes);
                key.exhume(bytes);
                Some(bytes)
            }
            Rpc::InsertArticle {
                ref mut aid,
                ref mut title,
            } => {
                let temp = bytes;
                bytes = if let Some(bytes) = aid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                let temp = bytes;
                bytes = if let Some(bytes) = title.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                Some(bytes)
            }
            Rpc::InsertVote {
                ref mut aid,
                ref mut uid,
            } => {
                let temp = bytes;
                bytes = if let Some(bytes) = aid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                let temp = bytes;
                bytes = if let Some(bytes) = uid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                Some(bytes)
            }
            Rpc::Flush => Some(bytes),
        }
    }
}

fn serialize(method: &Rpc, stream: &mut BufStream<TcpStream>) -> Result<usize, io::Error> {
    let mut buf = Vec::new();
    unsafe {
        encode(method, &mut buf);
    }
    stream.write(&buf)
}

pub struct C(BufStream<TcpStream>);
impl C {
    pub fn put_articles(&mut self, mut data: Vec<(i64, String)>) {
        let &mut C(ref mut bs) = self;
        //let n = data.len();
        let mut method = Rpc::InsertArticle {
            aid: 0,
            title: String::new(),
        };
        for r in &mut data {
            let mut a_id = &mut r.0;
            let mut a_title = &mut r.1;

            if let Rpc::InsertArticle {
                ref mut aid,
                ref mut title,
            } = method
            {
                use std::mem;
                mem::swap(aid, &mut a_id);
                mem::swap(title, &mut a_title);
            }
            serialize(&method, bs).unwrap();
        }
        let m = Rpc::Flush;
        serialize(&m, bs).unwrap();
        bs.flush().unwrap();
        /*for _ in 0..n {
            let _: i64 = bincode::deserialize_from(bs, bincode::Infinite).unwrap();
        }*/
    }

    pub fn put_votes(&mut self, mut data: Vec<(i64, i64)>) {
        let &mut C(ref mut bs) = self;
        //let n = data.len();
        let mut method = Rpc::InsertVote { aid: 0, uid: 0 };
        for r in &mut data {
            let mut v_aid = &mut r.0;
            let mut v_uid = &mut r.1;

            if let Rpc::InsertVote {
                ref mut aid,
                ref mut uid,
            } = method
            {
                use std::mem;
                mem::swap(aid, &mut v_aid);
                mem::swap(uid, &mut v_uid);
            }
            serialize(&method, bs).unwrap();
        }
        let m = Rpc::Flush;
        serialize(&m, bs).unwrap();
        bs.flush().unwrap();
        /*for _ in 0..n {
            let _: i64 = bincode::deserialize_from(bs, bincode::Infinite).unwrap();
        }*/
    }

    pub fn query(
        &mut self,
        view: usize,
        mut keys: Vec<DataType>,
    ) -> Result<Vec<Vec<Vec<DataType>>>, io::Error> {
        let &mut C(ref mut bs) = self;
        let n = keys.len();
        let mut method = Rpc::Query { view, key: 0 };
        for q_key in &mut keys {
            let mut q_key = match *q_key {
                DataType::Int(i) => i as i64,
                DataType::BigInt(i) => i as i64,
                _ => unreachable!(),
            };
            if let Rpc::Query { ref mut key, .. } = method {
                use std::mem;
                mem::swap(key, &mut q_key);
            }
            serialize(&method, bs)?;
        }
        let m = Rpc::Flush;
        serialize(&m, bs)?;
        bs.flush()?;
        /*Ok(
            (0..n)
                .map(|_| {
                    let result: Result<Vec<Vec<DataType>>, ()> =
                        bincode::deserialize_from(bs, bincode::Infinite).unwrap();
                    result.unwrap_or_default()
                })
                .collect(),
        )*/
        Ok(vec![vec![vec![]]])
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
    C(stream)
}

impl Writer for C {
    fn make_articles<I>(&mut self, articles: I)
    where
        I: Iterator<Item = (i64, String)>,
        I: ExactSizeIterator,
    {
        let articles = articles.collect();
        self.put_articles(articles);
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        let votes = ids.iter()
            .map(|&(user_id, article_id)| (user_id, article_id))
            .collect();
        self.put_votes(votes);
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
                        Some(row) => {
                            match row[1] {
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
