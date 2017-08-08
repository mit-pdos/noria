use abomonation::{encode, decode, Abomonation};
use distributary::DataType;

use common::{Writer, Reader, RuntimeConfig, ArticleResult, Period};

use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use net2::TcpBuilder;
use bufstream::BufStream;

const ARTICLE_NODE: usize = 1;
const VOTE_NODE: usize = 2;
const END_NODE: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Method {
    /// Query the given `view` for all records whose key column matches the given value.
    Query {
        /// The view to query
        view: usize,
        /// The key value to use for the given query's free parameter
        key: u64,
    },

    /// Insert a new record into the given view.
    ///
    /// `args` gives the column values for the new record.
    Insert {
        /// The view to insert into
        view: usize,
        /// The column values for the new record
        args: Vec<u64>,
    },

    /// Flush any buffered responses.
    Flush,
}

impl Abomonation for Method {
    #[inline]
    unsafe fn embalm(&mut self) {
        if let Method::Query { mut view, mut key } = *self {
            view.embalm();
            key.embalm();
        }
        if let Method::Insert {
            ref mut view,
            ref mut args,
        } = *self
        {
            view.embalm();
            args.embalm();
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        if let Method::Query { ref view, ref key } = *self {
            view.entomb(bytes);
            key.entomb(bytes);
        }
        if let Method::Insert { ref view, ref args } = *self {
            view.entomb(bytes);
            args.entomb(bytes);
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Method::Query {
                ref mut view,
                ref mut key,
            } => {
                view.exhume(bytes);
                key.exhume(bytes);
                Some(bytes)
            }
            Method::Insert {
                ref mut view,
                ref mut args,
            } => {
                let temp = bytes;
                bytes = if let Some(bytes) = view.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                let temp = bytes;
                bytes = if let Some(bytes) = args.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                Some(bytes)
            }
            Method::Flush => Some(bytes),
        }
    }
}

fn serialize(method: &Method, stream: &mut BufStream<TcpStream>) -> Result<usize, io::Error> {
    let mut buf = Vec::new();
    unsafe {
        encode(method, &mut buf);
    }
    stream.write(&buf)
}

pub struct C(BufStream<TcpStream>);
impl C {
    pub fn mput(&mut self, view: usize, mut data: Vec<Vec<DataType>>) {
        let &mut C(ref mut bs) = self;
        //let n = data.len();
        let mut method = Method::Insert {
            view,
            args: Vec::new(),
        };
        for r in &mut data {
            let mut rr: Vec<u64> = r.iter()
                .map(|d| match *d {
                    DataType::BigInt(i) => i as u64,
                    DataType::Int(i) => i as u64,
                    _ => 0u64,
                })
                .collect();
            if let Method::Insert { ref mut args, .. } = method {
                use std::mem;
                mem::swap(args, &mut rr);
            }
            serialize(&method, bs).unwrap();
        }
        let m = Method::Flush;
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
        let mut method = Method::Query { view, key: 0u64 };
        for q_key in &mut keys {
            let mut q_key = match *q_key {
                DataType::Int(i) => i as u64,
                DataType::BigInt(i) => i as u64,
                _ => unreachable!(),
            };
            if let Method::Query { ref mut key, .. } = method {
                use std::mem;
                mem::swap(key, &mut q_key);
            }
            serialize(&method, bs)?;
        }
        let m = Method::Flush;
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
