use crate::clients::{Parameters, ReadRequest, VoteClient, WriteRequest};
use clap;
use failure::ResultExt;
use mysql_async::prelude::*;
use redis::AsyncCommands;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower_service::Service;
use tower_util::ServiceExt;

pub(crate) struct Conn {
    pool: mysql_async::Pool,
    next: Option<mysql_async::Conn>,
    pending: Option<mysql_async::futures::GetConn>,
    // concurrency limit is per load generator!
    redis: tower_limit::ConcurrencyLimit<RedisConn>,
}

impl Clone for Conn {
    fn clone(&self) -> Self {
        Conn {
            pool: self.pool.clone(),
            next: None,
            pending: None,
            redis: self.redis.clone(),
        }
    }
}

impl Conn {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), failure::Error>> {
        // NOTE: any Request type will do here
        if let Poll::Pending = Service::<ReadRequest>::poll_ready(&mut self.redis, cx) {
            return Poll::Pending;
        }

        if self.next.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.pending.is_none() {
            self.pending = Some(self.pool.get_conn());
        }

        if let Some(ref mut f) = self.pending {
            match Pin::new(f).poll(cx) {
                Poll::Ready(r) => {
                    self.pending = None;
                    self.next = Some(r?);
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            unreachable!()
        }
    }
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, failure::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches<'_>) -> <Self as VoteClient>::Future {
        let my_addr = args.value_of("mysql-address").unwrap();
        let my_addr = format!("mysql://{}", my_addr);
        let db = args.value_of("database").unwrap().to_string();
        let r_addr = args.value_of("redis-address").unwrap();
        let connstr = format!("redis://{}/", r_addr);
        let r_client = redis::Client::open(connstr).unwrap();

        eprintln!("OBS OBS OBS: redis is single-threaded");

        async move {
            let mut r_conn = r_client.get_multiplexed_tokio_connection().await?;
            let my_opts = mysql_async::Opts::from_url(&my_addr).unwrap();

            if params.prime {
                let mut opts = mysql_async::OptsBuilder::from_opts(my_opts.clone());
                opts.db_name(None::<&str>);
                opts.init(vec![
                    "SET max_heap_table_size = 4294967296;",
                    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
                ]);
                let mut conn = mysql_async::Conn::new(opts).await.unwrap();
                let workaround = format!("DROP DATABASE IF EXISTS {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();
                let workaround = format!("CREATE DATABASE {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();

                // create tables with indices
                let workaround = format!("USE {}", db);
                conn = conn.drop_query(&workaround).await.unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE art \
                         (id bigint not null, votes bigint not null, \
                         PRIMARY KEY USING HASH (id)) ENGINE = MEMORY;",
                        (),
                    )
                    .await
                    .unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE vt \
                         (u bigint not null, id bigint not null, KEY id (id)) ENGINE = MEMORY;",
                        (),
                    )
                    .await
                    .unwrap();

                // prepop
                let mut aid = 1;
                let bs = 1000;
                assert_eq!(params.articles % bs, 0);
                for _ in 0..params.articles / bs {
                    let mut sql = String::new();
                    sql.push_str("INSERT INTO art (id, votes) VALUES ");
                    for i in 0..bs {
                        if i != 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str("(");
                        sql.push_str(&aid.to_string());
                        sql.push_str(", 0)");
                        aid += 1;
                    }
                    conn = conn.drop_query(sql).await.unwrap();
                }

                // also warm the cache
                let mut mset = redis::pipe();
                for i in 0..params.articles {
                    mset.cmd("SET").arg((i + 1) as i32).arg(0).ignore();
                }
                mset.query_async(&mut r_conn)
                    .await
                    .context("failed to do article prepopulation")?;
            }

            // now we connect for real
            let mut my_opts = mysql_async::OptsBuilder::from_opts(my_opts);
            my_opts.db_name(Some(db));
            my_opts.init(vec![
                "SET max_heap_table_size = 4294967296;",
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            ]);
            my_opts.stmt_cache_size(10000);

            // for redis, we need to provide _some_ backpressure, otherwise the load generator
            // won't start to batch (which we want to improve throughput). the choice of 8192 is
            // semi-random. it's the same as noria is using at time of writing.
            let redis = tower_limit::ConcurrencyLimit::new(RedisConn { c: r_conn }, 8192);

            // for mysql, fix the size of the pool so it does not keep dropping and re-starting
            // connections.
            my_opts.pool_options(mysql_async::PoolOptions::with_constraints(
                mysql_async::PoolConstraints::new(100, 100).unwrap(),
            ));

            Ok(Conn {
                pool: mysql_async::Pool::new(my_opts),
                next: None,
                pending: None,
                redis,
            })
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Conn::poll_ready(self, cx)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let redis_fut = Service::<ReadRequest>::call(&mut self.redis, req.clone());
        let r_conn = self.redis.clone();
        let my_conn = self.next.take().unwrap();
        async move {
            let misses = redis_fut.await?;
            if !misses.is_empty() {
                let req = misses;
                let len = req.len();
                let ids = req.iter().map(|a| a as &_).collect::<Vec<_>>();
                let vals = (0..len).map(|_| "?").collect::<Vec<_>>().join(",");
                let qstring = format!("SELECT id, votes FROM art WHERE id IN ({})", vals);

                let qresult = my_conn.prep_exec(qstring, &ids).await.unwrap();
                let (_, rows): (_, Vec<(i32, i32)>) = qresult.collect_and_drop().await.unwrap();

                // <= because IN() collapses duplicates
                assert!(rows.len() <= ids.len());

                // write back to the cache
                // NOTE: there's technically a race here, where we may have read the updated value,
                // then _another_ update + invalidation happens, and _then_ we write back to cache
                // here. fixing that is highly non-trivial, and out of the scope of this bench.
                ServiceExt::<Vec<(i32, i32)>>::ready_oneshot(r_conn)
                    .await?
                    .call(rows)
                    .await?;
            }

            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Conn::poll_ready(self, cx)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let redis_fut = Service::<WriteRequest>::call(&mut self.redis, req.clone());
        let conn = self.next.take().unwrap();
        async move {
            // need to first update mysql
            let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..len).map(|_| "(0, ?)").collect::<Vec<_>>().join(", ");
            let vote_qstring = format!("INSERT INTO vt (u, id) VALUES {}", vals);
            let conn = conn.drop_exec(vote_qstring, &ids).await.unwrap();

            let vals = (0..len).map(|_| "?").collect::<Vec<_>>().join(",");
            // NOTE: this is *not* correct for duplicate ids
            let vote_qstring = format!("UPDATE art SET votes = votes + 1 WHERE id IN ({})", vals);
            let _ = conn.drop_exec(vote_qstring, &ids).await.unwrap();

            // then invalidate cache
            let _ = redis_fut.await?;

            Ok(())
        }
    }
}

#[derive(Clone)]
pub(crate) struct RedisConn {
    c: redis::aio::MultiplexedConnection,
}

impl Service<ReadRequest> for RedisConn {
    type Response = Vec<i32>;
    type Error = failure::Error;
    type Future = impl Future<Output = Result<Vec<i32>, failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
        let len = req.0.len();
        let mut conn = self.c.clone();
        async move {
            if req.0.len() == 1 {
                // https://github.com/mitsuhiko/redis-rs/issues/336
                let r: Option<i32> = conn.get(req.0[0]).await?;
                if r.is_some() {
                    Ok(vec![])
                } else {
                    Ok(vec![req.0[0]])
                }
            } else {
                let rows: Vec<Option<i32>> = conn.get(&req.0[..]).await?;
                assert_eq!(rows.len(), len);
                let mut misses = Vec::new();
                for (req, res) in req.0.into_iter().zip(rows) {
                    if res.is_none() {
                        misses.push(req);
                    }
                }
                Ok(misses)
            }
        }
    }
}

impl Service<WriteRequest> for RedisConn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let mut conn = self.c.clone();
        let mut batch = redis::pipe();
        for aid in req.0 {
            batch.cmd("DEL").arg(aid).ignore();
        }

        async move {
            batch.query_async(&mut conn).await?;
            Ok(())
        }
    }
}

impl Service<Vec<(i32, i32)>> for RedisConn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Vec<(i32, i32)>) -> Self::Future {
        let mut conn = self.c.clone();
        let mut batch = redis::pipe();
        for (aid, count) in req {
            batch.cmd("SET").arg(aid).arg(count).ignore();
        }

        async move {
            batch.query_async(&mut conn).await?;
            Ok(())
        }
    }
}
