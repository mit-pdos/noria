#[cfg(test)]
use crate::controller::migrate::Migration;
use crate::controller::Event;
use dataflow::prelude::*;
use noria::consensus::Authority;
use noria::prelude::*;
use noria::SyncControllerHandle;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio::prelude::*;
use tokio_io_pool;

/// A handle to a controller that is running in the same process as this one.
pub struct WorkerHandle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<futures::sync::mpsc::UnboundedSender<Event>>,
    kill: Option<Trigger>,
    iopool: Option<tokio_io_pool::Runtime>,
}

impl<A: Authority> Deref for WorkerHandle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for WorkerHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> WorkerHandle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: futures::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        ControllerHandle::make(authority).map(move |c| WorkerHandle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
        })
    }

    #[cfg(test)]
    pub(crate) fn ready<E>(self) -> impl Future<Item = Self, Error = E> {
        let snd = self.event_tx.clone().unwrap();
        future::loop_fn((self, snd), |(this, snd)| {
            let (tx, rx) = futures::sync::oneshot::channel();
            snd.unbounded_send(Event::IsReady(tx)).unwrap();
            rx.map_err(|_| unimplemented!("worker loop went away"))
                .and_then(|v| {
                    if v {
                        future::Either::A(future::ok(future::Loop::Break(this)))
                    } else {
                        use std::time;
                        future::Either::B(
                            tokio::timer::Delay::new(
                                time::Instant::now() + time::Duration::from_millis(50),
                            )
                            .map(move |_| future::Loop::Continue((this, snd)))
                            .map_err(|_| unimplemented!("no timer available")),
                        )
                    }
                })
        })
    }

    #[cfg(test)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = futures::sync::oneshot::channel();
        let (fin_tx, fin_rx) = futures::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration| -> () {
            if ret_tx.send(f(m)).is_err() {
                unreachable!("could not return migration result");
            }
        });

        self.event_tx
            .clone()
            .unwrap()
            .unbounded_send(Event::ManualMigration { f: b, done: fin_tx })
            .unwrap();

        match fin_rx.wait() {
            Ok(()) => ret_rx.wait().unwrap(),
            Err(e) => unreachable!("{:?}", e),
        }
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub fn set_security_config(
        &mut self,
        p: String,
    ) -> impl Future<Item = (), Error = failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> impl Future<Item = (), Error = failure::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        self.rpc::<_, ()>(
            "create_universe",
            &context,
            "failed to create security universe",
        )
        .and_then(move |_| {
            // Write to Context table
            let bname = match context.get("group") {
                None => format!("UserContext_{}", uid.to_string()),
                Some(g) => format!("GroupContext_{}_{}", g.to_string(), uid.to_string()),
            };

            let mut fields: Vec<_> = context.keys().collect();
            fields.sort();
            let record: Vec<DataType> = fields
                .iter()
                .map(|&f| context.get(f).unwrap().clone())
                .collect();

            c.table(&bname).and_then(|table| {
                table
                    .insert(record)
                    .map_err(|e| format_err!("failed to make table: {:?}", e))
                    .map(|_| ())
            })
        })
    }

    /// Inform the local instance that it should exit.
    pub fn shutdown(&mut self) {
        if let Some(io) = self.iopool.take() {
            drop(self.c.take());
            drop(self.event_tx.take());
            drop(self.kill.take());
            io.shutdown_on_idle();
        }
    }
}

impl<A: Authority> Drop for WorkerHandle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A synchronous handle to a worker.
pub struct SyncWorkerHandle<A: Authority + 'static> {
    pub(crate) rt: Option<tokio::runtime::Runtime>,
    pub(crate) wh: WorkerHandle<A>,
    // this is an Option so we can drop it
    pub(crate) sh: Option<SyncControllerHandle<A, tokio::runtime::TaskExecutor>>,
}

impl<A: Authority> SyncWorkerHandle<A> {
    /// Construct a new synchronous handle on top of an existing runtime.
    ///
    /// Note that the given `WorkerHandle` must have been created through the given `Runtime`.
    pub fn from_existing(rt: tokio::runtime::Runtime, wh: WorkerHandle<A>) -> Self {
        let sch = wh.sync_handle(rt.executor());
        SyncWorkerHandle {
            rt: Some(rt),
            wh,
            sh: Some(sch),
        }
    }

    /// Construct a new synchronous handle on top of an existing external runtime.
    ///
    /// Note that the given `WorkerHandle` must have been created through the `Runtime` backing the
    /// given executor.
    pub fn from_executor(ex: tokio::runtime::TaskExecutor, wh: WorkerHandle<A>) -> Self {
        let sch = wh.sync_handle(ex);
        SyncWorkerHandle {
            rt: None,
            wh,
            sh: Some(sch),
        }
    }

    /// Stash away the given runtime inside this worker handle.
    pub fn wrap_rt(&mut self, rt: tokio::runtime::Runtime) {
        self.rt = Some(rt);
    }

    /// Run an operation on the underlying asynchronous worker handle.
    pub fn on_worker<F, FF>(&mut self, f: F) -> Result<FF::Item, FF::Error>
    where
        F: FnOnce(&mut WorkerHandle<A>) -> FF,
        FF: IntoFuture,
        FF::Future: Send + 'static,
        FF::Item: Send + 'static,
        FF::Error: Send + 'static,
    {
        let fut = f(&mut self.wh);
        self.sh.as_mut().unwrap().run(fut)
    }

    #[cfg(test)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.on_worker(move |w| -> Result<_, ()> { Ok(w.migrate(f)) })
            .unwrap()
    }
}

impl<A: Authority> Deref for SyncWorkerHandle<A> {
    type Target = SyncControllerHandle<A, tokio::runtime::TaskExecutor>;
    fn deref(&self) -> &Self::Target {
        self.sh.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for SyncWorkerHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sh.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Drop for SyncWorkerHandle<A> {
    fn drop(&mut self) {
        drop(self.sh.take());
        self.wh.shutdown();
        if let Some(rt) = self.rt.take() {
            rt.shutdown_now().wait().unwrap();
            // rt.shutdown_on_idle().wait().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::{SyncWorkerHandle, WorkerBuilder};
    use crate::controller::migrate::Migration;
    use dataflow::node::special::Base;
    use dataflow::node::ReplicaType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use noria::consensus::LocalAuthority;
    use petgraph::graph::NodeIndex;
    use std::{time, thread};

    fn build(sharding: Option<usize>, log: bool) -> SyncWorkerHandle<LocalAuthority> {
        use crate::logger_pls;
        let mut builder = WorkerBuilder::default();
        if log {
            builder.log_with(logger_pls());
        }
        builder.set_sharding(sharding);
        builder.start_simple().unwrap()
    }

    fn build_vote_controller(log: bool) -> SyncWorkerHandle<LocalAuthority> {
        let txt = "CREATE TABLE vote (user int, id int);\n
                   QUERY votecount: SELECT id, COUNT(*) AS votes FROM vote WHERE id = ?;";

        let mut g = build(None, log);
        g.install_recipe(txt).unwrap();
        g
    }

    #[test]
    #[should_panic]
    #[cfg_attr(not(debug_assertions), allow_fail)]
    fn limit_mutator_creation() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = WorkerBuilder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..2500 {
            let _ = c.table("a").unwrap();
        }
    }

    #[test]
    fn aggregations_have_a_replica() {
        let mut g = build_vote_controller(false);

        g.migrate(|mig| {
            assert_eq!(mig.mainline.ingredients.node_count(), 14);
            assert_eq!(mig.mainline.ingredients.edge_count(), 13);

            let a: NodeIndex = NodeIndex::new(1);
            let b: NodeIndex = NodeIndex::new(2);
            let c: NodeIndex = NodeIndex::new(3);
            let d: NodeIndex = NodeIndex::new(4);

            /*
            // identity node exists
            let identity = mig
                .mainline
                .ingredients
                .neighbors_directed(vc, petgraph::EdgeDirection::Outgoing)
                .next()
                .expect("view has one child");
            assert!(mig.mainline.ingredients[identity].is_internal());

            // create a reader
            let readers = vec![mig.maintain_anonymous(vc, &[0])];
            */

            // the replica types are marked correctly
            assert!(mig.mainline.ingredients[a].replica_type().is_none());
            assert!(mig.mainline.ingredients[b].replica_type().is_some());
            assert!(mig.mainline.ingredients[c].replica_type().is_some());
            assert!(mig.mainline.ingredients[d].replica_type().is_none());
            assert_eq!(
                mig.mainline.ingredients[b].replica_type().unwrap(),
                ReplicaType::Top { bottom_next_nodes: vec![d] },
            );
            assert_eq!(
                mig.mainline.ingredients[c].replica_type().unwrap(),
                ReplicaType::Bottom { top_prev_nodes: vec![a] },
            );
        });
    }

    #[test]
    fn packet_send_receive_sequence_numbers() {
        let mut g = build_vote_controller(false);

        println!("{}", g.graphviz().unwrap());

        println!("\ncreating mutx");
        let mut mutx = g.table("vote").unwrap().into_sync();
        println!("\ninserting a value");

        // insert a value and observe packet ids increase
        let id = 0;
        mutx.insert(vec![1337.into(), id.into()]).unwrap();
        thread::sleep(time::Duration::from_millis(2000));

        fn print_packet_info(mig: &Migration, ni: NodeIndex) {
            println!(
                "Node {} | {:?} | {:?} | {:?}",
                ni.index(),
                mig.mainline.ingredients[ni].last_packet_received,
                mig.mainline.ingredients[ni].next_packet_to_send,
                mig.mainline.ingredients[ni],
            );
        }

        g.migrate(move |mig| {
            println!("\nNodeIndex | last_packet_received | next_packet_to_send | debug");
            print_packet_info(mig, NodeIndex::new(1));
        });

        // send read requests
        println!("\ncreating a view");
        let mut q = g.view("votecount").unwrap().into_sync();
        println!("\nperforming a lookup");
        q.lookup(&[id.into()], true).unwrap();

        g.migrate(move |mig| {
            println!("\nNodeIndex | last_packet_received | next_packet_to_send | debug");
            print_packet_info(mig, NodeIndex::new(1));
        });
    }
}
