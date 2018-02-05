use channel::poll::{PollEvent, PollingLoop, ProcessResult};
use channel::tcp::TcpSender;
use consensus::{Authority, Epoch, STATE_KEY};
use dataflow::checktable::service::CheckTableServer;
use dataflow::{DomainConfig, PersistenceParameters};
use dataflow::prelude::*;

use controller::domain_handle::DomainHandle;
use controller::inner::ControllerInner;
use controller::recipe::Recipe;
use coordination::{CoordinationMessage, CoordinationPayload};

use std::error::Error;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Receiver, Sender};
use std::{io, time};

use futures::{self, Future, Stream};
use hyper::{self, Method, StatusCode};
use hyper::header::ContentType;
use hyper::server::{Http, NewService, Request, Response, Service};
use rand;
use serde::Serialize;
use serde_json;
use slog;

pub mod domain_handle;
pub mod keys;
pub mod migrate;

pub(crate) mod recipe;
pub(crate) mod sql;

mod builder;
mod getter;
mod handle;
mod inner;
mod mir_to_flow;
mod mutator;

pub use controller::builder::ControllerBuilder;
pub use controller::handle::ControllerHandle;
pub use controller::inner::RpcError;
pub use controller::migrate::Migration;
pub use controller::mutator::{Mutator, MutatorBuilder, MutatorError};
pub use controller::getter::{Getter, ReadQuery, ReadReply, RemoteGetter, RemoteGetterBuilder};
pub(crate) use controller::getter::LocalOrNot;

type WorkerIdentifier = SocketAddr;
type WorkerEndpoint = Arc<Mutex<TcpSender<CoordinationMessage>>>;

#[derive(Clone)]
struct WorkerStatus {
    healthy: bool,
    last_heartbeat: Instant,
    sender: Arc<Mutex<TcpSender<CoordinationMessage>>>,
}

impl WorkerStatus {
    pub fn new(sender: Arc<Mutex<TcpSender<CoordinationMessage>>>) -> Self {
        WorkerStatus {
            healthy: true,
            last_heartbeat: Instant::now(),
            sender,
        }
    }
}

/// Wrapper around the state of a thread serving network requests. Both tracks the address that the
/// thread is listening on and provides a way to stop it.
struct ServingThread {
    addr: SocketAddr,
    join_handle: JoinHandle<()>,
    stop: Box<Drop + Send>,
}
impl ServingThread {
    fn stop(self) {
        drop(self.stop);
        self.join_handle.join().unwrap();
    }
}

/// Describes a running controller instance. A serialized version of this struct is stored in
/// ZooKeeper so that clients can reach the currently active controller.
#[derive(Serialize, Deserialize)]
pub(crate) struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub internal_addr: SocketAddr,
    pub checktable_addr: SocketAddr,
    pub nonce: u64,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ControllerConfig {
    pub sharding: Option<usize>,
    pub partial_enabled: bool,
    pub domain_config: DomainConfig,
    pub persistence: PersistenceParameters,
    pub heartbeat_every: Duration,
    pub healthcheck_every: Duration,
    pub local_workers: usize,
    pub nworkers: usize,
    pub nreaders: usize,
}
impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sharding: Some(2),
            #[cfg(not(test))]
            sharding: None,
            partial_enabled: true,
            domain_config: DomainConfig {
                concurrent_replays: 512,
                replay_batch_timeout: time::Duration::from_millis(1),
                replay_batch_size: 32,
            },
            persistence: Default::default(),
            heartbeat_every: Duration::from_secs(1),
            healthcheck_every: Duration::from_secs(10),
            #[cfg(test)]
            local_workers: 2,
            #[cfg(not(test))]
            local_workers: 0,
            nworkers: 0,
            nreaders: 1,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub config: ControllerConfig,
    pub epoch: Epoch,
    pub snapshot_id: u64,
    pub recipe: (), // TODO: store all relevant state here.
}

pub(crate) enum ControlEvent {
    ControllerMessage(CoordinationMessage),
    ExternalRequest(
        Method,
        String,
        Vec<u8>,
        futures::sync::oneshot::Sender<Result<String, StatusCode>>,
    ),
    SnapshotCompleted((DomainIndex, usize), u64),
    WonLeaderElection(ControllerState),
    LostLeadership(Epoch),
    Shutdown,
    InitializeSnapshot,
    Error(Box<Error + Send + Sync>),
}

/// Runs the soup instance.
pub struct Controller<A: Authority + 'static> {
    current_epoch: Option<Epoch>,
    inner: Option<ControllerInner>,
    log: slog::Logger,

    listen_addr: IpAddr,
    internal: ServingThread,
    external: ServingThread,
    checktable: SocketAddr,
    _campaign: JoinHandle<()>,

    phantom: PhantomData<A>,
}

impl<A: Authority + 'static> Controller<A> {
    /// Start up a new `Controller` and return a handle to it. Dropping the handle will stop the
    /// controller.
    fn start(
        authority: A,
        listen_addr: IpAddr,
        config: ControllerConfig,
        log: slog::Logger,
    ) -> ControllerHandle<A> {
        let authority = Arc::new(authority);

        let (event_tx, event_rx) = mpsc::channel();
        let internal = Self::listen_internal(event_tx.clone(), SocketAddr::new(listen_addr, 0));
        let checktable = CheckTableServer::start(SocketAddr::new(listen_addr, 0));
        let external = Self::listen_external(
            event_tx.clone(),
            SocketAddr::new(listen_addr, 9000),
            authority.clone(),
        );

        if let Some(timeout) = config.persistence.snapshot_timeout {
            Self::initialize_snapshots(event_tx.clone(), timeout);
        }

        let descriptor = ControllerDescriptor {
            external_addr: external.addr,
            internal_addr: internal.addr,
            checktable_addr: checktable,
            nonce: rand::random(),
        };
        let campaign = Self::campaign(event_tx.clone(), authority.clone(), descriptor, config);

        let builder = thread::Builder::new().name("srv-main".to_owned());
        let controller_authority = authority.clone();
        let sender = event_tx.clone();
        let join_handle = builder
            .spawn(move || {
                let controller = Self {
                    current_epoch: None,
                    inner: None,
                    log,
                    internal,
                    external,
                    checktable,
                    _campaign: campaign,
                    listen_addr,
                    phantom: PhantomData,
                };
                controller.main_loop(controller_authority, sender, event_rx)
            })
            .unwrap();

        ControllerHandle {
            url: None,
            authority,
            local: Some((event_tx, join_handle)),
        }
    }

    fn main_loop(
        mut self,
        authority: Arc<A>,
        sender: Sender<ControlEvent>,
        receiver: Receiver<ControlEvent>,
    ) {
        for event in receiver {
            match event {
                ControlEvent::ControllerMessage(msg) => if let Some(ref mut inner) = self.inner {
                    if let CoordinationPayload::SnapshotCompleted(domain, snapshot_id) = msg.payload {
                        Self::handle_snapshot_completed(inner, authority.clone(), domain, snapshot_id);
                    }

                    inner.coordination_message(msg)
                },
                ControlEvent::SnapshotCompleted(domain, snapshot_id) => {
                    if let Some(ref mut inner) = self.inner {
                        Self::handle_snapshot_completed(inner, authority.clone(), domain, snapshot_id);
                    }
                }
                ControlEvent::ExternalRequest(method, path, body, reply_tx) => {
                    if let Some(ref mut inner) = self.inner {
                        reply_tx
                            .send(inner.external_request(method, path, body))
                            .unwrap()
                    } else {
                        reply_tx.send(Err(StatusCode::NotFound)).unwrap();
                    }
                }
                ControlEvent::WonLeaderElection(state) => {
                    self.current_epoch = Some(state.epoch);
                    self.inner = Some(ControllerInner::new(
                        self.listen_addr,
                        self.checktable,
                        self.log.clone(),
                        state,
                        sender.clone(),
                    ));
                }
                ControlEvent::LostLeadership(new_epoch) => {
                    self.current_epoch = Some(new_epoch);
                    self.inner = None;
                }
                ControlEvent::InitializeSnapshot => if let Some(ref mut inner) = self.inner {
                    inner.initialize_snapshot()
                },
                ControlEvent::Shutdown => break,
                ControlEvent::Error(e) => panic!("{}", e),
            }
        }
        self.external.stop();
        self.internal.stop();
    }

    // Persists snapshot IDs to controller's authority when all
    // domain shards have completed a snapshot.
    fn handle_snapshot_completed(
        inner: &mut ControllerInner,
        authority: Arc<A>,
        domain: (DomainIndex, usize),
        snapshot_id: u64,
    ) {
        if let Some(id) = inner.handle_snapshot_completed(domain, snapshot_id) {
            authority
                .read_modify_write("/state", |state: Option<ControllerState>| match state {
                    None => Err(()),
                    Some(mut state) => {
                        state.snapshot_id = id;
                        Ok(state)
                    }
                })
                .unwrap()
                .unwrap();
        }
    }

    fn listen_external(
        event_tx: Sender<ControlEvent>,
        addr: SocketAddr,
        authority: Arc<A>,
    ) -> ServingThread {
        struct ExternalServer<A: Authority>(Sender<ControlEvent>, Arc<A>);
        impl<A: Authority> Clone for ExternalServer<A> {
            // Needed due to #26925
            fn clone(&self) -> Self {
                ExternalServer(self.0.clone(), self.1.clone())
            }
        }
        impl<A: Authority> Service for ExternalServer<A> {
            type Request = Request;
            type Response = Response;
            type Error = hyper::Error;
            type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

            fn call(&self, req: Request) -> Self::Future {
                let mut res = Response::new();
                match (req.method().clone(), req.path().to_owned().as_ref()) {
                    (Method::Get, "/graph.html") => {
                        res.headers_mut().set(ContentType::html());
                        res.set_body(include_str!("graph.html"));
                        Box::new(futures::future::ok(res))
                    }
                    (Method::Get, "/js/layout-worker.js") => {
                        res.set_body(
                            "importScripts('https://cdn.rawgit.com/mstefaniuk/graph-viz-d3-js/\
                             cf2160ee3ca39b843b081d5231d5d51f1a901617/dist/layout-worker.js');",
                        );
                        Box::new(futures::future::ok(res))
                    }
                    (Method::Get, path) if path.starts_with("/zookeeper/") => {
                        match self.1.try_read(&format!("/{}", &path[11..])) {
                            Ok(Some(data)) => {
                                res.headers_mut().set(ContentType::json());
                                res.set_body(data);
                            }
                            _ => res.set_status(StatusCode::NotFound),
                        }
                        Box::new(futures::future::ok(res))
                    }
                    (method, path) => {
                        let path = path.to_owned();
                        let event_tx = self.0.clone();
                        Box::new(req.body().concat2().and_then(move |body| {
                            let body: Vec<u8> = body.iter().cloned().collect();
                            let (tx, rx) = futures::sync::oneshot::channel();
                            let _ = event_tx.send(ControlEvent::ExternalRequest(
                                method,
                                path,
                                body,
                                tx,
                            ));
                            rx.and_then(|reply| {
                                let mut res = Response::new();
                                match reply {
                                    Ok(reply) => res.set_body(reply),
                                    Err(status_code) => {
                                        res.set_status(status_code);
                                    }
                                }
                                Box::new(futures::future::ok(res))
                            }).or_else(|futures::Canceled| {
                                let mut res = Response::new();
                                res.set_status(StatusCode::NotFound);
                                Box::new(futures::future::ok(res))
                            })
                        }))
                    }
                }
            }
        }
        impl<A: Authority> NewService for ExternalServer<A> {
            type Request = Request;
            type Response = Response;
            type Error = hyper::Error;
            type Instance = Self;
            fn new_service(&self) -> Result<Self::Instance, io::Error> {
                Ok(self.clone())
            }
        }

        let (tx, rx) = mpsc::channel();
        let (done_tx, done_rx) = futures::sync::oneshot::channel();
        let builder = thread::Builder::new().name("srv-ext".to_owned());
        let join_handle = builder
            .spawn(move || {
                let service = ExternalServer(event_tx, authority);
                let server = Http::new().bind(&addr, service.clone());
                let server = match server {
                    Ok(s) => s,
                    Err(hyper::Error::Io(ref e))
                        if e.kind() == ErrorKind::AddrInUse && addr.port() != 0 =>
                    {
                        Http::new()
                            .bind(&SocketAddr::new(addr.ip(), 0), service)
                            .unwrap()
                    }
                    Err(e) => panic!("{}", e),
                };

                let addr = server.local_addr().unwrap();
                tx.send(addr).unwrap();
                server.run_until(done_rx.map_err(|_| ())).unwrap();
            })
            .unwrap();

        ServingThread {
            addr: rx.recv().unwrap(),
            join_handle,
            stop: Box::new(done_tx),
        }
    }

    /// Listen for messages from workers.
    fn listen_internal(event_tx: Sender<ControlEvent>, addr: SocketAddr) -> ServingThread {
        let mut done = Arc::new(());
        let done2 = done.clone();
        let mut pl: PollingLoop<CoordinationMessage> = PollingLoop::new(addr);
        let addr = pl.get_listener_addr().unwrap();
        let builder = thread::Builder::new().name("srv-int".to_owned());
        let join_handle = builder
            .spawn(move || {
                pl.run_polling_loop(move |e| {
                    if Arc::get_mut(&mut done).is_some() {
                        return ProcessResult::StopPolling;
                    }

                    match e {
                        PollEvent::ResumePolling(timeout) => {
                            *timeout = Some(Duration::from_millis(100));
                        }
                        PollEvent::Process(msg) => {
                            if event_tx.send(ControlEvent::ControllerMessage(msg)).is_err() {
                                return ProcessResult::StopPolling;
                            }
                        }
                        PollEvent::Timeout => {}
                    }
                    ProcessResult::KeepPolling
                });
            })
            .unwrap();

        ServingThread {
            addr,
            join_handle,
            stop: Box::new(done2),
        }
    }

    /// Starts a loop that attempts to initiate a snapshot every `timeout`.
    /// TODO(ekmartin): There's probably a better way of doing this.
    fn initialize_snapshots(event_tx: Sender<ControlEvent>, timeout: Duration) {
        let builder = thread::Builder::new().name("snapshots".to_owned());
        builder
            .spawn(move || loop {
                thread::sleep(timeout);
                if event_tx.send(ControlEvent::InitializeSnapshot).is_err() {
                    return;
                }
            })
            .unwrap();
    }

    fn campaign(
        event_tx: Sender<ControlEvent>,
        authority: Arc<A>,
        descriptor: ControllerDescriptor,
        config: ControllerConfig,
    ) -> JoinHandle<()> {
        let descriptor = serde_json::to_vec(&descriptor).unwrap();
        let campaign_inner =
            move |event_tx: Sender<ControlEvent>| -> Result<(), Box<Error + Send + Sync>> {
                loop {
                    // become leader
                    let current_epoch = authority.become_leader(descriptor.clone())?;
                    let state = authority.read_modify_write(
                        STATE_KEY,
                        |state: Option<ControllerState>| match state {
                            None => Ok(ControllerState {
                                config: config.clone(),
                                epoch: current_epoch,
                                snapshot_id: 0,
                                recipe: (),
                            }),
                            Some(ref state) if state.epoch > current_epoch => Err(()),
                            Some(mut state) => {
                                state.epoch = current_epoch;
                                if state.config != config {
                                    panic!("Config in Zk does not match requested config!")
                                }
                                Ok(state)
                            }
                        },
                    )?;
                    if state.is_err() {
                        continue;
                    }
                    if !event_tx
                        .send(ControlEvent::WonLeaderElection(state.unwrap()))
                        .is_ok()
                    {
                        break;
                    }

                    // watch for overthrow
                    let new_epoch = authority.await_new_epoch(current_epoch)?;
                    if !event_tx
                        .send(ControlEvent::LostLeadership(new_epoch))
                        .is_ok()
                    {
                        break;
                    }
                }
                Ok(())
            };

        thread::Builder::new()
            .name("srv-zk".to_owned())
            .spawn(move || {
                if let Err(e) = campaign_inner(event_tx.clone()) {
                    let _ = event_tx.send(ControlEvent::Error(e));
                }
            })
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus::ZookeeperAuthority;

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    #[allow_fail]
    fn it_works_default() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_default");
        {
            let _c = ControllerBuilder::default().build(authority);
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    #[allow_fail]
    fn it_works_blender_with_migration() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let authority = ZookeeperAuthority::new("127.0.0.1:2181/it_works_blender_with_migration");
        let mut c = ControllerBuilder::default().build(authority);
        assert!(c.install_recipe(r_txt.to_owned()).is_ok());
    }

    #[test]
    #[allow_fail]
    fn it_allows_authority_reuse() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let address = "127.0.0.1:2181/it_allows_authority_reuse";
        for _ in 0..2 {
            let authority = ZookeeperAuthority::new(address);
            let mut c = ControllerBuilder::default().build(authority);
            c.install_recipe(r_txt.to_owned()).unwrap();
        }
    }

    // Controller without any domains gets dropped once it leaves the scope.
    #[test]
    #[ignore]
    fn it_works_default_local() {
        // Controller gets dropped. It doesn't have Domains, so we don't see any dropped.
        {
            let _c = ControllerBuilder::default().build_local();
            thread::sleep(Duration::from_millis(100));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Controller with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration_local() {
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = ControllerBuilder::default().build_local();
        assert!(c.install_recipe(r_txt.to_owned()).is_ok());
    }
}
