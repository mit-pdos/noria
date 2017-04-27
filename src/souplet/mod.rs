use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::io;

use futures::Future;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use tarpc::util::Never;
use tokio_core::reactor;

use slog;

use flow::prelude::*;
use flow::domain;
use checktable;

service! {
    rpc start_domain(domain_index: domain::Index, nodes: DomainNodes);
    rpc recv_packet(domain_index: domain::Index, packet: Packet);
    rpc recv_input_packet(domain_index: domain::Index, packet: Packet);
}

struct SoupletServerInner {
    pub domain_rxs: HashMap<domain::Index, mpsc::SyncSender<Packet>>,
    pub domain_input_rxs: HashMap<domain::Index, mpsc::SyncSender<Packet>>,
}

#[derive(Clone)]
struct SoupletServer {
    inner: Arc<Mutex<SoupletServerInner>>,
}

impl SoupletServer {
    pub fn new() -> Self {
        let inner = SoupletServerInner {
            domain_rxs: HashMap::new(),
            domain_input_rxs: HashMap::new(),
        };

        Self { inner: Arc::new(Mutex::new(inner)) }
    }
}

impl FutureService for SoupletServer {
    type StartDomainFut = Result<(), Never>;
    fn start_domain(&self,
                    domain_index: domain::Index,
                    nodes: DomainNodes)
                    -> Self::StartDomainFut {
        let mock_logger = slog::Logger::root(slog::Discard, None);
        let mock_checktable = Arc::new(Mutex::new(checktable::CheckTable::new()));

        let (in_tx, in_rx) = mpsc::sync_channel(256);
        let (tx, rx) = mpsc::sync_channel(1);

        let domain = domain::Domain::new(mock_logger, domain_index, nodes, mock_checktable, 0);
        domain.boot(rx, in_rx);

        let mut inner = self.inner.lock().unwrap();
        inner.domain_rxs.insert(domain_index, tx);
        inner.domain_input_rxs.insert(domain_index, in_tx);

        Ok(())
    }

    type RecvPacketFut = Result<(), Never>;
    fn recv_packet(&self, domain_index: domain::Index, packet: Packet) -> Self::RecvPacketFut {
        let mut inner = self.inner.lock().unwrap();
        inner.domain_rxs[&domain_index].send(packet).unwrap();
        Ok(())
    }

    type RecvInputPacketFut = Result<(), Never>;
    fn recv_input_packet(&self,
                         domain_index: domain::Index,
                         packet: Packet)
                         -> Self::RecvInputPacketFut {
        let mut inner = self.inner.lock().unwrap();
        inner.domain_input_rxs[&domain_index]
            .send(packet)
            .unwrap();
        Ok(())
    }
}

pub struct Souplet {
    reactor: reactor::Core,
    peers: HashMap<SocketAddr, FutureClient>,
}

impl Souplet {
    pub fn new(addr: SocketAddr) -> Self {
        let reactor = reactor::Core::new().unwrap();
        let (_handle, server) = SoupletServer::new()
            .listen(addr, &reactor.handle(), server::Options::default())
            .unwrap();
        reactor.handle().spawn(server);

        Self {
            reactor,
            peers: HashMap::new(),
        }
    }

    pub fn connect_to_peer(&mut self, addr: SocketAddr) -> io::Result<()> {
        let options = client::Options::default().handle(self.reactor.handle());
        self.reactor.run(FutureClient::connect(addr, options)).map(|fc| {
            self.peers.insert(addr, fc);
        })
    }

    pub fn start_domain(&self, peer: &SocketAddr, domain: domain::Index, nodes: DomainNodes) {
        self.peers[peer]
            .start_domain(domain, nodes)
            .wait()
            .unwrap();
    }

    pub fn send_packet(&self, peer: &SocketAddr, domain: domain::Index, packet: Packet) {
        self.peers[peer]
            .recv_packet(domain, packet)
            .wait()
            .unwrap();
    }

    pub fn send_input_packet(&self, peer: &SocketAddr, domain: domain::Index, packet: Packet) {
        self.peers[peer]
            .recv_input_packet(domain, packet)
            .wait()
            .unwrap();
    }

    pub fn listen(&mut self) {
        loop {
            self.reactor.turn(None)
        }
    }
}

/// A `SoupletDaemon` listens for incoming connections, and starts up domains as requested.
pub struct SoupletDaemon {}
impl SoupletDaemon {
    /// Start a new WorkerDaemon instance.
    pub fn start(addr: SocketAddr) {
        Souplet::new(addr).listen();
    }
}
