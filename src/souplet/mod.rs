use std::collections::HashMap;
use std::collections::hash_map::Keys;
use std::net::SocketAddr;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::io;
use std::process;

use futures::Future;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use tarpc::util::Never;
use tokio_core::reactor;

use slog;

use channel;
use checktable;
use flow::prelude::*;
use flow::domain;

service! {
    rpc start_domain(domain_index: domain::Index, nodes: DomainNodes);
    rpc recv_packet(domain_index: domain::Index, packet: Packet);
    rpc recv_input_packet(domain_index: domain::Index, packet: Packet);
    rpc recv_on_channel(tag: u64, data: Vec<u8>);
    rpc close_channel(tag: u64);
    rpc shutdown();
}

struct SoupletServerInner {
    pub domain_rxs: HashMap<domain::Index, mpsc::SyncSender<Packet>>,
    pub domain_input_rxs: HashMap<domain::Index, mpsc::SyncSender<Packet>>,
}

#[derive(Clone)]
struct SoupletServer {
    inner: Arc<Mutex<SoupletServerInner>>,
    demux_table: channel::DemuxTable,
    remote: reactor::Remote,
}

impl SoupletServer {
    pub fn new(demux_table: channel::DemuxTable, remote: reactor::Remote) -> Self {
        let inner = SoupletServerInner {
            domain_rxs: HashMap::new(),
            domain_input_rxs: HashMap::new(),
        };

        Self { inner: Arc::new(Mutex::new(inner)), demux_table, remote }
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

        println!("Started Domain");
        Ok(())
    }

    type RecvPacketFut = Result<(), Never>;
    fn recv_packet(&self, domain_index: domain::Index, packet: Packet) -> Self::RecvPacketFut {
        println!("Received Packet: {:?}", packet);
        let inner = self.inner.lock().unwrap();
        inner.domain_rxs[&domain_index].send(packet).unwrap();
        Ok(())
    }

    type RecvInputPacketFut = Result<(), Never>;
    fn recv_input_packet(&self,
                         domain_index: domain::Index,
                         packet: Packet)
                         -> Self::RecvInputPacketFut {
        let inner = self.inner.lock().unwrap();
        inner.domain_input_rxs[&domain_index]
            .send(packet)
            .unwrap();
        Ok(())
    }

    type ShutdownFut = Result<(), Never>;
    fn shutdown(&self) -> Self::ShutdownFut {
        process::exit(0)
    }

    type RecvOnChannelFut = Result<(), Never>;
    fn recv_on_channel(&self, tag: u64, data: Vec<u8>) -> Self::RecvOnChannelFut {
        let demux_table = &self.demux_table.lock().unwrap().1;
        demux_table[&tag].recv_bytes(&data[..]).unwrap();
        Ok(())
    }

    type CloseChannelFut = Result<(), Never>;
    fn close_channel(&self, tag: u64) -> Self::CloseChannelFut {
        let mut demux_table = &mut self.demux_table.lock().unwrap().1;
        demux_table.remove(&tag);
        Ok(())
    }

}

pub struct Souplet {
    reactor: reactor::Remote,
    reactor_thread: thread::JoinHandle<()>,
    peers: HashMap<SocketAddr, FutureClient>,
    demux_table: channel::DemuxTable,
    local_addr: SocketAddr,
}

impl Souplet {
    pub fn new(addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel();

        let demux_table = Arc::new(Mutex::new((0, HashMap::new())));
        let demux_table2 = demux_table.clone();

        let reactor_thread = thread::spawn(move ||{
            let mut reactor = reactor::Core::new().unwrap();
            tx.send(reactor.remote()).unwrap();

            let (_handle, server) = SoupletServer::new(demux_table2, reactor.remote())
                .listen(addr, &reactor.handle(), server::Options::default())
                .unwrap();
            reactor.handle().spawn(server);

            loop {
                reactor.turn(None)
            }
        });

        Self {
            reactor: rx.recv().unwrap(),
            reactor_thread,
            peers: HashMap::new(),
            demux_table,
            local_addr: addr,
        }
    }

    pub fn connect_to_peer(&mut self, addr: SocketAddr) -> io::Result<()> {
        let options = client::Options::default().remote(self.reactor.clone());
        FutureClient::connect(addr, options).map(|fc| {
            self.peers.insert(addr, fc);
        }).wait()
    }

    pub fn start_domain(&self,
                        peer: &SocketAddr,
                        domain: domain::Index,
                        nodes: DomainNodes)
                        -> channel::PacketSender {
        let peer = &self.peers[peer];
        peer.start_domain(domain, nodes).wait().unwrap();

        channel::PacketSender::make_remote(domain, peer.clone(), self.demux_table.clone(),self.local_addr)
    }

    // pub fn send_packet(&self, peer: &SocketAddr, domain: domain::Index, mut packet: Packet) {
    //     packet.make_serializable();

    //     self.peers[peer]
    //         .recv_packet(domain, packet)
    //         .wait()
    //         .unwrap();
    // }

    // pub fn send_input_packet(&self, peer: &SocketAddr, domain: domain::Index, mut packet: Packet) {
    //     packet.make_serializable();

    //     self.peers[peer]
    //         .recv_input_packet(domain, packet)
    //         .wait()
    //         .unwrap();
    // }

    pub fn listen(self) {
        self.reactor_thread.join().unwrap();
    }

    pub fn get_peers(&self) -> Keys<SocketAddr, FutureClient> {
        self.peers.keys()
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
