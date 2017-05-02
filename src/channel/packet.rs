
use futures::Future;

use std::sync::mpsc;
use std::net::SocketAddr;

use flow::payload::Packet;
use flow::domain;
use souplet;
use channel;


#[derive(Debug)]
pub enum Error {
    Unknown,
}

#[derive(Clone)]
pub enum PacketSender {
    Local(mpsc::SyncSender<Packet>),
    Remote {
        domain: domain::Index,
        client: souplet::FutureClient,
        local_addr: SocketAddr,
        demux_table: channel::DemuxTable,
    },
}

impl PacketSender {
    pub fn make_remote(domain: domain::Index,
                       client: souplet::FutureClient,
                       demux_table: channel::DemuxTable,
                       local_addr: SocketAddr)
                       -> Self {
        PacketSender::Remote {
            domain,
            client,
            demux_table,
            local_addr,
        }
    }

    pub fn send(&self, mut packet: Packet) -> Result<(), Error> {
        match *self {
            PacketSender::Local(ref s) => s.send(packet).map_err(|_| Error::Unknown),
            PacketSender::Remote {
                domain,
                ref client,
                local_addr,
                ref demux_table,
            } => {
                packet.make_serializable(local_addr, demux_table);
                client.recv_packet(domain, packet)
                    .wait()
                    .map_err(|_| Error::Unknown)
            }
        }
    }

    pub fn as_local(&self) -> Option<mpsc::SyncSender<Packet>> {
        match *self {
            PacketSender::Local(ref s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl From<mpsc::SyncSender<Packet>> for PacketSender {
    fn from(s: mpsc::SyncSender<Packet>) -> Self {
        PacketSender::Local(s)
    }
}
