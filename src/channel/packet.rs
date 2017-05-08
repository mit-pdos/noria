
use std::sync::mpsc;
use std::net::SocketAddr;

use serde::{Serialize, Serializer, Deserialize, Deserializer};

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
        client: souplet::SyncClient,
        client_addr: SocketAddr,

        demux_table: channel::DemuxTable,
        local_addr: SocketAddr,

        input: bool,
    },
}

impl PacketSender {
    pub fn make_remote(domain: domain::Index,
                       client: souplet::SyncClient,
                       client_addr: SocketAddr,
                       demux_table: channel::DemuxTable,
                       local_addr: SocketAddr)
                       -> Self {
        PacketSender::Remote {
            domain,
            client,
            client_addr,
            demux_table,
            local_addr,
            input: false,
        }
    }

    pub fn make_remote_input(domain: domain::Index,
                             client: souplet::SyncClient,
                             client_addr: SocketAddr,
                             demux_table: channel::DemuxTable,
                             local_addr: SocketAddr)
                             -> Self {
        PacketSender::Remote {
            domain,
            client,
            client_addr,
            demux_table,
            local_addr,
            input: true,
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
                input,
                ..
            } => {
                packet.make_serializable(local_addr, demux_table);
                if input {
                    client
                        .recv_input_packet(domain, packet)
                        .map_err(|_| Error::Unknown)
                } else {
                    client
                        .recv_packet(domain, packet)
                        .map_err(|_| Error::Unknown)
                }
            }
        }
    }

    pub fn as_local(&self) -> Option<mpsc::SyncSender<Packet>> {
        match *self {
            PacketSender::Local(ref s) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn get_client_addr(&self) -> Option<SocketAddr> {
        match *self {
            PacketSender::Remote { ref client_addr, .. } => Some(client_addr.clone()),
            _ => None,
        }
    }
}

impl From<mpsc::SyncSender<Packet>> for PacketSender {
    fn from(s: mpsc::SyncSender<Packet>) -> Self {
        PacketSender::Local(s)
    }
}

impl Serialize for PacketSender {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        unreachable!()
    }
}
impl Deserialize for PacketSender {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        unreachable!()
    }
}
