use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::marker::PhantomData;
use std::mem;

use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde_json;
use tokio_core::reactor;

mod packet;
pub use self::packet::PacketSender;

pub type DemuxTable = Arc<Mutex<(u64, HashMap<u64, Box<BytesEndpoint>>)>>;

#[derive(Serialize, Deserialize)]
struct SenderDef<T> {
    addr: SocketAddr,
    tag: u64,

    phantom: PhantomData<T>,
}

#[derive(Clone, Debug)]
pub enum GenericSender<T, TS>
    where TS: TypedSender<T> + BytesEndpoint
{
    Local(TS),
    Serialized(SocketAddr, u64),
    Invalid(PhantomData<T>),
}
impl<T: Send + Serialize + Deserialize + 'static, TS: TypedSender<T> + BytesEndpoint + 'static>
    GenericSender<T, TS> {
    pub fn send(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        match *self {
            GenericSender::Local(ref s) => s.tsend(t),
            GenericSender::Serialized(..) => unreachable!(),
            GenericSender::Invalid(..) => unreachable!(),
        }
    }

    pub fn make_serializable(&mut self, local_addr: SocketAddr, demux_table: &DemuxTable) {
        match mem::replace(self, GenericSender::Invalid(PhantomData)) {
            GenericSender::Local(s) => {
                let (ref mut ntag, ref mut dt) = *demux_table.lock().unwrap();
                let tag = *ntag;
                dt.insert(tag, Box::new(s));
                *ntag += 1;
                *self = GenericSender::Serialized(local_addr, tag);
            }
            s @ GenericSender::Serialized(..) => {
                *self = s;
            }
            GenericSender::Invalid(..) => unreachable!(),
        }
    }
    pub fn complete_deserialize(&mut self, remote: &reactor::Remote) {
        unimplemented!()
    }
    pub fn unwrap_local(self) -> TS {
        if let GenericSender::Local(s) = self {
            s
        } else {
            unreachable!()
        }
    }
}
impl<T: Send + Serialize + Deserialize, TS: TypedSender<T> + BytesEndpoint> Serialize
    for GenericSender<T, TS> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            GenericSender::Serialized(addr, tag) => {
                let def = SenderDef::<T> {
                    addr,
                    tag,
                    phantom: PhantomData,
                };
                def.serialize(serializer)
            }
            _ => unreachable!(),
        }
    }
}
impl<T: Send + Serialize + Deserialize, TS: TypedSender<T> + BytesEndpoint> Deserialize
    for GenericSender<T, TS> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let def = SenderDef::<T>::deserialize(deserializer);
        def.map(|d| GenericSender::Serialized(d.addr, d.tag))
    }
}
impl<T: Send + Serialize + Deserialize, TS: TypedSender<T> + BytesEndpoint> From<TS>
    for GenericSender<T, TS> {
    fn from(s: TS) -> Self {
        GenericSender::Local(s)
    }
}

pub type Sender<T> = GenericSender<T, mpsc::Sender<T>>;
pub type SyncSender<T> = GenericSender<T, mpsc::SyncSender<T>>;

pub trait BytesEndpoint: Send {
    fn recv_bytes(&self, data: &[u8]) -> Result<(), ()>;
}

impl<T: Deserialize + Send> BytesEndpoint for mpsc::Sender<T> {
    fn recv_bytes(&self, data: &[u8]) -> Result<(), ()> {
        let t = serde_json::from_slice(data).unwrap();
        self.send(t).map_err(|_| {})
    }
}
impl<T: Deserialize + Send> BytesEndpoint for mpsc::SyncSender<T> {
    fn recv_bytes(&self, data: &[u8]) -> Result<(), ()> {
        let t = serde_json::from_slice(data).unwrap();
        self.send(t).map_err(|_| {})
    }
}

pub trait TypedSender<T> {
    fn tsend(&self, t: T) -> Result<(), mpsc::SendError<T>>;
}
impl<T: Send> TypedSender<T> for mpsc::Sender<T> {
    fn tsend(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        self.send(t)
    }
}
impl<T: Send> TypedSender<T> for mpsc::SyncSender<T> {
    fn tsend(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        self.send(t)
    }
}
