use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::marker::PhantomData;
use std::mem;

use tarpc::sync::client;
use tarpc::sync::client::ClientExt;

use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::DeserializeOwned;
use serde_json;

mod packet;
pub use self::packet::PacketSender;

use souplet;

pub type DemuxTable = Arc<Mutex<(u64, HashMap<u64, Box<BytesEndpoint>>)>>;

#[derive(Serialize, Deserialize)]
struct SenderDef<T> {
    addr: SocketAddr,
    tag: u64,

    phantom: PhantomData<T>,
}

#[derive(Debug)]
enum GenericSenderInner<T, TS: TypedSender<T> + BytesEndpoint> {
    Local(TS),
    Remote(SocketAddr, u64, souplet::SyncClient),
    Serialized(SocketAddr, u64),
    Invalid(PhantomData<T>),
}

#[derive(Debug)]
pub struct GenericSender<T, TS: TypedSender<T> + BytesEndpoint> {
    inner: GenericSenderInner<T, TS>,
}

impl<'de, T: Send + Serialize + Deserialize<'de> + 'static, TS: TypedSender<T> + BytesEndpoint + 'static>
    GenericSender<T, TS> {
    pub fn send(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        match self.inner {
            GenericSenderInner::Local(ref s) => s.tsend(t),
            GenericSenderInner::Remote(_, tag, ref client) => {
                client
                    .recv_on_channel(tag, serde_json::to_vec(&t).unwrap())
                    .unwrap();
                Ok(())
            }
            GenericSenderInner::Serialized(..) => unreachable!(),
            GenericSenderInner::Invalid(..) => unreachable!(),
        }
    }

    pub fn make_serializable(&mut self, local_addr: SocketAddr, demux_table: &DemuxTable) {
        match mem::replace(&mut self.inner, GenericSenderInner::Invalid(PhantomData)) {
            GenericSenderInner::Local(s) => {
                let (ref mut ntag, ref mut dt) = *demux_table.lock().unwrap();
                let tag = *ntag;
                dt.insert(tag, Box::new(s));
                *ntag += 1;
                self.inner = GenericSenderInner::Serialized(local_addr, tag);
            }
            GenericSenderInner::Remote(addr, tag, _) => {
                self.inner = GenericSenderInner::Serialized(addr, tag);
            }
            s @ GenericSenderInner::Serialized(..) => {
                self.inner = s;
            }
            GenericSenderInner::Invalid(..) => unreachable!(),
        }
    }
    pub fn unwrap_local(mut self) -> TS {
        if let GenericSenderInner::Local(s) =
            mem::replace(&mut self.inner, GenericSenderInner::Invalid(PhantomData)) {
            mem::forget(self);
            s
        } else {
            unreachable!()
        }
    }
    pub fn is_local(&self) -> bool {
        if let GenericSenderInner::Local(_) = self.inner {
            true
        } else {
            false
        }
    }
}
impl<T, TS: TypedSender<T> + BytesEndpoint> Drop for GenericSender<T, TS> {
    fn drop(&mut self) {
        match self.inner {
            GenericSenderInner::Remote(_, tag, ref client) => {
                let _ = client.close_channel(tag);
            }
            GenericSenderInner::Local(..) |
            GenericSenderInner::Serialized(..) => {}
            GenericSenderInner::Invalid(..) => unreachable!(),
        }
    }
}
impl<'de, T: Send + Serialize + Deserialize<'de>, TS: TypedSender<T> + BytesEndpoint> Serialize
    for GenericSender<T, TS> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match self.inner {
            GenericSenderInner::Serialized(addr, tag) => {
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
impl<'de, T: Send + Serialize + Deserialize<'de>, TS: TypedSender<T> + BytesEndpoint> Deserialize<'de>
    for GenericSender<T, TS> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let def = SenderDef::<T>::deserialize(deserializer);
        def.map(|d| {
                    let client = souplet::SyncClient::connect(d.addr, client::Options::default())
                        .unwrap();
                    Self { inner: GenericSenderInner::Remote(d.addr, d.tag, client) }
                })
    }
}
impl<'de, T: Send + Serialize + Deserialize<'de>, TS: TypedSender<T> + BytesEndpoint> From<TS>
    for GenericSender<T, TS> {
    fn from(s: TS) -> Self {
        Self { inner: GenericSenderInner::Local(s) }
    }
}

pub type Sender<T> = GenericSender<T, mpsc::Sender<T>>;
pub type SyncSender<T> = GenericSender<T, mpsc::SyncSender<T>>;

pub trait BytesEndpoint: Send {
    fn recv_bytes(&self, data: &[u8]) -> Result<(), ()>;
}

impl<T> BytesEndpoint for mpsc::Sender<T> where T: DeserializeOwned + Send {
    fn recv_bytes(&self, data: &[u8]) -> Result<(), ()> {
        let t = serde_json::from_slice(data).unwrap();
        self.send(t).map_err(|_| {})
    }
}
impl<T> BytesEndpoint for mpsc::SyncSender<T> where T: DeserializeOwned + Send {
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
