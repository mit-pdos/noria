use std::net::SocketAddr;
use std::sync::mpsc;
use std::marker::PhantomData;

use serde::{Serialize, Serializer, Deserialize, Deserializer};

#[derive(Serialize, Deserialize)]
struct SenderDef<T> {
    phantom: PhantomData<T>,
}

#[derive(Clone, Debug)]
pub enum SyncSender<T: Send + Serialize + Deserialize> {
    Local(mpsc::SyncSender<T>),
    Remote,
}
impl<T: Send + Serialize + Deserialize> SyncSender<T> {
    pub fn send(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        match *self {
            SyncSender::Local(ref s) => s.send(t),
            SyncSender::Remote => unreachable!(),
        }
    }
    pub fn make_serializable(&mut self) {
        unimplemented!();
    }
}
impl<T: Send + Serialize + Deserialize> Serialize for SyncSender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            SyncSender::Remote => {
                let def = SenderDef::<T> { phantom: PhantomData };
                def.serialize(serializer)
            }
            _ => unreachable!(),
        }
    }
}
impl<T: Send + Serialize + Deserialize> Deserialize for SyncSender<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let def = SenderDef::<T>::deserialize(deserializer);
        Ok(SyncSender::Remote)
    }
}
impl<T: Send + Serialize + Deserialize> From<mpsc::SyncSender<T>> for SyncSender<T> {
    fn from(s: mpsc::SyncSender<T>) -> Self {
        SyncSender::Local(s)
    }
}
impl<T: Send + Serialize + Deserialize> Into<mpsc::SyncSender<T>> for SyncSender<T> {
    fn into(self) -> mpsc::SyncSender<T> {
        if let SyncSender::Local(s) = self {
            s
        } else {
            unreachable!()
        }
    }
}

#[derive(Clone, Debug)]
pub enum Sender<T: Send + Serialize + Deserialize> {
    Local(mpsc::Sender<T>),
    Remote,
}
impl<T: Send + Serialize + Deserialize> Sender<T> {
    pub fn send(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        match *self {
            Sender::Local(ref s) => s.send(t),
            Sender::Remote => unreachable!(),
        }
    }
    pub fn make_serializable(&mut self) {
        unimplemented!();
    }
}
impl<T: Send + Serialize + Deserialize> Serialize for Sender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            Sender::Remote => {
                let def = SenderDef::<T> { phantom: PhantomData };
                def.serialize(serializer)
            }
            _ => unreachable!(),
        }
    }
}
impl<T: Send + Serialize + Deserialize> Deserialize for Sender<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer
    {
        let def = SenderDef::<T>::deserialize(deserializer);
        Ok(Sender::Remote)
    }
}
impl<T: Send + Serialize + Deserialize> From<mpsc::Sender<T>> for Sender<T> {
    fn from(s: mpsc::Sender<T>) -> Self {
        Sender::Local(s)
    }
}
impl<T: Send + Serialize + Deserialize> Into<mpsc::Sender<T>> for Sender<T> {
    fn into(self) -> mpsc::Sender<T> {
        if let Sender::Local(s) = self {
            s
        } else {
            unreachable!()
        }
    }
}

// #[derive(Serialize, Deserialize)]
// struct ReceiverDef<T> {
//     phantom: PhantomData<T>,
// }

// #[derive(Debug)]
// pub enum Receiver<T: Send + Serialize + Deserialize> {
//     Local(mpsc::Receiver<T>),
//     Remote,
// }
// impl<T: Send + Serialize + Deserialize> Receiver<T> {
//     pub fn recv(&self) -> Result<T, mpsc::RecvError> {
//         match *self {
//             Receiver::Local(ref r) => r.recv(),
//             Receiver::Remote => unreachable!(),
//         }
//     }
// }
// impl<T: Send + Serialize + Deserialize> Serialize for Receiver<T> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//         where S: Serializer
//     {
//         match *self {
//             Receiver::Remote => {
//                 let def = ReceiverDef::<T> { phantom: PhantomData };
//                 def.serialize(serializer)
//             }
//             _ => unreachable!(),
//         }
//     }
// }
// impl<T: Send + Serialize + Deserialize> Deserialize for Receiver<T> {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//         where D: Deserializer
//     {
//         let def = ReceiverDef::<T>::deserialize(deserializer);
//         Ok(Receiver::Remote)
//     }
// }
// impl<T: Send + Serialize + Deserialize> From<mpsc::Receiver<T>> for Receiver<T> {
//     fn from(r: mpsc::Receiver<T>) -> Self {
//         Receiver::Local(r)
//     }
// }
