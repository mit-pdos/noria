use memcached;
use memcached::proto::{Operation, ProtoType};

pub struct Memcache(memcached::Client);
unsafe impl Send for Memcache {}

use std::ops::Deref;
impl Deref for Memcache {
    type Target = memcached::Client;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub fn make(dbn: &str, getters: usize) -> Vec<Memcache> {
    (0..(getters + 1))
        .into_iter()
        .map(|_| {
            Memcache(memcached::Client::connect(&[(&format!("tcp://{}", dbn), 1)],
                                                ProtoType::Binary)
                .unwrap())
        })
        .collect::<Vec<_>>()
}

impl Backend for Vec<Memcache> {
    type P = Memcache;
    type G = Memcache;

    fn getter(&mut self) -> Self::G {
        self.pop().unwrap()
    }

    fn putter(&mut self) -> Self::P {
        self.pop().unwrap()
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for Memcache {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        let ref mut memd = self.0;
        Box::new(move |id, title| {
            memd.set(format!("article_{}", id).as_bytes(), title.as_bytes(), 0, 0)
                .unwrap();
            memd.set(format!("article_{}_vc", id).as_bytes(), b"0", 0, 0).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        let ref mut memd = self.0;
        Box::new(move |_user, id| {
            //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
            drop(memd.increment(format!("article_{}_vc", id).as_bytes(), 1, 0, 0));
        })
    }
}

impl Getter for Memcache {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        Box::new(move |id| {
            // TODO: use mget
            //let title = self.get_raw(&format!("article_{}", id));
            let title: Result<_, ()> = Ok((Vec::from(format!("article_{}", id).as_bytes()),));
            let vc = self.0.get(format!("article_{}_vc", id).as_bytes());
            match (title, vc) {
                (Ok(title), Ok(vc)) => {
                    let vc: i64 = String::from_utf8_lossy(&vc.0[..]).parse().unwrap();
                    Ok(Some((id, String::from_utf8_lossy(&title.0[..]).into_owned(), vc)))
                }
                _ => Ok(None),
            }
        })
    }
}
