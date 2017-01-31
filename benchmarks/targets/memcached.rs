use memcache;

pub struct Memcache(memcache::Memcache);
unsafe impl Send for Memcache {}

use std::ops::Deref;
impl Deref for Memcache {
    type Target = memcache::Memcache;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub fn make(dbn: &str, getters: usize) -> Vec<Memcache> {
    let mut dbn = dbn.splitn(2, ':');
    let host = dbn.next().unwrap();
    let port: u64 = dbn.next().unwrap().parse().unwrap();
    (0..(getters + 1))
        .into_iter()
        .map(|_| Memcache(memcache::connect(&(host, port)).unwrap()))
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
        Box::new(move |id, title| {
            self.set_raw(&format!("article_{}", id), title.as_bytes(), 0, 0).unwrap();
            self.set_raw(&format!("article_{}_vc", id), b"0", 0, 0).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
            self.increment(&format!("article_{}_vc", id), 1).unwrap();
        })
    }
}

impl Getter for Memcache {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a> {
        Box::new(move |id| {
            let title = self.get_raw(&format!("article_{}", id));
            let vc = self.get_raw(&format!("article_{}_vc", id));
            match (title, vc) {
                (Ok(title), Ok(vc)) => {
                    let vc: i64 = String::from_utf8_lossy(&vc.0[..]).parse().unwrap();
                    Ok(Some((id, String::from_utf8_lossy(&title.0[..]).into_owned(), vc)))
                }
                _ => Err(()),
            }
        })
    }
}
