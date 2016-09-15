use bmemcached::MemcachedClient;

use Backend;
use Putter;
use Getter;

pub fn make(dbn: &str, getters: usize) -> Box<Backend> {
    Box::new((0..(getters + 1))
        .into_iter()
        .map(|_| MemcachedClient::new(vec![dbn], 1).unwrap())
        .collect::<Vec<_>>())
}

impl Backend for Vec<MemcachedClient> {
    fn getter(&mut self) -> Box<Getter> {
        Box::new(self.pop().unwrap())
    }

    fn putter(&mut self) -> Box<Putter> {
        Box::new(self.pop().unwrap())
    }
}

impl Putter for MemcachedClient {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            println!("setting article_{} to {} and article_{}_vc to {}",
                     id,
                     title,
                     id,
                     0);
            self.set(&format!("article_{}", id), &title, 0).unwrap();
            self.set(&format!("article_{}_vc", id), 0u32, 0).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| {
            self.set(&format!("voted_{}_{}", user, id), 1u8, 0).unwrap();
            self.increment(&format!("article_{}_vc", id), 1, 0, 0).unwrap();
        })
    }
}

impl Getter for MemcachedClient {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> (i64, String, i64) + 'a> {
        Box::new(move |id| {
            println!("getting article_{} and article_{}_vc", id, id);
            let title = MemcachedClient::get(self, &format!("article_{}", id)).unwrap();
            let vc: u64 = MemcachedClient::get(self, &format!("article_{}_vc", id)).unwrap();
            (id, title, vc as i64)
        })
    }
}
