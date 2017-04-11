use memcached;
use memcached::proto::{Operation, MultiOperation, ProtoType};

pub struct Memcache(memcached::Client);
unsafe impl Send for Memcache {}

use common::{Writer, Reader, ArticleResult, Period};

pub fn make_writer(addr: &str) -> Memcache {
    Memcache(memcached::Client::connect(&[(&format!("tcp://{}", addr), 1)], ProtoType::Binary)
                 .unwrap())
}

pub fn make_reader(addr: &str) -> Memcache {
    Memcache(memcached::Client::connect(&[(&format!("tcp://{}", addr), 1)], ProtoType::Binary)
                 .unwrap())
}

impl Writer for Memcache {
    type Migrator = ();

    fn make_article(&mut self, article_id: i64, title: String) {
        self.0
            .set(format!("article_{}", article_id).as_bytes(),
                 title.as_bytes(),
                 0,
                 0)
            .unwrap();
        self.0.set(format!("article_{}_vc", article_id).as_bytes(), b"0", 0, 0).unwrap();
    }
    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        for &(_, article_id) in ids {
            //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
            drop(self.0.increment(format!("article_{}_vc", article_id).as_bytes(), 1, 0, 0));
        }
        Period::PreMigration

    }
}

impl Reader for Memcache {
    fn get(&mut self, ids: &[(i64, i64)]) -> (Result<Vec<ArticleResult>, ()>, Period) {
        let keys: Vec<_> = ids.iter()
            .flat_map(|&(_, ref article_id)| {
                          vec![format!("article_{}", article_id),
                               format!("article_{}_vc", article_id)]
                      })
            .collect();
        let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();

        let vals = match self.0.get_multi(&keys[..]) {
            Err(_) => return (Err(()), Period::PreMigration),
            Ok(v) => v,
        };

        let res = keys.iter()
            .enumerate()
            .filter_map(|(i, &key)| {
                if i % 2 == 1 {
                    // already read
                    return None;
                }

                // title (vc has key i+1)
                let r = match (vals.get(key), vals.get(keys[i + 1])) {
                    (Some(title), Some(vc)) => {
                        let vc: i64 = String::from_utf8_lossy(&vc.0[..]).parse().unwrap();
                        ArticleResult::Article {
                            id: ids[i / 2].1,
                            title: String::from_utf8_lossy(&title.0[..]).into_owned(),
                            votes: vc,
                        }
                    }
                    _ => ArticleResult::NoSuchArticle,
                };

                Some(r)
            })
            .collect();

        (Ok(res), Period::PreMigration)
    }
}
