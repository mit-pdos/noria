use memcached;
use memcached::proto::{MultiOperation, ProtoType};

pub struct Memcache(memcached::Client);
unsafe impl Send for Memcache {}

use common::{Writer, Reader, ArticleResult, Period};

pub fn make(addr: &str) -> Memcache {
    Memcache(memcached::Client::connect(&[(&format!("tcp://{}", addr), 1)], ProtoType::Binary)
                 .unwrap())
}

impl Writer for Memcache {
    type Migrator = ();

    fn make_articles<I>(&mut self, articles: I)
        where I: ExactSizeIterator,
              I: Iterator<Item = (i64, String)>
    {
        use std::collections::BTreeMap;
        let articles: Vec<_> = articles
            .map(|(article_id, title)| {
                     (format!("article_{}", article_id),
                      title,
                      format!("article_{}_vc", article_id),
                      b"0")
                 })
            .collect();
        let mut m = BTreeMap::new();
        for &(ref tkey, ref title, ref vck, ref vc) in &articles {
            m.insert(tkey.as_bytes(), (title.as_bytes(), 0, 0));
            m.insert(vck.as_bytes(), (&vc[..], 0, 0));
        }
        self.0.set_multi(m).unwrap();
    }

    fn vote(&mut self, ids: &[(i64, i64)]) -> Period {
        use std::collections::HashMap;
        let keys: Vec<_> = ids.iter()
            .map(|&(_, article_id)| format!("article_{}_vc", article_id))
            .collect();
        let ids: HashMap<_, _> = keys.iter()
            .map(|key| (key.as_bytes(), (1, 0, 0)))
            .collect();
        //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
        drop(self.0.increment_multi(ids));
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
