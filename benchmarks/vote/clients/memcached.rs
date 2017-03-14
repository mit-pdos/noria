use memcached;
use memcached::proto::{Operation, ProtoType};

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
    fn vote(&mut self, _: i64, article_id: i64) -> Period {
        //self.set_raw(&format!("voted_{}_{}", user, id), b"1", 0, 0).unwrap();
        drop(self.0.increment(format!("article_{}_vc", article_id).as_bytes(), 1, 0, 0));
        Period::PreMigration

    }
}

impl Reader for Memcache {
    fn get(&mut self, article_id: i64) -> (ArticleResult, Period) {
        // TODO: use mget
        //let title = self.get_raw(&format!("article_{}", id));
        let title: Result<_, ()> = Ok((Vec::from(format!("article_{}", article_id).as_bytes()),));
        let vc = self.0.get(format!("article_{}_vc", article_id).as_bytes());
        let res = match (title, vc) {
            (Ok(title), Ok(vc)) => {
                let vc: i64 = String::from_utf8_lossy(&vc.0[..]).parse().unwrap();
                ArticleResult::Article {
                    id: article_id,
                    title: String::from_utf8_lossy(&title.0[..]).into_owned(),
                    votes: vc,
                }
            }
            _ => ArticleResult::NoSuchArticle,
        };
        (res, Period::PreMigration)
    }
}
