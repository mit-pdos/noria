use distributary::srv;
use distributary::{Blender, Base, Aggregation, JoinBuilder, DataType};
use tarpc;

use targets::Backend;
use targets::Putter;
use targets::Getter;

pub struct SoupTarget<D: tarpc::transport::Dialer> {
    vote: usize,
    article: usize,
    end: usize,
    addr: String,
    _srv: tarpc::ServeHandle<D>, // so the server won't quit
}

pub fn make(addr: &str, _: usize) -> SoupTarget<tarpc::transport::tcp::TcpDialer> {
    // set up graph
    let mut g = Blender::new();

    let (article, vote, end) = {
        let mut mig = g.start_migration();

        // add article base node
        let article = mig.add_ingredient("article", &["id", "title"], Base {});

        // add vote base table
        let vote = mig.add_ingredient("vote", &["user", "id"], Base {});

        // add vote count
        let vc = mig.add_ingredient("vc",
                                    &["id", "votes"],
                                    Aggregation::COUNT.over(vote, 0, &[1]));

        // add final join -- joins on first field of each input
        let j = JoinBuilder::new(vec![(article, 0), (article, 1), (vc, 1)])
            .from(article, vec![1, 0])
            .join(vc, vec![1, 0]);
        let end = mig.add_ingredient("awvc", &["id", "title", "votes"], j);

        mig.maintain(end, 0);
        mig.commit();

        (article, vote, end)
    };

    // start processing
    SoupTarget {
        vote: vote.into(),
        article: article.into(),
        end: end.into(),
        addr: addr.to_owned(),
        _srv: srv::run(g, addr),
    }
}

impl<D: tarpc::transport::Dialer> Backend for SoupTarget<D> {
    type P = (srv::ext::Client, usize, usize);
    type G = (srv::ext::Client, usize);

    fn getter(&mut self) -> Self::G {
        (srv::ext::Client::new(&self.addr).unwrap(), self.end)
    }

    fn putter(&mut self) -> Self::P {
        (srv::ext::Client::new(&self.addr).unwrap(), self.vote, self.article)
    }

    fn migrate(&mut self, ngetters: usize) -> (Self::P, Vec<Self::G>) {
        unimplemented!()
    }
}

impl Putter for (srv::ext::Client, usize, usize) {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a> {
        Box::new(move |id, title| {
            self.0.insert(self.2, vec![id.into(), title.into()]).unwrap();
        })
    }

    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a> {
        Box::new(move |user, id| { self.0.insert(self.1, vec![user.into(), id.into()]).unwrap(); })
    }
}

impl Getter for (srv::ext::Client, usize) {
    fn get<'a>(&'a self) -> Box<FnMut(i64) -> Option<(i64, String, i64)> + 'a> {
        Box::new(move |id| {
            for row in self.0.query(self.1, id.into()).unwrap().into_iter() {
                match row[1] {
                    DataType::Text(ref s) => {
                        return Some((row[0].clone().into(), (**s).clone(), row[2].clone().into()));
                    }
                    _ => unreachable!(),
                }
            }
            None
        })
    }
}
