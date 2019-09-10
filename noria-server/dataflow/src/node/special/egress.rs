use fnv::FnvHashMap;
use hdrhistogram::Histogram;
use prelude::*;
use std::collections::{HashMap, VecDeque};
use std::time;

#[derive(Serialize, Deserialize)]
struct EgressTx {
    node: NodeIndex,
    local: LocalNodeIndex,
    dest: ReplicaAddr,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Egress {
    txs: Vec<EgressTx>,
    tags: HashMap<Tag, NodeIndex>,

    /// Size of packet data in bytes
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    data_size_hist: Option<Histogram<u64>>,
    /// Time the sharder started existing
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    start: Option<time::Instant>,
    /// The next time to print data from the histograms
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    check: Option<time::Instant>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
            ..Default::default()
        }
    }
}

impl Egress {
    pub fn add_tx(&mut self, dst_g: NodeIndex, dst_l: LocalNodeIndex, addr: ReplicaAddr) {
        self.txs.push(EgressTx {
            node: dst_g,
            local: dst_l,
            dest: addr,
        });
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);

        // ygina: ugh, just want to put this here so the histogram exists
        self.data_size_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.start = Some(time::Instant::now());
        self.check = Some(time::Instant::now() + CHECK_EVERY);
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let &mut Self {
            ref mut txs,
            ref tags,
            ref mut data_size_hist,
            ref start,
            ref mut check,
        } = self;

        // send any queued updates to all external children
        assert!(!txs.is_empty());
        let txn = txs.len() - 1;

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().unwrap().tag().map(|tag| {
            tags.get(&tag)
                .cloned()
                .expect("egress node told about replay message, but not on replay path")
        });

        for (txi, ref mut tx) in txs.iter_mut().enumerate() {
            let mut take = txi == txn;
            if let Some(replay_to) = replay_to.as_ref() {
                if *replay_to == tx.node {
                    take = true;
                } else {
                    continue;
                }
            }

            // Avoid cloning if this is last send
            let mut m = if take {
                m.take().unwrap()
            } else {
                // we know this is a data (not a replay)
                // because, a replay will force a take
                m.as_ref().map(|m| box m.clone_data()).unwrap()
            };

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

            let is_message = match m {
                box Packet::Message { .. } => true,
                _ => false,
            };
            if is_message && shard == 0 {
                let h = data_size_hist.as_mut().unwrap();
                if h.record(m.size_of_data()).is_err() {
                    let m = h.high();
                    h.record(m).unwrap();
                }
                let total = data_size_hist.as_ref().unwrap().len();
                let now = time::Instant::now();
                if now > *check.as_ref().unwrap() {
                    *check = Some(check.unwrap() + CHECK_EVERY);
                    let dur = now.duration_since(start.unwrap()) / 1_000;
                    println!(
                        "EGRESS Sent {} messages in {:?} for {:?} messages / s",
                        total,
                        dur,
                        (total as u128) / dur.as_millis(),
                    );
                    println!(
                        "EGRESS Size of packet data: [{}, {}, {}, {}]",
                        data_size_hist.as_ref().unwrap().value_at_quantile(0.5),
                        data_size_hist.as_ref().unwrap().value_at_quantile(0.95),
                        data_size_hist.as_ref().unwrap().value_at_quantile(0.99),
                        data_size_hist.as_ref().unwrap().max(),
                    );
                }
            }

            output.entry(tx.dest).or_default().push_back(m);
            if take {
                break;
            }
        }
    }
}
