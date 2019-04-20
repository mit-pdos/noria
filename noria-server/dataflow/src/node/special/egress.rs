use fnv::FnvHashMap;
use prelude::*;
use std::collections::{HashMap, VecDeque};

#[derive(Serialize, Deserialize)]
struct EgressTx {
    node: NodeIndex,
    local: LocalNodeIndex,
    dest: ReplicaAddr,
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
enum Where {
    InAll(usize),
    InReplay(usize),
}

#[derive(Serialize, Deserialize)]
pub struct Egress {
    all_txs: Vec<EgressTx>,
    replay_txs: Vec<EgressTx>,
    tags: HashMap<Tag, Where>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert_eq!(self.all_txs.len() + self.replay_txs.len(), 0);
        Self {
            all_txs: Vec::new(),
            replay_txs: Vec::new(),
            tags: self.tags.clone(),
        }
    }
}

impl Default for Egress {
    fn default() -> Self {
        Self {
            tags: Default::default(),
            all_txs: Default::default(),
            replay_txs: Default::default(),
        }
    }
}

impl Egress {
    pub fn add_tx(&mut self, dst_g: NodeIndex, dst_l: LocalNodeIndex, addr: ReplicaAddr) {
        self.all_txs.push(EgressTx {
            node: dst_g,
            local: dst_l,
            dest: addr,
        });
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        let i = self.all_txs.iter().position(|tx| tx.node == dst);
        if let Some(i) = i {
            self.tags.insert(tag, Where::InAll(i));
            return;
        }

        let i = self
            .replay_txs
            .iter()
            .position(|tx| tx.node == dst)
            .expect("egress node told about unknown tag destination");
        self.tags.insert(tag, Where::InReplay(i));
    }

    pub fn drop_writes_to(&mut self, dst: NodeIndex) {
        // remove from all_txs so we don't even have to iterate over it
        let mv = self
            .all_txs
            .iter()
            .position(|etx| etx.node == dst)
            .expect("told to drop writes to unknown node");
        let tx = self.all_txs.swap_remove(mv);
        self.replay_txs.push(tx);

        // update the tag refs list
        for tag in self.tags.values_mut() {
            if let Where::InAll(i) = *tag {
                if i == mv {
                    *tag = Where::InReplay(self.replay_txs.len() - 1);
                    break;
                }
            }
        }
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let &mut Self {
            ref mut all_txs,
            ref mut replay_txs,
            ref tags,
        } = self;
        assert_ne!(all_txs.len() + replay_txs.len(), 0);

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().unwrap().tag().map(|tag| {
            *tags
                .get(&tag)
                .expect("egress node told about replay message, but not on replay path")
        });

        let mut send = |tx: &mut EgressTx, mut m: Box<Packet>| {
            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

            output.entry(tx.dest).or_default().push_back(m);
        };

        // don't send writes to replay-only ingress nodes
        // (they're replay-only because they are beyond the materialization frontier)
        if let Some(replay_to) = replay_to {
            let tx = match replay_to {
                Where::InAll(i) => &mut all_txs[i],
                Where::InReplay(i) => &mut replay_txs[i],
            };
            send(tx, m.take().unwrap());
            return;
        }

        if all_txs.is_empty() {
            // no need to do anything!
            drop(m.take());
            return;
        }

        let txn = all_txs.len() - 1;
        for (txi, ref mut tx) in all_txs.iter_mut().enumerate() {
            // Avoid cloning if this is last send
            let m = if txi == txn {
                m.take().unwrap()
            } else {
                m.as_ref().map(|m| box m.clone_data()).unwrap()
            };

            send(tx, m);
        }
    }
}
