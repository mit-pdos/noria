use fnv::FnvHashMap;
use prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

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

    /// Base provenance, including the label it represents
    min_provenance: Provenance,
    /// Provenance updates of depth 1 starting with the first packet in payloads
    updates: Vec<ProvenanceUpdate>,
    /// Packet payloads
    payloads: Vec<Box<Packet>>,

    /// Nodes it's ok to send packets too and the minimum labels (inclusive)
    min_label_to_send: HashMap<NodeIndex, usize>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
            min_provenance: self.min_provenance.clone(),
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
        }
    }
}

const PROVENANCE_DEPTH: usize = 3;

impl Egress {
    pub fn init(&mut self, graph: &Graph, ni: NodeIndex) {
        self.min_provenance.init(graph, ni, PROVENANCE_DEPTH);
    }

    pub fn add_tx(&mut self, dst_g: NodeIndex, dst_l: LocalNodeIndex, addr: ReplicaAddr) {
        // avoid adding duplicate egress txs. this happens because we send Update Egress messages
        // both when reconnecting a replicated stateless domain, and so the domain gets the correct
        // tags for each tx. TODO(ygina): make this less hacky
        for tx in &self.txs {
            if tx.node == dst_g {
                return;
            }
        }

        self.txs.push(EgressTx {
            node: dst_g,
            local: dst_l,
            dest: addr,
        });
        self.min_label_to_send.insert(dst_g, 0);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        msg: Box<Packet>,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
        to_nodes: &HashSet<NodeIndex>,
    ) {
        let &mut Self {
            ref mut txs,
            ..
        } = self;

        // send any queued updates to all external children
        assert!(!txs.is_empty());
        for (_, ref mut tx) in txs.iter_mut().enumerate() {
            if !self.min_label_to_send.contains_key(&tx.node) {
                continue;
            }

            let label = msg.id().as_ref().unwrap().label;
            if label < *self.min_label_to_send.get(&tx.node).unwrap() {
                continue;
            }

            if !to_nodes.contains(&tx.node) {
                continue;
            }

            // calculate and set the label before sending
            let mut m = box msg.clone_data();
            let mtype = match *m {
                Packet::Message { .. } => "Message",
                Packet::ReplayPiece { .. } => "ReplayPiece",
                _ => unreachable!(),
            };
            println!(
                "SEND PACKET {} #{} -> {} {:?}",
                mtype,
                m.id().as_ref().unwrap().label,
                tx.node.index(),
                m.id().as_ref().unwrap().update,
            );

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

            // TODO(ygina): don't clone the last send
            output.entry(tx.dest).or_default().push_back(m);
            if to_nodes.len() == 1 {
                break;
            }
        }
    }
}

// fault tolerance
impl Egress {
    /// Stop sending messages to this child.
    pub fn remove_child(&mut self, child: NodeIndex) {
        for i in 0..self.txs.len() {
            if self.txs[i].node == child {
                self.txs.swap_remove(i);
                break;
            }
        }

        // Remove the tags associated with this child
        let mut tags_to_remove = vec![];
        for (tag, ni) in &self.tags {
            if *ni == child {
                tags_to_remove.push(*tag);
            }
        }
        for tag in &tags_to_remove {
            self.tags.remove(tag);
        }

        self.min_label_to_send.remove(&child);
    }

    pub fn new_incoming(&mut self, old: DomainIndex, new: DomainIndex) {
        if self.min_provenance.new_incoming(old, new) {
            // Remove the old domain from the updates entirely
            for update in self.updates.iter_mut() {
                assert_eq!(update[0].0, old);
                update.remove(0);
            }
        } else {
            // Replace the old domain with the new domain in all updates
            for update in self.updates.iter_mut() {
                assert_eq!(update[0].0, old);
                update[0].0 = new;
            }
        }
    }

    pub fn get_last_provenance(&self) -> Provenance {
        let mut provenance = self.min_provenance.clone();
        provenance.apply_updates(&self.updates[..]);
        provenance
    }

    /// Stores the packet in the buffer and tests whether we should send to each node corresponding
    /// to an egress tx. Returns the nodes we should actually send to. If a node wasn't returned,
    /// we are probably waiting for a ResumeAt message from it.
    ///
    /// Note that it's ok for the next packet to send to be ahead of the packets that have actually
    /// been sent. Either this information is nulled in anticipation of a ResumeAt message, or
    /// it is lost anyway on crash.
    pub fn send_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let mut m = m.as_ref().map(|m| box m.clone_data()).unwrap();
        let is_replay = match m {
            box Packet::ReplayPiece { .. } => true,
            _ => false,
        };

        // update packet id to include the correct label, provenance update, and from node.
        // replays don't get buffered and don't increment their label (they use the last label
        // sent by this domain - think of replays as a snapshot of what's already been sent).
        let mut update = Vec::new();
        if let Some(ref pid) = m.as_ref().id() {
            // TODO(ygina): trim this if necessary
            // TODO(ygina): could probably more efficiently handle message cloning
            update.push((pid.from, pid.label));
            update.append(&mut pid.update.clone());
        }
        let label = if is_replay {
            self.min_provenance.label() + self.payloads.len()
        } else {
            self.min_provenance.label() + self.payloads.len() + 1
        };
        *m.id_mut() = Some(PacketId::new(label, from, update.clone()));
        if !is_replay {
            self.payloads.push(box m.clone_data());
        }
        self.updates.push(update);

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
        let to_nodes = if let Some(ni) = replay_to {
            assert!(is_replay);
            let mut set = HashSet::new();
            set.insert(*ni);
            set
        } else {
            assert!(!is_replay);
            self.txs.iter().map(|tx| tx.node).collect::<HashSet<NodeIndex>>()
        };

        // finally, send the message
        self.process(m, shard, output, &to_nodes);
    }

    pub fn set_min_label(&mut self, label: usize) {
        self.min_provenance.set_label(label);
    }

    /// Resume sending messages to this node at the label after getting that node up to date.
    ///
    /// If we don't have the appropriate messages buffered, that means we lost a stateless domain.
    /// Simply mark down where to resume sending messages to the child node for later.
    pub fn resume_at(
        &mut self,
        node: NodeIndex,
        label: usize,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let next_label = self.min_provenance.label() + self.payloads.len() + 1;
        // if label is 1, then just resume at 0
        if label == 1 {
            self.min_label_to_send.insert(node, 0);
        } else {
            self.min_label_to_send.insert(node, label);
        }

        // we don't have the messages we need to send
        // should only happen if we lost a stateless domain
        if label >= next_label {
            println!("{} >= {}", label, next_label);
            assert!(self.payloads.is_empty());
            return;
        }

        // send all buffered messages to this node only from the resume at label
        println!("RESUME [#{}, #{}) -> {:?}", label, next_label, node.index());
        let mut to_nodes = HashSet::new();
        to_nodes.insert(node);
        for m_label in label..next_label {
            let m = &self.payloads[m_label - self.min_provenance.label() - 1];
            let replay_to = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
            if let Some(ni) = replay_to {
                if node != *ni {
                    continue;
                }
            }
            self.process(box m.clone_data(), on_shard.unwrap_or(0), output, &to_nodes);
        }
    }
}
