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

    /// Whitelist of nodes it's ok to send packets too
    ok_to_send: HashSet<NodeIndex>,
    /// The set of nodes it is waiting to hear from for recovery
    waiting_for: HashSet<NodeIndex>,
    /// Where to resume, if in recovery mode
    resume_at: Option<usize>,
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
            ok_to_send: self.ok_to_send.clone(),
            waiting_for: Default::default(),
            resume_at: None,
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
        self.ok_to_send.insert(dst_g);
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
            if !self.ok_to_send.contains(&tx.node) {
                continue;
            }

            if !to_nodes.contains(&tx.node) {
                continue;
            }

            // calculate and set the label before sending
            let mut m = box msg.clone_data();
            println!("SEND PACKET #{} -> {}", m.id().as_ref().unwrap().label, tx.node.index());

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
        self.ok_to_send.remove(&child);
    }

    /// Replace mentions of the old connection with the new connection
    pub fn new_incoming(&mut self, old: NodeIndex, new: NodeIndex) {
        for ref mut update in self.updates.iter_mut() {
            for ref mut node_label in update.iter_mut() {
                if (*node_label).0 == old {
                    (*node_label).0 = new;
                }
            }
        }
    }

    fn get_provenance(&self, label: usize) -> Provenance {
        // TODO(ygina): egress-wide label counters
        let min_label = self.min_provenance.label();
        assert!(label >= min_label);
        assert!(label <= self.updates.len());

        let mut provenance = self.min_provenance.clone();
        provenance.apply_updates(&self.updates[min_label..label].to_vec());
        provenance
    }

    pub fn get_last_provenance(&self) -> Provenance {
        let max_label = self.min_provenance.label() + self.updates.len();
        let provenance = self.get_provenance(max_label);
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
        from: NodeIndex,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        // update packet id to include the correct label, provenance update, and from node.
        let mut m = m.as_ref().map(|m| box m.clone_data()).unwrap();
        let label = self.min_provenance.label() + self.payloads.len() + 1;
        let mut update = Vec::new();
        if let Some(ref pid) = m.as_ref().id() {
            // TODO(ygina): trim this if necessary
            // TODO(ygina): could probably more efficiently handle message cloning
            update.push((pid.from, pid.label));
            update.append(&mut pid.update.clone());
        }
        *m.id_mut() = Some(PacketId::new(label, from, update.clone()));

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
        let to_nodes = if let Some(ni) = replay_to {
            let mut set = HashSet::new();
            set.insert(*ni);
            set
        } else {
            self.txs.iter().map(|tx| tx.node).collect::<HashSet<NodeIndex>>()
        };

        // each message in payload should have a corresponding provenance update
        // (unless the provenance doesn't reach back that far)
        // for example, the root domain doesn't have any provenance history since all messages
        // are derived from base tables.
        self.payloads.push(m);
        if update.is_empty() {
            assert!(self.updates.is_empty());
        } else {
            self.updates.push(update);
        }

        // finally, send the message
        let m = &self.payloads[label - self.min_provenance.label() - 1];
        self.process(box m.clone_data(), shard, output, &to_nodes);
    }

    /// Enter recovery mode, in which we are waiting to process one ResumeAt message for each
    /// child. That's one message per egress tx. Does nothing if we are already in recovery mode.
    /// The egress node will exit recovery mode once it has processed the last ResumeAt.
    pub fn wait_for_resume_at(&mut self) {
        if self.waiting_for.len() == 0 {
            self.waiting_for = self.txs.iter().map(|tx| tx.node).collect();
            assert!(self.waiting_for.len() > 0);
            assert!(self.resume_at.is_none());
        }
    }

    /// Resume sending messages to this node at the label after getting that node up to date.
    ///
    /// Returns whether the node that called this method should propagate a secondary resume at
    /// to its ancestors. The point to resume at is constructed from the node's own provenance
    /// graph. TODO(ygina): the controller should be in charge of when to call the secondary
    /// resume at.
    pub fn resume_at(
        &mut self,
        node: NodeIndex,
        label: usize,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> bool {
        let next_label = self.min_provenance.label() + self.payloads.len() + 1;
        self.ok_to_send.insert(node);

        // we don't have the messages we need to send
        // should only happen if we lost a stateless domain
        if label >= next_label {
            println!("{} >= {}", label, next_label);
            assert!(self.waiting_for.remove(&node));
            assert!(self.payloads.is_empty());
            self.resume_at = match self.resume_at {
                // TODO(ygina): internal mapping from node-local label to provenance
                // otherwise this is totally incorrect!
                Some(old_label) => {
                    if label < old_label {
                        Some(label)
                    } else {
                        Some(old_label)
                    }
                },
                None => Some(label),
            };

            // if we are not waiting for any more nodes, return the label for the parent to
            // resume at to make the dataflow graph complete
            if self.waiting_for.len() == 0 {
                let min_label = self.resume_at.take().unwrap();
                self.min_provenance.set_label(min_label - 1);
                return true;
            } else {
                return false;
            }
        }

        // send all buffered messages to this node only from the resume at label
        assert!(self.waiting_for.is_empty());
        println!("RESUME [#{}, #{}) -> {:?}", label, next_label, node.index());
        let mut to_nodes = HashSet::new();
        to_nodes.insert(node);
        for m_label in label..next_label {
            let m = &self.payloads[m_label - self.min_provenance.label()];
            let replay_to = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
            if let Some(ni) = replay_to {
                if node != *ni {
                    continue;
                }
            }
            self.process(box m.clone_data(), on_shard.unwrap_or(0), output, &to_nodes);
        }
        false
    }
}
