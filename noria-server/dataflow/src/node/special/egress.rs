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
    pub(crate) min_provenance: Provenance,
    /// Base provenance with all diffs applied
    pub(crate) max_provenance: Provenance,
    /// Provenance updates sent in outgoing packets
    pub(crate) updates: Vec<ProvenanceUpdate>,
    /// Packet payloads
    pub(crate) payloads: Vec<Box<Packet>>,

    /// Nodes it's ok to send packets too and the minimum labels (inclusive)
    min_label_to_send: HashMap<NodeIndex, usize>,
    /// The provenance of the last packet send to each node
    last_provenance: HashMap<NodeIndex, Provenance>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
            min_provenance: self.min_provenance.clone(),
            max_provenance: self.max_provenance.clone(),
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            last_provenance: self.last_provenance.clone(),
        }
    }
}

const PROVENANCE_DEPTH: usize = 3;

impl Egress {
    pub fn init(&mut self, graph: &Graph, ni: NodeIndex) {
        self.min_provenance.init(graph, ni, PROVENANCE_DEPTH);
        self.max_provenance = self.min_provenance.clone();
    }

    // We initially have sent nothing to each node. Diffs are one depth shorter.
    fn insert_default_last_provenance(&mut self, ni: NodeIndex) {
        let mut p = self.min_provenance.clone();
        p.trim(PROVENANCE_DEPTH - 1);
        p.zero();
        self.last_provenance.insert(ni, p);
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
        self.min_label_to_send.insert(dst_g, 1);
        self.insert_default_last_provenance(dst_g);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        msg: Box<Packet>,
        label: usize,
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

            if !to_nodes.contains(&tx.node) {
                continue;
            }

            let mut m = box msg.clone_data();
            let (mtype, is_replay) = match *m {
                Packet::Message { .. } => ("Message", false),
                Packet::ReplayPiece { .. } => ("ReplayPiece", true),
                _ => unreachable!(),
            };

            // forward all replays, but only messages with label at least the minimum label
            let min_label = *self.min_label_to_send.get(&tx.node).unwrap();
            if !is_replay && label < min_label {
                continue;
            }

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

            // set the diff per child right before sending
            let diff = self.last_provenance
                .get(&tx.node)
                .unwrap()
                .diff(&self.max_provenance);
            // TODO(ygina): is this valid if the sender domain just restarted? what if we
            // are sending from an earlier point in time? uphold this assertion later.
            assert_eq!(label, diff.label());
            self.last_provenance.get_mut(&tx.node).unwrap().apply_update(&diff);
            *m.id_mut() = Some(diff);

            println!(
                "SEND PACKET {} #{} -> {} {:?}",
                mtype,
                label,
                tx.node.index(),
                m.id().as_ref().unwrap(),
            );

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

        self.min_label_to_send.remove(&child);
    }

    pub fn remove_tag(&mut self, tag: Tag) {
        self.tags.remove(&tag);
    }

    pub fn new_incoming(&mut self, old: DomainIndex, new: DomainIndex) {
        if self.min_provenance.new_incoming(old, new) {
            /*
            // Remove the old domain from the updates entirely
            for update in self.updates.iter_mut() {
                if update.len() == 0 {
                    panic!(format!(
                        "empty update: {:?}, old: {}, new: {}",
                        self.updates,
                        old.index(),
                        new.index(),
                    ));
                }
                assert_eq!(update[0].0, old);
                update.remove(0);
            }
            */
            unimplemented!();
        } else {
            // Regenerated domains should have the same index
        }
    }

    pub fn get_last_provenance(&self) -> &Provenance {
        &self.max_provenance
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
        let m = m.as_ref().map(|m| box m.clone_data()).unwrap();
        let is_replay = match m {
            box Packet::ReplayPiece { .. } => true,
            _ => false,
        };

        // update packet id to include the correct label, provenance update, and from node.
        // replays don't get buffered and don't increment their label (they use the last label
        // sent by this domain - think of replays as a snapshot of what's already been sent).
        let label = if is_replay {
            self.min_provenance.label() + self.payloads.len()
        } else {
            self.min_provenance.label() + self.payloads.len() + 1
        };
        let update = if let Some(diff) = m.as_ref().id() {
            ProvenanceUpdate::new_with(from, label, &[diff.clone()])
        } else {
            ProvenanceUpdate::new(from, label)
        };
        self.max_provenance.apply_update(&update);
        if !is_replay {
            self.payloads.push(box m.clone_data());
            self.updates.push(self.max_provenance.clone());
        }

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
        self.process(m, self.max_provenance.label(), shard, output, &to_nodes);
    }

    /// Set the minimum label of the provenance, which represents the label of the first
    /// packet payload we have that is not stored, but don't truncate the payload buffer.
    ///
    /// If the label is beyond the label of the payloads we actually have, just set the label
    /// as given and clear the payload buffer. There's no reason the label should be beyond what
    /// we actually have if we have a non-zero number of packets.
    pub fn set_min_label(&mut self, label: usize) {
        assert!(label >= self.min_provenance.label());
        let num_to_truncate = label - self.min_provenance.label();
        if num_to_truncate > self.payloads.len() {
            assert!(self.payloads.is_empty());
            self.payloads = Vec::new();
            // WARNING: setting the label here without setting the rest of the provenance might
            // cause some issues, since the min provenance won't represent the actual provenance
            // of that message label
            self.min_provenance.set_label(label);
        } else {
            // self.payloads.drain(0..num_to_truncate);
        }
    }

    /// Resume sending messages to these children at the given labels.
    pub fn resume_at(
        &mut self,
        node_labels: Vec<(NodeIndex, usize)>,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let mut min_label = std::usize::MAX;
        for &(node, label) in &node_labels {
            // calculate the min label
            if label < min_label {
                min_label = label;
            }
            // don't duplicate sent messages
            self.min_label_to_send.insert(node, label);
        }

        let next_label = self.min_provenance.label() + self.payloads.len() + 1;
        for &(_, label) in &node_labels {
            // we don't have the messages we need to send
            // we must have lost a stateless domain
            if label > next_label {
                println!("{} > {}", label, next_label);
                assert!(self.payloads.is_empty());
                assert!(self.updates.is_empty());
                self.min_provenance.set_label(min_label - 1);
                self.max_provenance.set_label(min_label - 1);
                return;
            }
            // if this is a stateless domain that was just regenerated, then it must not have sent
            // any messages at all. otherwise, it just means no new messages were sent since the
            // connection went down. only return in the first case since other children might not
            // be as up to date.
            if label == next_label && label == 1 {
                println!("{} == {}", label, next_label);
                return;
            }
        }

        // If we made it this far, it means we have all the messages we need to send (assuming
        // log truncation works correctly). Roll back provenance state to the minimum label and
        // replay each message and diff as if they were just received.
        // TODO(ygina): we can probably also just truncate up to min label
        self.max_provenance = self.min_provenance.clone();
        let min_label_index = min_label - self.min_provenance.label() - 1;
        for i in 0..min_label_index {
            let update = &self.updates[i];
            self.max_provenance.apply_update(update);
        }
        for &(node, label) in &node_labels {
            println!("RESUME [#{}, #{}) -> {:?}", label, next_label, node.index());
            self.insert_default_last_provenance(node);
        }

        // Resend all messages from the minimum label.
        for i in min_label_index..self.payloads.len() {
            let update = &self.updates[i];
            let m = box self.payloads[i].clone_data();
            let label = update.label();
            self.max_provenance.apply_update(update);

            // Who would this message normally be sent to?
            let replay_to = m.tag().map(|tag| self.tags.get(&tag).unwrap());
            let to_nodes = if let Some(_) = replay_to {
                // TODO(ygina): may be more selective with sharding
                unreachable!()
            } else {
                self.txs.iter().map(|tx| tx.node).collect::<HashSet<NodeIndex>>()
            };

            self.process(
                m,
                label,
                on_shard.unwrap_or(0),
                output,
                &to_nodes,
            );
        }
    }
}
