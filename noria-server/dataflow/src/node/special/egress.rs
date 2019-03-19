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

    /// Provenance of the first packet in payloads
    min_provenance: Provenance,
    /// Provenance updates of depth 1
    updates: Vec<(NodeIndex, usize)>,

    /// Packet payloads
    payloads: Vec<Box<Packet>>,
    /// Label of the first packet per child buffer
    node_min_labels: FnvHashMap<NodeIndex, usize>,
    /// Buffer per child
    node_buffers: FnvHashMap<NodeIndex, Vec<usize>>,

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
            node_min_labels: self.node_min_labels.clone(),
            node_buffers: self.node_buffers.clone(),
            ok_to_send: self.ok_to_send.clone(),
            waiting_for: Default::default(),
            resume_at: None,
        }
    }
}

impl Egress {
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

        if !self.node_min_labels.contains_key(&dst_g) {
            self.node_min_labels.insert(dst_g, 1);
            self.node_buffers.insert(dst_g, vec![]);
            self.ok_to_send.insert(dst_g);
        }
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        index: usize,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
        to_nodes: &FnvHashMap<NodeIndex, usize>,
    ) {
        let &mut Self {
            ref mut txs,
            ..
        } = self;

        // send any queued updates to all external children
        assert!(!txs.is_empty());
        let original_m = &self.payloads[index];

        for (_, ref mut tx) in txs.iter_mut().enumerate() {
            if !self.ok_to_send.contains(&tx.node) {
                continue;
            }

            if !to_nodes.contains_key(&tx.node) {
                continue;
            }

            // calculate and set the label before sending
            let mut m = box original_m.clone_data();
            let label = to_nodes.get(&tx.node).unwrap();

            m.id_mut().as_mut().unwrap().label = *label;
            // println!("SEND PACKET #{} to {}", label, tx.node.index());

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

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
    pub fn remove_child(&mut self, child: NodeIndex, replace_with: Option<Vec<NodeIndex>>) {
        for i in 0..self.txs.len() {
            if self.txs[i].node == child {
                self.txs.swap_remove(i);
                break;
            }
        }
        self.ok_to_send.remove(&child);

        if let Some(replace_with) = replace_with {
            let min_label = *self.node_min_labels.get(&child).unwrap();
            let old_buffer = self.node_buffers.get(&child).unwrap().clone();

            // if the child node were a bottom replica, and the replica had multiple children,
            // then the child node could have only forwarded certain packets in the buffer to
            // certain children. thus when we're reconnecting the graph to skip the failed
            // bottom replica, we need to make sure only certain packets are in the buffers
            // corresponding to each child.
            // TODO(ygina): if min_label != 1 how do we know the packet labels?
            assert_eq!(min_label, 1);
            for ni in replace_with {
                let buffer = old_buffer
                    .iter()
                    .filter(|&&i| {
                        let m = &self.payloads[i];
                        let tag = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
                        match tag {
                            Some(tag_ni) => ni == *tag_ni,
                            None => true,
                        }
                    })
                    .map(|&i| i)
                    .collect();
                self.node_min_labels.insert(ni, min_label);
                self.node_buffers.insert(ni, buffer);
            }

            // clean up buffer of the failed bottom replica
            self.node_min_labels.remove(&child);
            self.node_buffers.remove(&child);
        }
    }

    /// Replace mentions of the old connection with the new connection
    pub fn new_incoming(&mut self, old: NodeIndex, new: NodeIndex) {
        for i in 0..self.updates.len() {
            if self.updates[i].0 == old {
                self.updates[i].0 = new;
            }
        }
    }

    fn get_provenance(&self, index: usize) -> Provenance {
        // TODO(ygina): egress-wide label counters
        let min_label = 1;
        assert!(index >= min_label);
        assert!(index <= self.payloads.len());

        let mut provenance = self.min_provenance.clone();
        for i in (min_label - 1)..index {
            let (node, label) = self.updates[i];
            provenance.insert(node, label);
        }
        provenance
    }

    pub fn get_last_provenance(&self) -> (usize, Provenance) {
        // TODO(ygina): egress-wide label counters
        let max_label = self.payloads.len();
        let provenance = self.get_provenance(max_label);
        (max_label, provenance)
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
        // update packet id to include the correct provenance update and from node. should not
        // contain any label information because that's set by the egress right before sending.
        let mut m = m.as_ref().map(|m| box m.clone_data()).unwrap();
        let update = m.id().map(|pid| (pid.update, pid.label));
        *m.id_mut() = Some(PacketId::new(from, from));

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().tag().map(|tag| self.tags.get(&tag).unwrap());
        let to_nodes = if let Some(ni) = replay_to {
            vec![*ni]
        } else {
            self.txs.iter().map(|tx| tx.node).collect()
        };

        // each message in payload should have a corresponding provenance update
        self.payloads.push(m);
        if let Some(update) = update {
            self.updates.push(update);
        }

        // add to node buffers since it's the first time we're seeing this message
        let index = self.payloads.len() - 1;
        let mut to_nodes_map = FnvHashMap::default();
        for &node in &to_nodes {
            let min_label = *self.node_min_labels.get_mut(&node).unwrap();
            let buffer = self.node_buffers.get_mut(&node).unwrap();
            to_nodes_map.insert(node, min_label + buffer.len());
            buffer.push(index);
        }

        self.process(index, shard, output, &to_nodes_map);
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
    /// Returns the label of the message it needs to send next that is not in the buffer.
    pub fn resume_at(
        &mut self,
        node: NodeIndex,
        label: usize,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> Option<usize> {
        let min_label = *self.node_min_labels.get(&node).unwrap();
        let buffer = self.node_buffers.get(&node).unwrap();
        let next_label = min_label + buffer.len();
        self.ok_to_send.insert(node);

        // we don't have the messages we need to send
        // should only happen if we lost a stateless domain
        if label >= next_label {
            assert!(self.waiting_for.remove(&node));
            assert!(self.payloads.is_empty());
            *self.node_min_labels.get_mut(&node).unwrap() = label;

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
                // TODO(ygina): shouldn't be returning min label but earliest provenance
                return Some(min_label);
            } else {
                return None;
            }
        }

        // send all buffered messages to this node only from the resume at label
        assert!(self.waiting_for.is_empty());
        println!("RESUME [#{}, #{}) -> {:?}", label, next_label, node.index());
        let mut label_index = Vec::new();
        for i in label..next_label {
            label_index.push((i, buffer[i - min_label]));
        }
        let mut to_nodes_map = FnvHashMap::default();
        for (label, index) in label_index {
            to_nodes_map.insert(node, label);
            self.process(index, on_shard.unwrap_or(0), output, &to_nodes_map);
        }
        None
    }
}
