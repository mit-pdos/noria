use fnv::FnvHashMap;
use prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Serialize, Deserialize)]
struct EgressTx {
    node: NodeIndex,
    local: LocalNodeIndex,
    dest: ReplicaAddr,
}

#[derive(Clone, Serialize, Deserialize)]
struct PacketBuffer {
    /// Label of the first packet in the buffer
    min_label: usize,
    /// Packet payloads and lists of to-nodes
    arr: Vec<(Box<Packet>, HashSet<NodeIndex>)>,
}

impl Default for PacketBuffer {
    fn default() -> PacketBuffer {
        PacketBuffer {
            min_label: 1,
            arr: Default::default(),
        }
    }
}

impl PacketBuffer {
    fn max_label(&self) -> usize {
        self.next_label_to_add() - 1
    }

    fn next_label_to_add(&self) -> usize {
        self.min_label + self.arr.len()
    }

    fn get(&self, label: usize) -> Option<&(Box<Packet>, HashSet<NodeIndex>)> {
        let i = label - self.min_label;
        self.arr.get(i)
    }

    fn add_packet(&mut self, packet: Box<Packet>, label: usize, to: NodeIndex) {
        assert_eq!(label, self.next_label_to_add());
        assert_eq!(label, packet.get_id().label());

        let mut to_nodes = HashSet::new();
        to_nodes.insert(to);
        self.arr.push((packet, to_nodes));
    }

    fn add_to_node(&mut self, label: usize, to: NodeIndex) {
        let i = label - self.min_label;
        self.arr.get_mut(i).unwrap().1.insert(to);
    }
}

#[derive(Serialize, Deserialize)]
pub struct Egress {
    txs: Vec<EgressTx>,
    tags: HashMap<Tag, NodeIndex>,

    /// The next packet to send to each child, where the key DNE if waiting for a ResumeAt
    next_packet_to_send: HashMap<NodeIndex, usize>,
    /// The packet buffer
    buffer: PacketBuffer,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
            next_packet_to_send: self.next_packet_to_send.clone(),
            buffer: self.buffer.clone(),
        }
    }
}

impl Default for Egress {
    fn default() -> Self {
        Self {
            tags: Default::default(),
            txs: Default::default(),
            next_packet_to_send: Default::default(),
            buffer: Default::default(),
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
        self.next_packet_to_send.insert(dst_g, 1);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
        to_nodes: &HashSet<NodeIndex>,
    ) {
        let &mut Self {
            ref mut txs,
            ref tags,
            ..
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
            if !to_nodes.contains(&tx.node) {
                continue;
            }

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

            output.entry(tx.dest).or_default().push_back(m);
            if take {
                break;
            }
        }
    }
}

// fault tolerance
impl Egress {
    /// TODO(ygina): a hack
    /// is there state about replay paths somewhere else? like in materialization/plan.rs?
    ///
    /// Replace the old single egress tx with this new tx, also replacing the replay path
    /// the old tx was previously a part of. Makes a lot of assumptions here, like only having
    /// one outgoing connection, or that the paths are the same.
    pub fn replace_tx(&mut self, dst_g: NodeIndex, dst_l: LocalNodeIndex, addr: ReplicaAddr) {
        self.txs.clear();
        self.add_tx(dst_g, dst_l, addr);
        assert_eq!(self.tags.len(), 1);

        // update the replay path
        let tag = self.tags.keys().next().unwrap();
        self.tags.insert(*tag, dst_g);
    }

    /// Stop sending messages to this child.
    pub fn remove_child(&mut self, child: NodeIndex) {
        self.next_packet_to_send.remove(&child);
    }

    /// The label to be assigned to the next outgoing packet.
    pub fn next_label_to_add(&self) -> usize {
        self.buffer.next_label_to_add()
    }

    /// Stores the packet in the buffer and tests whether we should send to each node corresponding
    /// to an egress tx. Returns the nodes we should actually send to. If a node wasn't returned,
    /// we are probably waiting for a ResumeAt message from it.
    ///
    /// Note that it's ok for the next packet to send to be ahead of the packets that have actually
    /// been sent. Either this information is nulled in anticipation of a ResumeAt message, or
    /// it is lost anyway on crash.
    pub fn send_packet(&mut self, m: &Box<Packet>) -> HashSet<NodeIndex> {
        let nodes: Vec<NodeIndex> = self.txs
            .iter()
            .map(|tx| tx.node)
            .collect();

        // println!("SEND PACKET #{} -> {:?}", m.get_id().label(), nodes);
        nodes
            .iter()
            .filter(|&&ni| {
                // push the packet payload and target to-nodes to the buffer
                let label = m.get_id().label();
                if label > self.buffer.max_label() {
                    self.buffer.add_packet(box m.clone_data(), label, ni);
                } else {
                    self.buffer.add_to_node(label, ni);
                }

                // update internal state if we should send the packet
                if let Some(old_label) = self.next_packet_to_send.get(&ni) {
                    // skipped packets from [old_label, label) shouldn't have been sent anyway
                    for i in *old_label..label {
                        assert!(!self.buffer.get(i).unwrap().1.contains(&ni));
                    }

                    self.next_packet_to_send.insert(ni, label + 1);
                    true
                } else {
                    false
                }
            })
            .map(|&ni| ni)
            .collect()
    }

    /// Resume sending messages to this node at the label after getting that node up to date.
    pub fn resume_at(
        &mut self,
        node: NodeIndex,
        label: usize,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let next_label = self.buffer.next_label_to_add();
        let to_nodes = {
            let mut hs = HashSet::new();
            hs.insert(node);
            hs
        };
        for i in label..next_label {
            // println!("RESUME AT #[{}, {}) -> {:?}", label, next_label, to_nodes);
            // TODO(ygina): ignore to nodes, which i think only matter when a packet is sent
            // as part of a replay from a node with multiple children
            // let (m, to_nodes) = &self.buffer[i - 1];
            // if to_nodes.contains(&node) {
            //     packets.push(box m.clone_data());
            // }
            let (m, _) = &self.buffer.get(i).unwrap();
            self.process(
                &mut Some(box m.clone_data()),
                on_shard.unwrap_or(0),
                output,
                &to_nodes,
            );
        }
        self.next_packet_to_send.insert(node, next_label);
    }
}
