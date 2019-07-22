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
    pub(crate) updates: Updates,
    payloads: Payloads,

    // Log truncation
    /// All labels associated with an address
    labels: AddrLabels,
    /// The minimum label associated with an address
    min_labels: AddrLabel,

    /// Replicas that should not be sent to
    do_not_send: HashSet<ReplicaAddr>,
    /// The minimum label that should be sent to each replica (inclusive)
    min_label_to_send: HashMap<ReplicaAddr, usize>,
    /// Target provenances to hit as we're generating new messages
    targets: Vec<Provenance>,
    /// Buffered messages per parent for when we can't hit the next target provenance
    parent_buffer: HashMap<ReplicaAddr, Vec<(usize, Box<Packet>)>>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            labels: self.labels.clone(),
            min_labels: self.min_labels.clone(),
            do_not_send: self.do_not_send.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            targets: self.targets.clone(),
            parent_buffer: self.parent_buffer.clone(),
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
        self.min_label_to_send.insert(addr, 1);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        label: usize,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
        to_addrs: &HashSet<ReplicaAddr>,
    ) {
        let &mut Self {
            ref mut txs,
            ref min_label_to_send,
            ref do_not_send,
            ..
        } = self;

        let (mtype, is_message) = match *m {
            Some(box Packet::Message { .. }) => ("Message", true),
            Some(box Packet::ReplayPiece { .. }) => ("ReplayPiece", false),
            Some(box Packet::EvictKeys { .. }) => ("EvictKeys", false),
            _ => unreachable!(),
        };

        // only send to a node if:
        // 1) it is requested
        // 2) the egress has not been told NOT to send
        // 3) the message has a label at least the min label to send, unless it's a replay
        let to_addrs = to_addrs
            .iter()
            .filter(|addr| {
                let min_label = *min_label_to_send.get(addr).unwrap();
                !(do_not_send.contains(&addr) || (is_message && label < min_label))
            })
            .collect::<Vec<_>>();

        // send any queued updates to all external children
        assert!(!txs.is_empty());
        let mut sends_left = to_addrs.len();
        for (_, ref mut tx) in txs.iter_mut().enumerate() {
            if !to_addrs.contains(&&tx.dest) {
                continue;
            }

            let mut m = if sends_left > 1 {
                box m.as_ref().unwrap().clone_data()
            } else {
                assert_eq!(sends_left, 1);
                m.take().unwrap()
            };
            sends_left -= 1;

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
            m.link_mut().dst = tx.local;

            // println!(
            //     "SEND PACKET {} #{} -> D{}.{} {:?}",
            //     mtype,
            //     label,
            //     tx.dest.0.index(),
            //     tx.dest.1,
            //     m.id().as_ref().unwrap(),
            // );

            // TODO(ygina): don't clone the last send
            output.entry(tx.dest).or_default().push_back(m);
            if to_addrs.len() == 1 {
                break;
            }
        }
    }
}

// fault tolerance
impl Egress {
    /// Stop sending messages to this child.
    pub fn remove_child(&mut self, addr: ReplicaAddr) {
        self.do_not_send.insert(addr);
    }

    pub fn remove_tag(&mut self, tag: Tag) {
        self.tags.remove(&tag);
    }

    pub fn init(&mut self, graph: &DomainGraph, root: ReplicaAddr) {
        self.updates.init(graph, root);
    }

    pub fn init_in_domain(&mut self, shard: usize) {
        self.labels = self.updates.init_in_domain(shard);
        self.min_labels = self
            .labels
            .keys()
            .map(|&addr| (addr, 0))
            .collect();
    }

    pub fn new_incoming(&mut self, old: ReplicaAddr, new: ReplicaAddr) {
        /*
        if self.min_provenance.new_incoming(old, new) {
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
        } else {
            // Regenerated domains should have the same index
        }
        */
    }

    pub fn send_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> AddrLabel {
        // With no targets, all messages are forwarded.
        if self.targets.is_empty() {
            return self.send_packet_internal(m, from, shard, output);
        }

        let next = m.as_ref().unwrap().id().as_ref().expect("message must have id if targets exist");
        // TODO(ygina): can we not clone here?
        self.parent_buffer
            .entry(next.root())
            .or_insert(vec![])
            .push((next.label(), m.as_ref().unwrap().clone()));

        // Whether all parent labels would match the target if the update were applied to the base
        fn would_hit_target(update: &Provenance, target: &Provenance) -> bool {
            assert_eq!(target.edges().len(), 1);
            let p = target.edges().values().next().unwrap();
            return p.root() == update.root() && p.label() == update.label();
        }

        let mut changed = AddrLabel::default();
        fn merge_changed(acc: &mut AddrLabel, x: AddrLabel) {
            for (addr, label) in x.into_iter() {
                acc.insert(addr, label);
            }
        }

        loop {
            // Look in the buffer for any messages we can forward to reach the current target.
            // Forward a packet if it is from a parent noted in the target provenance, and if the
            // packet label is at most the label of the parent in the target provenance.
            let mut hit = false;
            let target_provenance = self.targets.get(0).unwrap().clone();
            for (addr, p) in target_provenance.edges().iter() {
                let max_label = p.label();
                if self.parent_buffer.contains_key(&addr) {
                    while !self.parent_buffer.get(&addr).unwrap().is_empty() {
                        let label = self.parent_buffer.get(&addr).unwrap()[0].0;
                        if label > max_label {
                            break;
                        }
                        let (_, m) = self.parent_buffer.get_mut(&addr).unwrap().remove(0);
                        let update = m.id().as_ref().unwrap();
                        if would_hit_target(update, &target_provenance) {
                            hit = true;
                            // WARNING: Sometimes no child has the provenance of a packet we want
                            // to recover because the packet was lost in the network, even though a
                            // following packet was received by a different child. Sometimes the
                            // provenance is implicit in that its labels are less than the target
                            // provenance's labels, but other times we have no information because
                            // the packet was generated by a separate parent. That's when we reach
                            // this branch. But we don't want to generate ANY packet in case it
                            // gets sent out-of-order to children who have already received
                            // following packets, so we must skip the packet.
                            let target_label = target_provenance.label();
                            let next_label = self.updates.next_label_to_send(true);
                            if next_label < target_label {
                                println!(
                                    "WARNING: increasing next label to send from {} to {}",
                                    next_label,
                                    target_label,
                                );

                                // Insert dummy packets that are not sent to anyone so that packets
                                // can be indexed directly based on their labels.
                                let num_dummy_packets = target_label - next_label;
                                for _ in 0..num_dummy_packets {
                                    self.send_packet_internal(
                                        &mut Some(box Packet::Dummy { id: None }),
                                        from,
                                        shard,
                                        output,
                                    );
                                }
                            }
                        }
                        let x = self.send_packet_internal(
                            &mut Some(m),
                            from,
                            shard,
                            output,
                        );
                        merge_changed(&mut changed, x);
                    }
                }
            }

            // If we did not just send the target, wait for the next packet to arrive.
            if !hit {
                return changed;
            }

            // If we have sent the target, assert that all the non-root labels also match. Then set
            // the target to the next one, and if there is no packet, forward all remaining messages.
            // Otherwise, restart the process.
            assert_eq!(target_provenance.label(), self.updates.max().label());
            for (addr, p) in target_provenance.edges().iter() {
                let label = p.label();
                assert_eq!(label, self.updates.max().edges().get(addr).unwrap().label());
            }
            let target = self.targets.remove(0);
            if self.targets.is_empty() {
                let ms = self.parent_buffer
                    .drain()
                    .flat_map(|(_, buffer)| buffer)
                    .map(|(_, ms)| ms)
                    .collect::<Vec<_>>();
                for m in ms {
                    let x = self.send_packet_internal(
                        &mut Some(m),
                        from,
                        shard,
                        output,
                    );
                    merge_changed(&mut changed, x);
                }
                return changed;
            }
        }
    }

    // Prepare the packet to be sent. Update the packet provenance to be from the domain of _this_
    // egress mode using the packet's existing provenance. Set the label according to the packet
    // type and current packet buffer. Apply this new packet provenance to the domain-wide
    // provenance, and store the update in our provenance history.
    pub fn preprocess_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: ReplicaAddr,
    ) -> (AddrLabels, AddrLabels) {
        let is_message = match m {
            Some(box Packet::Message { .. }) => true,
            Some(box Packet::ReplayPiece { .. }) => false,
            Some(box Packet::EvictKeys { .. }) => false,
            _ => unreachable!(),
        };

        let label = self.updates.next_label_to_send(is_message);

        // Construct the provenance from the provenance of the incoming packet. In most cases
        // we just add the label of the next packet to send of this domain as the root of the
        // new provenance.
        let mut update = if let Some(diff) = m.as_ref().unwrap().id() {
            ProvenanceUpdate::new_with(from, label, &[diff.clone()])
        } else {
            assert!(self.targets.is_empty());
            ProvenanceUpdate::new(from, label)
        };
        let (old, new) = self.updates.add_update(&update);

        // Keep a list of these updates in case a parent domain with multiple parents needs to be
        // reconstructed, but only for messages and not replays. Buffer messages but not replays.
        if is_message {
            update.trim(PROVENANCE_DEPTH - 1);
            *m.as_mut().unwrap().id_mut() = Some(update);
            // buffer
            self.payloads.add_payload(box m.as_ref().unwrap().clone_data());
        } else {
            // TODO(ygina): Replays don't send just the linear path of the message, but the
            // entire provenance. As evidenced below, the root only has one child, which seems
            // insufficient, so I don't think this correctly considers replays.
            update = self.updates.max().clone();
            update.trim(PROVENANCE_DEPTH - 1);
            *m.as_mut().unwrap().id_mut() = Some(update);
        }
        (old, new)
    }
}

impl Egress {
    /// Stores the packet in the buffer and tests whether we should send to each node corresponding
    /// to an egress tx. Returns the nodes we should actually send to. If a node wasn't returned,
    /// we are probably waiting for a ResumeAt message from it.
    ///
    /// Note that it's ok for the next packet to send to be ahead of the packets that have actually
    /// been sent. Either this information is nulled in anticipation of a ResumeAt message, or
    /// it is lost anyway on crash.
    fn send_packet_internal(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        shard: usize,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> AddrLabel {
        let (mut old, mut new) = self.preprocess_packet(m, (from, shard));

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let is_message = match m {
            Some(box Packet::Message { .. }) => true,
            Some(box Packet::ReplayPiece { .. }) => false,
            Some(box Packet::EvictKeys { .. }) => false,
            _ => unreachable!(),
        };
        let send_to = m.as_ref().unwrap().tag().map(|tag| {
            self.tags.get(&tag).unwrap()
        });
        let to_addrs = if let Some(ni) = send_to {
            assert!(!is_message);
            self.txs
                .iter()
                .filter(|tx| tx.node == *ni)
                .map(|tx| tx.dest)
                .collect::<HashSet<_>>()
        } else {
            assert!(is_message);
            self.txs.iter().map(|tx| tx.dest).collect::<HashSet<_>>()
        };

        // finally, send the message
        self.process(m, self.updates.max().label(), shard, output, &to_addrs);

        // Use the changed labels to determine if there is a new minimum label associated
        // with a replica address in this particular replica.
        let mut changed = HashMap::new();
        for (addr, old_labels) in old.drain() {
            // TODO(ygina): do this more efficiently with a min heap
            // Remove labels that were replaced by the update
            let labels = self.labels.get_mut(&addr).unwrap();
            let mut new_labels = new.remove(&addr).expect("old and new have the same keys");
            for label in old_labels {
                let mut removed = false;
                for i in 0..labels.len() {
                    if labels[i] == label {
                        labels.swap_remove(i);
                        removed = true;
                        break;
                    }
                }
                assert!(removed);
            }

            // Replace removed labels with the update

            labels.append(&mut new_labels);
            let min = labels.iter().fold(std::usize::MAX, |mut min, &val| {
                if val < min {
                    min = val;
                }
                min
            });
            if min > *self.min_labels.get(&addr).unwrap() {
                *self.min_labels.get_mut(&addr).unwrap() = min;
                changed.insert(addr, min);
            }
        }
        changed
    }

    /// Resume sending messages to these children at the given labels.
    pub fn resume_at(
        &mut self,
        addr_labels: Vec<(ReplicaAddr, usize)>,
        mut min_provenance: Option<Provenance>,
        targets: Vec<Provenance>,
        on_shard: Option<usize>,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let mut min_label = std::usize::MAX;
        for &(addr, label) in &addr_labels {
            // calculate the min label
            if label < min_label {
                min_label = label;
            }
            // don't duplicate sent messages
            self.do_not_send.remove(&addr);
            self.min_label_to_send.insert(addr, label);
        }

        self.targets = targets;
        let next_label = self.updates.next_label_to_send(true);
        for &(_, label) in &addr_labels {
            // we don't have the messages we need to send
            // we must have lost a stateless domain
            if label > next_label {
                println!("{} > {}", label, next_label);
                self.labels = self.updates.init_after_resume_at(min_provenance.take().unwrap());
                self.min_labels = self
                    .labels
                    .iter()
                    .map(|(addr, labels)| {
                        let min = labels.iter().fold(std::usize::MAX, |mut min, &val| {
                            if val < min {
                                min = val;
                            }
                            min
                        });
                        (*addr, min)
                    })
                    .collect();
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
        // log truncation works correctly).
        assert!(self.targets.is_empty());
        assert!(min_provenance.is_none());
        for &(addr, label) in &addr_labels {
            println!("RESUME [#{}, #{}) -> D{}.{}", label, next_label, addr.0.index(), addr.1);
        }

        // Resend all messages from the minimum label.
        for (i, m) in self.payloads.slice(min_label).into_iter().enumerate() {
            let label = min_label + i;

            // Who would this message normally be sent to?
            let replay_to = m.tag().map(|tag| self.tags.get(&tag).unwrap());
            let to_addrs = if let Some(_) = replay_to {
                // TODO(ygina): may be more selective with sharding
                unreachable!()
            } else {
                self.txs.iter().map(|tx| tx.dest).collect::<HashSet<_>>()
            };

            self.process(
                &mut Some(m),
                label,
                on_shard.unwrap_or(0),
                output,
                &to_addrs,
            );
        }
    }
}
