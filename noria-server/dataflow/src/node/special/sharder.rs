use fnv::FnvHashMap;
use payload;
use prelude::*;
use std::collections::{HashMap, VecDeque};
use vec_map::VecMap;

#[derive(Serialize, Deserialize)]
pub struct Sharder {
    txs: Vec<(LocalNodeIndex, ReplicaAddr)>,
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,

    pub(crate) updates: Updates,
    payloads: Payloads,

    // Log truncation
    /// All labels associated with an address
    labels: AddrLabels,
    /// The minimum label associated with an address
    min_labels: AddrLabel,

    /// Nodes it's ok to send packets too and the minimum labels (inclusive)
    min_label_to_send: HashMap<ReplicaAddr, usize>,
    /// Target provenances to hit as we're generating new messages
    targets: Vec<Provenance>,
    /// Buffered messages per parent for when we can't hit the next target provenance
    parent_buffer: HashMap<ReplicaAddr, Vec<(usize, Box<Packet>)>>,
}

impl Clone for Sharder {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Sharder {
            txs: Vec::new(),
            sharded: Default::default(),
            shard_by: self.shard_by,
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            labels: self.labels.clone(),
            min_labels: self.min_labels.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            targets: self.targets.clone(),
            parent_buffer: self.parent_buffer.clone(),
        }
    }
}

impl Sharder {
    pub fn new(by: usize) -> Self {
        Self {
            txs: Default::default(),
            shard_by: by,
            sharded: VecMap::default(),
            updates: Default::default(),
            payloads: Default::default(),
            labels: Default::default(),
            min_labels: Default::default(),
            min_label_to_send: Default::default(),
            targets: Default::default(),
            parent_buffer: Default::default(),
        }
    }

    pub fn take(&mut self) -> Self {
        use std::mem;
        let txs = mem::replace(&mut self.txs, Vec::new());
        Self {
            txs,
            sharded: VecMap::default(),
            shard_by: self.shard_by,
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            labels: self.labels.clone(),
            min_labels: self.min_labels.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            targets: self.targets.clone(),
            parent_buffer: self.parent_buffer.clone(),
        }

    }

    pub fn add_sharded_child(&mut self, dst: LocalNodeIndex, txs: Vec<ReplicaAddr>) {
        assert_eq!(self.txs.len(), 0);
        // TODO: add support for "shared" sharder?
        for tx in txs {
            self.min_label_to_send.insert(tx, 1);
            self.txs.push((dst, tx));
        }
    }

    pub fn sharded_by(&self) -> usize {
        self.shard_by
    }

    #[inline]
    fn to_shard(&self, r: &Record) -> usize {
        self.shard(&r[self.shard_by])
    }

    #[inline]
    fn shard(&self, dt: &DataType) -> usize {
        ::shard_by(dt, self.txs.len())
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        label: usize,
        index: LocalNodeIndex,
        is_sharded: bool,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        let mut m = m.take().unwrap();
        let (mtype, is_replay) = match m {
            box Packet::Message { .. } => ("Message", false),
            box Packet::ReplayPiece { .. } => ("ReplayPiece", true),
            box Packet::Dummy { .. } => { return; },
            _ => unreachable!(),
        };

        // we need to shard the records inside `m` by their key,
        for record in m.take_data() {
            let shard = self.to_shard(&record);
            let p = self
                .sharded
                .entry(shard)
                .or_insert_with(|| box m.clone_data());
            p.map_data(|rs| rs.push(record));
        }

        let mut force_all = false;
        if let Packet::ReplayPiece {
            context: payload::ReplayPieceContext::Regular { last: true },
            ..
        } = *m
        {
            // this is the last replay piece for a full replay
            // we need to make sure it gets to every shard so they know to ready the node
            force_all = true;
        }
        if let Packet::ReplayPiece {
            context: payload::ReplayPieceContext::Partial { .. },
            ..
        } = *m
        {
            // we don't know *which* shard asked for a replay of the keys in this batch, so we need
            // to send data to all of them. or for that matter, maybe the replay started below the
            // eventual shard merged! pretty unfortunate. TODO
            force_all = true;
        }
        if force_all {
            for shard in 0..self.txs.len() {
                self.sharded
                    .entry(shard)
                    .or_insert_with(|| box m.clone_data());
            }
        }

        if is_sharded {
            // FIXME: we don't know how many shards in the destination domain our sibling Sharders
            // sent to, so we don't know what to put here. we *could* put self.txs.len() and send
            // empty messages to all other shards, which is probably pretty sensible, but that only
            // solves half the problem. the destination shard domains will then recieve *multiple*
            // replay pieces for each incoming replay piece, and needs to combine them somehow.
            // it's unclear how we do that.
            unimplemented!();
        }

        for (i, &mut (dst, addr)) in self.txs.iter_mut().enumerate() {
            if let Some(mut shard) = self.sharded.remove(i) {
                // Don't send messages that are not at least the min label to send.
                // This happens on recovery when replaying old messages.
                let min_label = *self.min_label_to_send.get(&addr).unwrap();
                if !is_replay && label < min_label {
                    continue;
                }

                shard.link_mut().src = index;
                shard.link_mut().dst = dst;

                println!(
                    "SEND PACKET {} #{} -> D{}.{} {:?}",
                    mtype,
                    label,
                    addr.0.index(),
                    addr.1,
                    shard.id().as_ref().unwrap(),
                );
                output.entry(addr).or_default().push_back(shard);
            }
        }
    }

    pub fn process_eviction(
        &mut self,
        id: Option<ProvenanceUpdate>,
        key_columns: &[usize],
        tag: Tag,
        keys: &[Vec<DataType>],
        src: LocalNodeIndex,
        is_sharded: bool,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        assert!(!is_sharded);

        if key_columns.len() == 1 && key_columns[0] == self.shard_by {
            // Send only to the shards that must evict something.
            for key in keys {
                let shard = self.shard(&key[0]);
                let dst = self.txs[shard].0;
                let p = self
                    .sharded
                    .entry(shard)
                    .or_insert_with(|| box Packet::EvictKeys {
                        id: id.clone(),
                        link: Link { src, dst },
                        keys: Vec::new(),
                        tag,
                    });
                match **p {
                    Packet::EvictKeys { ref mut keys, .. } => keys.push(key.to_vec()),
                    _ => unreachable!(),
                }
            }

            for (i, &mut (_, addr)) in self.txs.iter_mut().enumerate() {
                if let Some(shard) = self.sharded.remove(i) {
                    output.entry(addr).or_default().push_back(shard);
                }
            }
        } else {
            assert_eq!(!key_columns.len(), 0);
            assert!(!key_columns.contains(&self.shard_by));

            // send to all shards
            for &mut (dst, addr) in self.txs.iter_mut() {
                output
                    .entry(addr)
                    .or_default()
                    .push_back(Box::new(Packet::EvictKeys {
                        id: id.clone(),
                        link: Link { src, dst },
                        keys: keys.to_vec(),
                        tag,
                    }))
            }
        }
    }

    fn send_packet_internal(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        index: LocalNodeIndex,
        is_sharded: bool,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> AddrLabel {
        let (mut old, mut new) = self.preprocess_packet(m, from);
        self.process(m, self.updates.max().label(), index, is_sharded, output);

        // Use the changed labels to determine if there is a new minimum label associated
        // with a replica address in this particular replica.
        let mut changed = HashMap::new();
        for (addr, old_labels) in old.drain() {
            // TODO(ygina): do this more efficiently with a min heap
            // Remove labels that were replaced by the update
            let labels = self.labels.get_mut(&addr).unwrap();
            let mut new_labels = new.remove(&addr).expect("old and new have the same keys");
            println!("sharder {:?} {:?} {:?}", labels, old_labels, new_labels);
            for label in old_labels {
                let mut removed = false;
                for i in 0..labels.len() {
                    if labels[i] == label {
                        labels.swap_remove(i);
                        removed = true;
                        break;
                    }
                }
                if !removed {
                    println!("ALERT");
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
}

// fault tolerance
impl Sharder {
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
            unimplemented!();
        } else {
            // Regenerated domains should have the same index
        }
        */
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
        for &(addr, label) in &addr_labels {
            // don't duplicate sent messages
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
        unimplemented!();
    }

    pub fn send_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        index: LocalNodeIndex,
        is_sharded: bool,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) -> AddrLabel {
        // With no targets, all messages are forwarded.
        if self.targets.is_empty() {
            return self.send_packet_internal(m, from, index, is_sharded, output);
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
                                        index,
                                        is_sharded,
                                        output,
                                    );
                                }
                            }
                        }
                        let x = self.send_packet_internal(
                            &mut Some(m),
                            from,
                            index,
                            is_sharded,
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
            self.targets.remove(0);
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
                        index,
                        is_sharded,
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
        from: DomainIndex,
    ) -> (AddrLabels, AddrLabels) {
        // sharders are unsharded
        let from = (from, 0);
        let is_message = match m {
            Some(box Packet::Message { .. }) => true,
            Some(box Packet::ReplayPiece { .. }) => false,
            Some(box Packet::EvictKeys { .. }) => false,
            Some(box Packet::Dummy { .. }) => true,  // because we want to store it in payloads
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
