use fnv::FnvHashMap;
use hdrhistogram::Histogram;
use payload;
use prelude::*;
use std::time;
use std::collections::{HashMap, VecDeque};
use vec_map::VecMap;

#[derive(Serialize, Deserialize)]
pub struct Sharder {
    txs: Vec<(LocalNodeIndex, ReplicaAddr)>,
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,

    /// Base provenance, including the label it represents
    pub(crate) min_provenance: Provenance,
    /// Base provenance with all diffs applied
    pub(crate) max_provenance: Provenance,
    /// Provenance updates sent in outgoing packets
    pub(crate) updates: Vec<ProvenanceUpdate>,
    /// Packet payloads
    pub(crate) payloads: Vec<Box<Packet>>,

    /// Nodes it's ok to send packets too and the minimum labels (inclusive)
    min_label_to_send: HashMap<ReplicaAddr, usize>,
    /// The provenance of the last packet send to each node
    last_provenance: HashMap<ReplicaAddr, Provenance>,
    /// Target provenances to hit as we're generating new messages
    targets: Vec<Provenance>,
    /// Buffered messages per parent for when we can't hit the next target provenance
    parent_buffer: HashMap<ReplicaAddr, Vec<(usize, Box<Packet>)>>,

    /// Number of shards each message is sharded into by author id
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    shards_hist: Option<Histogram<u64>>,
    /// Size of packet id in bytes
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    id_size_hist: Option<Histogram<u64>>,
    /// Size of packet data in bytes
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    data_size_hist: Option<Histogram<u64>>,
    /// Time the sharder started existing
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    start: Option<time::Instant>,
}

impl Default for Sharder {
    fn default() -> Sharder {
        Sharder {
            txs: Default::default(),
            sharded: Default::default(),
            shard_by: Default::default(),
            min_provenance: Default::default(),
            max_provenance: Default::default(),
            updates: Default::default(),
            payloads: Default::default(),
            min_label_to_send: Default::default(),
            last_provenance: Default::default(),
            targets: Default::default(),
            parent_buffer: Default::default(),
            shards_hist: None,
            id_size_hist: None,
            data_size_hist: None,
            start: None,
        }
    }
}

impl Clone for Sharder {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Sharder {
            txs: Vec::new(),
            sharded: Default::default(),
            shard_by: self.shard_by,
            min_provenance: self.min_provenance.clone(),
            max_provenance: self.max_provenance.clone(),
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            last_provenance: self.last_provenance.clone(),
            targets: self.targets.clone(),
            parent_buffer: self.parent_buffer.clone(),
            ..Default::default()
        }
    }
}

const CHECK_EVERY: u64 = 500_000;

impl Sharder {
    pub fn new(by: usize) -> Self {
        Self {
            txs: Default::default(),
            shard_by: by,
            sharded: VecMap::default(),
            min_provenance: Default::default(),
            max_provenance: Default::default(),
            updates: Default::default(),
            payloads: Default::default(),
            min_label_to_send: Default::default(),
            last_provenance: Default::default(),
            targets: Default::default(),
            parent_buffer: Default::default(),
            ..Default::default()
        }
    }

    pub fn take(&mut self) -> Self {
        use std::mem;
        let txs = mem::replace(&mut self.txs, Vec::new());
        Self {
            txs,
            sharded: VecMap::default(),
            shard_by: self.shard_by,
            min_provenance: self.min_provenance.clone(),
            max_provenance: self.max_provenance.clone(),
            updates: self.updates.clone(),
            payloads: self.payloads.clone(),
            min_label_to_send: self.min_label_to_send.clone(),
            last_provenance: self.last_provenance.clone(),
            targets: self.targets.clone(),
            parent_buffer: self.parent_buffer.clone(),
            ..Default::default()
        }

    }

    pub fn add_sharded_child(&mut self, dst: LocalNodeIndex, txs: Vec<ReplicaAddr>) {
        assert_eq!(self.txs.len(), 0);
        // TODO: add support for "shared" sharder?
        for tx in txs {
            self.min_label_to_send.insert(tx, 1);
            self.insert_default_last_provenance(tx);
            self.txs.push((dst, tx));
        }
        // ygina: ugh, just want to put this here so the histogram exists
        self.shards_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.id_size_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.data_size_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.start = Some(time::Instant::now());
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
        // we need to shard the records inside `m` by their key,
        let mut m = m.take().unwrap();
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

        let (mtype, is_replay) = match m {
            box Packet::Message { .. } => ("Message", false),
            box Packet::ReplayPiece { .. } => ("ReplayPiece", true),
            _ => unreachable!(),
        };

        if !is_replay {
            let h = self.shards_hist.as_mut().unwrap();
            if h.record(self.sharded.len() as u64).is_err() {
                let m = h.high();
                h.record(m).unwrap();
            }
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

                // Benchmark packet size if it is a message
                if !is_replay {
                    let h = self.id_size_hist.as_mut().unwrap();
                    if h.record(shard.size_of_id()).is_err() {
                        let m = h.high();
                        h.record(m).unwrap();
                    }
                    let h = self.data_size_hist.as_mut().unwrap();
                    if h.record(shard.size_of_data()).is_err() {
                        let m = h.high();
                        h.record(m).unwrap();
                    }
                    let total = self.id_size_hist.as_ref().unwrap().len();
                    if total % CHECK_EVERY == 0 {
                        let now = time::Instant::now();
                        let dur = now.duration_since(self.start.unwrap()) / 1_000;
                        let total_unsharded = self.shards_hist.as_ref().unwrap().len();
                        println!(
                            "Sent {} unsharded messages in {:?} for {:?} messages / ms",
                            total_unsharded,
                            dur,
                            (total_unsharded as u128) / dur.as_millis(),
                        );
                        println!(
                            "Sent {} sharded messages in {:?} for {:?} messages / ms",
                            total,
                            dur,
                            (total as u128) / dur.as_millis(),
                        );
                        println!(
                            "Size of packet id: [{}, {}, {}, {}]",
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.5),
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.95),
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.99),
                            self.id_size_hist.as_ref().unwrap().max(),
                        );
                        println!(
                            "Size of packet data: [{}, {}, {}, {}]",
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.5),
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.95),
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.99),
                            self.data_size_hist.as_ref().unwrap().max(),
                        );
                        println!(
                            "Num shards by author_id: [{}, {}, {}, {}]",
                            self.shards_hist.as_ref().unwrap().value_at_quantile(0.5),
                            self.shards_hist.as_ref().unwrap().value_at_quantile(0.95),
                            self.shards_hist.as_ref().unwrap().value_at_quantile(0.99),
                            self.shards_hist.as_ref().unwrap().max(),
                        );
                    }
                }

                // println!(
                //     "SEND PACKET {} #{} -> D{}.{} {:?}",
                //     mtype,
                //     label,
                //     addr.0.index(),
                //     addr.1,
                //     shard.id().as_ref().unwrap(),
                // );
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
    ) {
        self.preprocess_packet(m, from);
        self.process(m, self.max_provenance.label(), index, is_sharded, output);
    }
}

const PROVENANCE_DEPTH: usize = 3;

// fault tolerance
impl Sharder {
    // We initially have sent nothing to each node. Diffs are one depth shorter.
    fn insert_default_last_provenance(&mut self, addr: ReplicaAddr) {
        let mut p = self.min_provenance.clone();
        p.trim(PROVENANCE_DEPTH - 1);
        p.zero();
        self.last_provenance.insert(addr, p);
    }

    pub fn init(&mut self, graph: &DomainGraph, root: ReplicaAddr) {
        for ni in graph.node_indices() {
            if graph[ni] == root {
                self.min_provenance.init(graph, root, ni, PROVENANCE_DEPTH);
                return;
            }
        }
        unreachable!();
    }

    pub fn init_in_domain(&mut self, shard: usize) {
        self.min_provenance.set_shard(shard);
        self.max_provenance = self.min_provenance.clone();
    }

    pub fn new_incoming(&mut self, old: ReplicaAddr, new: ReplicaAddr) {
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

    /// Resume sending messages to these children at the given labels.
    pub fn resume_at(
        &mut self,
        addr_labels: Vec<(ReplicaAddr, usize)>,
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
            self.min_label_to_send.insert(addr, label);
        }

        self.targets = targets;
        let next_label = self.min_provenance.label() + self.payloads.len() + 1;
        for &(_, label) in &addr_labels {
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
        unimplemented!();
    }

    pub fn send_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        index: LocalNodeIndex,
        is_sharded: bool,
        output: &mut FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,
    ) {
        // With no targets, all messages are forwarded.
        if self.targets.is_empty() {
            self.send_packet_internal(m, from, index, is_sharded, output);
            return;
        }

        let next = m.as_ref().unwrap().id().as_ref().expect("message must have id if targets exist");
        // TODO(ygina): can we not clone here?
        self.parent_buffer
            .entry(next.root())
            .or_insert(vec![])
            .push((next.label(), m.as_ref().unwrap().clone()));

        loop {
            // Look in the buffer for any messages we can forward to reach the current target.
            // Forward a packet if it is from a parent noted in the target provenance, and if the
            // packet label is at most the label of the parent in the target provenance.
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
                        self.send_packet_internal(&mut Some(m), from, index, is_sharded, output);
                        println!("SHARDER: sent {:?}", self.max_provenance);
                    }
                }
            }

            // If we did not just send the target, wait for the next packet to arrive.
            if self.max_provenance.label() < target_provenance.label() {
                return;
            }

            // If we have sent the target, assert that all the non-root labels also match. Then set
            // the target to the next one, and if there is no packet, forward all remaining messages.
            // Otherwise, restart the process.
            assert_eq!(target_provenance.label(), self.max_provenance.label());
            for (addr, p) in target_provenance.edges().iter() {
                let label = p.label();
                assert_eq!(label, self.max_provenance.edges().get(addr).unwrap().label());
            }
            let target = self.targets.remove(0);
            println!("SHARDER: sent {:?}, hit target {:?}", self.max_provenance, target);
            if self.targets.is_empty() {
                let ms = self.parent_buffer
                    .drain()
                    .flat_map(|(_, buffer)| buffer)
                    .map(|(_, ms)| ms)
                    .collect::<Vec<_>>();
                println!("targets done! draining {} messages in buffer", ms.len());
                for m in ms {
                    self.send_packet_internal(&mut Some(m), from, index, is_sharded, output);
                }
                println!("done draining... {:?} resume normal op", self.max_provenance);
                return;
            }
        }
    }

    // Prepare the packet to be sent. Update the packet provenance to be from the domain of _this_
    // egress mode using the packet's existing provenance. Set the label according to the packet
    // type and current packet buffer. Apply this new packet provenance to the domain-wide
    // provenance, and store the update in our provenance history.
    pub fn preprocess_packet(&mut self, m: &mut Option<Box<Packet>>, from: DomainIndex) {
        // sharders are unsharded
        let from = (from, 0);
        let is_message = match m {
            Some(box Packet::Message { .. }) => true,
            Some(box Packet::ReplayPiece { .. }) => false,
            Some(box Packet::EvictKeys { .. }) => false,
            _ => unreachable!(),
        };

        // replays don't get buffered and don't increment their label (they use the last label
        // sent by this domain - think of replays as a snapshot of what's already been sent).
        let label = if is_message {
            self.min_provenance.label() + self.payloads.len() + 1
        } else {
            self.min_provenance.label() + self.payloads.len()
        };

        // Construct the provenance from the provenance of the incoming packet. In most cases
        // we just add the label of the next packet to send of this domain as the root of the
        // new provenance.
        let mut update = if let Some(diff) = m.as_ref().unwrap().id() {
            ProvenanceUpdate::new_with(from, label, &[diff.clone()])
        } else {
            assert!(self.targets.is_empty());
            ProvenanceUpdate::new(from, label)
        };
        self.max_provenance.apply_update(&update);

        // Keep a list of these updates in case a parent domain with multiple parents needs to be
        // reconstructed, but only for messages and not replays. Buffer messages but not replays.
        if is_message {
            // TODO(ygina): Might want to trim more efficiently with sharding, especially if we
            // know it doesn't have to be trimmed.
            self.updates.push(update.clone());
            update.trim(PROVENANCE_DEPTH - 1);
            *m.as_mut().unwrap().id_mut() = Some(update);
            // buffer
            self.payloads.push(box m.as_ref().unwrap().clone_data());
        } else {
            // TODO(ygina): Replays don't send just the linear path of the message, but the
            // entire provenance. As evidenced below, the root only has one child, which seems
            // insufficient, so I don't think this correctly considers replays.
            update = self.max_provenance.clone();
            update.trim(PROVENANCE_DEPTH - 1);
            *m.as_mut().unwrap().id_mut() = Some(update);
        }
    }
}
