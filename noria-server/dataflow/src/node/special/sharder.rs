use fnv::FnvHashMap;
use hdrhistogram::Histogram;
use payload;
use prelude::*;
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
            shards_hist: None,
            id_size_hist: None,
            data_size_hist: None,
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
            ..Default::default()
        }
    }
}

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
            self.shards_hist.as_mut().unwrap().record(self.sharded.len() as u64).unwrap();
            if self.shards_hist.as_ref().unwrap().len() == 100000 {
                println!(
                    "Num shards by author_id: [{}, {}, {}, {}]",
                    self.shards_hist.as_ref().unwrap().value_at_quantile(0.5),
                    self.shards_hist.as_ref().unwrap().value_at_quantile(0.95),
                    self.shards_hist.as_ref().unwrap().value_at_quantile(0.99),
                    self.shards_hist.as_ref().unwrap().max(),
                );
            }
        }

        for (i, &mut (dst, addr)) in self.txs.iter_mut().enumerate() {
            if let Some(mut shard) = self.sharded.remove(i) {
                shard.link_mut().src = index;
                shard.link_mut().dst = dst;

                // set the diff per child right before sending
                let diff = self.last_provenance
                    .get(&addr)
                    .unwrap()
                    .diff(&self.max_provenance);
                assert_eq!(label, diff.label());
                self.last_provenance.get_mut(&addr).unwrap().apply_update(&diff);
                *shard.id_mut() = Some(diff);

                // Benchmark packet size if it is a message
                if !is_replay {
                    self.id_size_hist.as_mut().unwrap().record(shard.size_of_id()).unwrap();
                    self.data_size_hist.as_mut().unwrap().record(shard.size_of_data()).unwrap();
                    if self.id_size_hist.as_ref().unwrap().len() == 400000 {
                        println!(
                            "Size of packet id: [{}, {}, {}, {}]",
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.5),
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.95),
                            self.id_size_hist.as_ref().unwrap().value_at_quantile(0.99),
                            self.id_size_hist.as_ref().unwrap().max(),
                        );
                    }
                    if self.data_size_hist.as_ref().unwrap().len() == 400000 {
                        println!(
                            "Size of packet data: [{}, {}, {}, {}]",
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.5),
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.95),
                            self.data_size_hist.as_ref().unwrap().value_at_quantile(0.99),
                            self.data_size_hist.as_ref().unwrap().max(),
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

    pub fn send_packet(
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
    fn insert_default_last_provenance(&mut self, tx: ReplicaAddr) {
        let mut p = self.min_provenance.clone();
        p.trim(PROVENANCE_DEPTH - 1);
        p.zero();
        self.last_provenance.insert(tx, p);
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

    pub fn preprocess_packet(&mut self, m: &mut Option<Box<Packet>>, from: DomainIndex) {
        // sharders are unsharded
        let from = (from, 0);
        let is_replay = match m {
            Some(box Packet::ReplayPiece { .. }) => true,
            Some(box Packet::Message { .. }) => false,
            _ => unreachable!(),
        };

        // update packet id to include the correct label, provenance update, and from node.
        // replays don't get buffered and don't increment their label (they use the last label
        // sent by this domain - think of replays as a snapshot of what's already been sent).
        let label = if is_replay {
            self.min_provenance.label() + self.payloads.len()
        } else {
            self.min_provenance.label() + self.payloads.len() + 1
        };
        let update = if let Some(diff) = m.as_ref().unwrap().id() {
            ProvenanceUpdate::new_with(from, label, &[diff.clone()])
        } else {
            ProvenanceUpdate::new(from, label)
        };
        self.max_provenance.apply_update(&update);
        if !is_replay {
            self.payloads.push(box m.as_ref().unwrap().clone_data());
            self.updates.push(self.max_provenance.clone());
        }
    }
}
