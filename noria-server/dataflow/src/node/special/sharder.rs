use fnv::FnvHashMap;
use hdrhistogram::Histogram;
use payload;
use prelude::*;
use std::collections::VecDeque;
use std::time;
use vec_map::VecMap;

#[derive(Default, Serialize, Deserialize)]
pub struct Sharder {
    txs: Vec<(LocalNodeIndex, ReplicaAddr)>,
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,

    /// Number of shards each message is sharded into by author id
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    shards_hist: Option<Histogram<u64>>,
    /// Size of packet data in bytes
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    data_size_hist: Option<Histogram<u64>>,
    /// Time the sharder started existing
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    start: Option<time::Instant>,
    /// The next time to print data from the histograms
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    check: Option<time::Instant>,
}

impl Clone for Sharder {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Sharder {
            txs: Vec::new(),
            sharded: Default::default(),
            shard_by: self.shard_by,
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
            ..Default::default()
        }
    }

    pub fn add_sharded_child(&mut self, dst: LocalNodeIndex, txs: Vec<ReplicaAddr>) {
        assert_eq!(self.txs.len(), 0);
        // TODO: add support for "shared" sharder?
        for tx in txs {
            self.txs.push((dst, tx));
        }
        // ygina: ugh, just want to put this here so the histogram exists
        self.shards_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.data_size_hist = Some(Histogram::new_with_max(10_000, 5).unwrap());
        self.start = Some(time::Instant::now());
        self.check = Some(time::Instant::now() + CHECK_EVERY);
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


        let is_replay = match m {
            box Packet::ReplayPiece { .. } => true,
            _ => false,
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
                shard.link_mut().src = index;
                shard.link_mut().dst = dst;

                 // Benchmark packet size if it is a message
                if !is_replay {
                    let h = self.data_size_hist.as_mut().unwrap();
                    if h.record(shard.size_of_data()).is_err() {
                        let m = h.high();
                        h.record(m).unwrap();
                    }
                    let total = self.data_size_hist.as_ref().unwrap().len();
                    let now = time::Instant::now();
                    if now > *self.check.as_ref().unwrap() {
                        self.check = Some(self.check.unwrap() + CHECK_EVERY);
                        let dur = now.duration_since(self.start.unwrap()) / 1_000;
                        let total_unsharded = self.shards_hist.as_ref().unwrap().len();
                        println!(
                            "Sent {} unsharded messages in {:?} for {:?} messages / s",
                            total_unsharded,
                            dur,
                            (total_unsharded as u128) / dur.as_millis(),
                        );
                        println!(
                            "Sent {} sharded messages in {:?} for {:?} messages / s",
                            total,
                            dur,
                            (total as u128) / dur.as_millis(),
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

                output.entry(addr).or_default().push_back(shard);
            }
        }
    }

    pub fn process_eviction(
        &mut self,
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
                        link: Link { src, dst },
                        keys: keys.to_vec(),
                        tag,
                    }))
            }
        }
    }
}
