use backlog;
use noria::channel;
use prelude::*;

/// A StreamUpdate reflects the addition or deletion of a row from a reader node.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamUpdate {
    /// Indicates the addition of a new row
    AddRow(Vec<DataType>),
    /// Indicates the removal of an existing row
    DeleteRow(Vec<DataType>),
}

impl From<Record> for StreamUpdate {
    fn from(other: Record) -> Self {
        match other {
            Record::Positive(u) => StreamUpdate::AddRow(u),
            Record::Negative(u) => StreamUpdate::DeleteRow(u),
        }
    }
}

impl From<Vec<DataType>> for StreamUpdate {
    fn from(other: Vec<DataType>) -> Self {
        StreamUpdate::AddRow(other)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Reader {
    #[serde(skip)]
    writer: Option<backlog::WriteHandle>,

    #[serde(skip)]
    streamers: Vec<channel::StreamSender<Vec<StreamUpdate>>>,

    for_node: NodeIndex,
    state: Option<Vec<usize>>,

    /// Base provenance, including the label it represents
    pub(crate) min_provenance: Provenance,
    /// Base provenance with all diffs applied
    pub(crate) max_provenance: Provenance,
    /// Provenance updates sent in outgoing packets
    /// We don't have to store payloads in readers because there are no outgoing packets.
    pub(crate) updates: Vec<ProvenanceUpdate>,
    /// Number of non-replay messages received.
    pub(crate) num_payloads: usize,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        assert!(self.writer.is_none());
        Reader {
            writer: None,
            streamers: self.streamers.clone(),
            state: self.state.clone(),
            for_node: self.for_node,
            min_provenance: self.min_provenance.clone(),
            max_provenance: self.max_provenance.clone(),
            updates: self.updates.clone(),
            num_payloads: self.num_payloads,
        }
    }
}

const PROVENANCE_DEPTH: usize = 3;

impl Reader {
    pub fn new(for_node: NodeIndex) -> Self {
        Reader {
            writer: None,
            streamers: Vec::new(),
            state: None,
            for_node,
            min_provenance: Default::default(),
            max_provenance: Default::default(),
            updates: Default::default(),
            num_payloads: 0,
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    #[allow(dead_code)]
    fn writer(&self) -> Option<&backlog::WriteHandle> {
        self.writer.as_ref()
    }

    crate fn writer_mut(&mut self) -> Option<&mut backlog::WriteHandle> {
        self.writer.as_mut()
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        use std::mem;
        Self {
            writer: self.writer.take(),
            streamers: mem::replace(&mut self.streamers, Vec::new()),
            state: self.state.clone(),
            for_node: self.for_node,
            min_provenance: self.min_provenance.clone(),
            max_provenance: self.max_provenance.clone(),
            updates: self.updates.clone(),
            num_payloads: self.num_payloads,
        }
    }

    crate fn add_streamer(
        &mut self,
        new_streamer: channel::StreamSender<Vec<StreamUpdate>>,
    ) -> Result<(), channel::StreamSender<Vec<StreamUpdate>>> {
        self.streamers.push(new_streamer);
        Ok(())
    }

    pub fn is_materialized(&self) -> bool {
        self.state.is_some()
    }

    crate fn is_partial(&self) -> bool {
        match self.writer {
            None => false,
            Some(ref state) => state.is_partial(),
        }
    }

    crate fn set_write_handle(&mut self, wh: backlog::WriteHandle) {
        assert!(self.writer.is_none());
        self.writer = Some(wh);
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.state.as_ref().map(|s| &s[..])
    }

    pub fn set_key(&mut self, key: &[usize]) {
        if let Some(ref skey) = self.state {
            assert_eq!(&skey[..], key);
        } else {
            self.state = Some(Vec::from(key));
        }
    }

    crate fn state_size(&self) -> Option<u64> {
        self.writer.as_ref().map(SizeOf::deep_size_of)
    }

    /// Evict a randomly selected key, returning the number of bytes evicted.
    /// Note that due to how `evmap` applies the evictions asynchronously, we can only evict a
    /// single key at a time here.
    crate fn evict_random_key(&mut self) -> u64 {
        let mut bytes_freed = 0;
        if let Some(ref mut handle) = self.writer {
            let mut rng = rand::thread_rng();
            bytes_freed = handle.evict_random_key(&mut rng);
            handle.swap();
        }
        bytes_freed
    }

    pub(in crate::node) fn on_eviction(&mut self, _key_columns: &[usize], keys: &[Vec<DataType>]) {
        // NOTE: *could* be None if reader has been created but its state hasn't been built yet
        if let Some(w) = self.writer.as_mut() {
            for k in keys {
                w.mut_with_key(&k[..]).mark_hole();
            }
            w.swap();
        }
    }

    pub(in crate::node) fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: DomainIndex,
        shard: usize,
        swap: bool,
    ) {
        if self.writer.is_some() {
            self.preprocess_packet(m, (from, shard));
        }

        if let Some(ref mut state) = self.writer {
            let m = m.as_mut().unwrap();

            // make sure we don't fill a partial materialization
            // hole with incomplete (i.e., non-replay) state.
            if m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Ok((None, _)) => {
                                // row would miss in partial state.
                                // leave it blank so later lookup triggers replay.
                                false
                            }
                            Err(_) => unreachable!(),
                            _ => {
                                // state is already present,
                                // so we can safely keep it up to date.
                                true
                            }
                        }
                    });
                });
            }

            // it *can* happen that multiple readers miss (and thus request replay for) the
            // same hole at the same time. we need to make sure that we ignore any such
            // duplicated replay.
            if !m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Ok((None, _)) => {
                                // filling a hole with replay -- ok
                                true
                            }
                            Ok((Some(_), _)) => {
                                // a given key should only be replayed to once!
                                false
                            }
                            Err(_) => {
                                // state has not yet been swapped, which means it's new,
                                // which means there are no readers, which means no
                                // requests for replays have been issued by readers, which
                                // means no duplicates can be received.
                                true
                            }
                        }
                    });
                });
            }

            if self.streamers.is_empty() {
                state.add(m.take_data());
            } else {
                state.add(m.data().iter().cloned());
            }

            if swap {
                // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
                state.swap();
            }
        }

        // TODO: don't send replays to streams?

        m.as_mut().unwrap().trace(PacketEvent::ReachedReader);

        if !self.streamers.is_empty() {
            let mut data = Some(m.take().unwrap().take_data()); // so we can .take() for last tx
            let mut left = self.streamers.len();

            // remove any channels where the receiver has hung up
            self.streamers.retain(|tx| {
                left -= 1;
                if left == 0 {
                    tx.send(data.take().unwrap().into_iter().map(Into::into).collect())
                } else {
                    tx.send(data.clone().unwrap().into_iter().map(Into::into).collect())
                }
                .is_ok()
            });
        }
    }
}

// fault tolerance (duplicate code from egress.rs)
impl Reader {
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

    pub fn new_incoming(&mut self, old: DomainIndex, new: DomainIndex) {
        if self.min_provenance.new_incoming(old, new) {
            /*
            // Remove the old domain from the updates entirely
            for update in self.updates.iter_mut() {
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

    pub fn preprocess_packet(&mut self, m: &mut Option<Box<Packet>>, from: ReplicaAddr) {
        // provenance
        let update = if let Some(diff) = m.as_ref().unwrap().id() {
            ProvenanceUpdate::new_with(from, self.num_payloads, &[diff.clone()])
        } else {
            ProvenanceUpdate::new(from, self.num_payloads)
        };
        self.max_provenance.apply_update(&update);
        self.updates.push(update);

        // update num_payloads if not a replay
        if let Some(box Packet::ReplayPiece { .. }) = m {
        } else {
            self.num_payloads += 1;
        }
    }
}
