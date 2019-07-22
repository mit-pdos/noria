use backlog;
use noria::channel;
use prelude::*;
use std::collections::HashMap;

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

    pub(crate) updates: Updates,
    /// Number of non-replay messages received.
    num_payloads: usize,

    // Log truncation
    /// All labels associated with an address
    labels: AddrLabels,
    /// The minimum label associated with an address
    min_labels: AddrLabel,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        assert!(self.writer.is_none());
        Reader {
            writer: None,
            streamers: self.streamers.clone(),
            state: self.state.clone(),
            for_node: self.for_node,
            updates: self.updates.clone(),
            num_payloads: self.num_payloads,
            labels: self.labels.clone(),
            min_labels: self.min_labels.clone(),
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex) -> Self {
        Reader {
            writer: None,
            streamers: Vec::new(),
            state: None,
            for_node,
            updates: Default::default(),
            num_payloads: 0,
            labels: Default::default(),
            min_labels: Default::default(),
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
            updates: self.updates.clone(),
            num_payloads: self.num_payloads,
            labels: self.labels.clone(),
            min_labels: self.min_labels.clone(),
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
    ) -> AddrLabel {
        let changed = if self.writer.is_some() {
            let (mut old, mut new) = self.preprocess_packet(m, (from, shard));

            // Use the changed labels to determine if there is a new minimum label associated
            // with a replica address in this particular replica.
            let mut changed = HashMap::new();
            for (addr, old_labels) in old.drain() {
                // TODO(ygina): do this more efficiently with a min heap
                // Remove labels that were replaced by the update
                let labels = self.labels.get_mut(&addr).unwrap();
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
                let mut new_labels = new.remove(&addr).expect("old and new have the same keys");
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
        } else {
            AddrLabel::default()
        };

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
        changed
    }
}

// fault tolerance (duplicate code from egress.rs)
impl Reader {
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
                assert_eq!(update[0].0, old);
                update.remove(0);
            }
            unimplemented!();
        } else {
            // Regenerated domains should have the same index
        }
        */
    }

    pub fn preprocess_packet(
        &mut self,
        m: &mut Option<Box<Packet>>,
        from: ReplicaAddr,
    ) -> (AddrLabels, AddrLabels) {
        let (mtype, is_replay) = match m {
            Some(box Packet::ReplayPiece { .. }) => ("ReplayPiece", true),
            Some(box Packet::Message { .. }) => ("Message", false),
            _ => unreachable!(),
        };

        // provenance
        let label = if is_replay {
            self.num_payloads
        } else {
            self.num_payloads + 1
        };
        let update = if let Some(diff) = m.as_ref().unwrap().id() {
            ProvenanceUpdate::new_with(from, label, &[diff.clone()])
        } else {
            ProvenanceUpdate::new(from, label)
        };
        let (old, new) = self.updates.add_update(&update);

        // update num_payloads if not a replay
        if !is_replay {
            self.num_payloads += 1;
        }
        (old, new)
    }
}
