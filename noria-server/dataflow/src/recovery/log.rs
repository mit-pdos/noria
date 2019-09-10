use prelude::*;
use std::collections::HashMap;

pub const PROVENANCE_DEPTH: usize = 3;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Updates {
    /// Whether updates should be stored or just max provenance
    store_updates: bool,
    /// Base provenance with all diffs applied
    max_clock: TreeClock,

    // The following fields are only used if we are storing updates
    /// Base provenance, including the label it represents
    min_clock: TreeClock,
    /// TreeClock updates sent in outgoing packets
    updates: Vec<TreeClockDiff>,
}

impl Updates {
    pub fn init(&mut self, graph: &DomainGraph, store_updates: bool, root: ReplicaAddr) {
        self.store_updates = store_updates;
        for ni in graph.node_indices() {
            if graph[ni] == root {
                self.min_clock.init(graph, root, ni, PROVENANCE_DEPTH);
                return;
            }
        }
        unreachable!();
    }

    pub fn init_in_domain(&mut self, shard: usize) -> AddrLabels {
        self.min_clock.set_shard(shard);
        self.max_clock = self.min_clock.clone();
        self.max_clock.into_addr_labels()
    }

    pub fn init_after_resume_at(&mut self, provenance: TreeClock) -> AddrLabels {
        assert!(self.updates.is_empty());
        assert_eq!(self.min_clock.label(), 0);
        assert_eq!(self.max_clock.label(), 0);
        self.min_clock = provenance;
        self.max_clock = self.min_clock.clone();
        self.max_clock.into_addr_labels()
    }

    /// The label of the next message to send
    ///
    /// Replays don't get buffered and don't increment their label (they use the last label
    /// sent by this domain - think of replays as a snapshot of what's already been sent).
    pub fn next_label_to_send(&self, is_message: bool) -> usize {
        if is_message {
            self.max_clock.label() + 1
        } else {
            self.max_clock.label()
        }
    }

    /// Add the update of the next message we're about to send to our state
    pub fn add_update(&mut self, update: &TreeClockDiff) -> (AddrLabels, AddrLabels) {
        if self.store_updates {
            self.updates.push(update.clone());
        }
        self.max_clock.apply_update(update)
    }

    /// Max provenance
    pub fn max(&self) -> &TreeClock {
        &self.max_clock
    }

    /// The provenance and updates that should be sent to ack a new incoming message
    pub fn ack_new_incoming(
        &self,
        incoming: ReplicaAddr,
    ) -> (Box<TreeClock>, Vec<TreeClockDiff>) {
        if self.store_updates {
            let provenance = self.min_clock.subgraph(incoming).unwrap().clone();
            let updates = self.updates
                .iter()
                .filter_map(|update| update.subgraph(incoming))
                .map(|update| *update.clone())
                .collect::<Vec<_>>();
            (provenance, updates)
        } else {
            assert!(self.updates.is_empty());
            let provenance = self.max_clock.subgraph(incoming).unwrap().clone();
            (provenance, vec![])
        }
    }

    /// Truncate updates based on the parent node in the provenance update -- we can truncate any
    /// update where the parent's label is at most the corresponding parent's label in the map.
    pub fn truncate(&mut self, at: HashMap<ReplicaAddr, usize>) -> usize {
        assert!(self.store_updates);

        // Find the index at which we'd like to keep all proceeding updates
        let mut i_to_keep = 0;
        for (i, update) in self.updates.iter().enumerate() {
            if let Some(parent) = update.parent() {
                let max_label = at.get(&parent.root()).expect("truncation map includes all parents");
                if parent.label() <= *max_label {
                    i_to_keep = i;
                } else {
                    break;
                }
            }
        }

        // Drain and apply the rest of the updates to min_clock
        // println!("TRUNCATING {} UPDATES", i_to_keep);
        for update in self.updates.drain(..i_to_keep) {
            self.min_clock.apply_update(&update);
        }
        self.min_clock.label()
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Payloads {
    /// Label of the last packet not in payloads
    min_label: usize,
    /// Packet payloads
    payloads: Vec<Box<Packet>>,
}

impl Payloads {
    /// Add the payload to our buffer
    pub fn add_payload(&mut self, m: Box<Packet>) {
        self.payloads.push(m);
    }

    /// Get payloads after this index, inclusive
    pub fn slice(&self, i: usize) -> Vec<Box<Packet>> {
        assert!(i >= self.min_label);
        if i == self.min_label {
            vec![]
        } else {
            let real_i = i - self.min_label - 1;
            self.payloads[real_i..]
                .iter()
                .map(|m| box m.clone_data())
                .collect()
        }
    }

    pub fn init_after_resume_at(&mut self, min_label: usize) {
        self.min_label = min_label;
    }

    /// Truncate payloads - the label in the argument will _not_ remain in the payloads
    pub fn truncate(&mut self, at: usize) {
        if at >= self.min_label {
            self.payloads = self.payloads.split_off(at - self.min_label);
            self.min_label = at;
        } else {
            // log truncation was sent during recovery
        }
    }
}
