use std::cmp::Ordering;
use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use vec_map::VecMap;

use core::time::Time;
use prelude::*;
use payload::{EgressForBase, IngressFromBase, ReplayTransactionState, TransactionState};
use domain;

struct BufferedMessage {
    at: Time,
    prev: Option<VectorTime>,
    base: NodeIndex,
    packets: Vec<Box<Packet>>,
}
impl Ord for BufferedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at)
    }
}
impl PartialOrd for BufferedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.at.cmp(&other.at))
    }
}
impl PartialEq for BufferedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}
impl Eq for BufferedMessage {}

enum SpecialEntry {
    MigrationStart,
    MigrationEnd(IngressFromBase, EgressForBase),
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
}
struct BufferedSpecial {
    at: VectorTime,
    prev: VectorTime,
    special: SpecialEntry,
}

enum Bundle {
    Messages(Vec<Box<Packet>>),
    MigrationStart,
    MigrationEnd(IngressFromBase, EgressForBase),
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
}

pub enum Event {
    Transaction(Vec<Box<Packet>>),
    StartMigration,
    CompleteMigration,
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
    None,
}

pub struct TimeAssignment {
    pub source: TimeSource,
    pub time: Time,
    pub prev: Option<VectorTime>,
}

pub struct DomainState {
    domain_index: domain::Index,

    next_transaction: Option<Bundle>,

    /// Map for TimeSource to Vec for BufferedMessage. At all times, every Vec must be sorted in
    /// timestamp order and be contigious. This property should arise for free because messages
    /// along a single path must arrive in order.
    message_buffer: VecMap<Vec<BufferedMessage>>,

    /// Unsorted Vec of special messages that can't yet be processed.
    special_buffer: Vec<BufferedSpecial>,

    /// Number of ingress nodes in the domain that receive updates from each base node. Base nodes
    /// that are only connected by timestamp ingress nodes are not included.
    ingress_from_base: Vec<usize>,

    /// Reachable egress (or rather, output) nodes from a given base inside this domain.
    egress_for_base: EgressForBase,

    /// Map from base node to the next time assignment it will give.
    base_times: HashMap<NodeIndex, TimeAssignment>,

    /// Timestamp that the domain has seen all transactions up to.
    ts: VectorTime,
}

impl DomainState {
    pub fn new(domain_index: domain::Index, ts: VectorTime) -> Self {
        Self {
            domain_index,
            next_transaction: None,
            message_buffer: VecMap::new(),
            special_buffer: Vec::new(),
            ingress_from_base: Vec::new(),
            egress_for_base: Default::default(),
            base_times: HashMap::new(),
            ts,
        }
    }

    pub fn egress_for(&self, base: NodeIndex) -> &[LocalNodeIndex] {
        &self.egress_for_base[&base][..]
    }

    pub fn assign_time(&mut self, base: NodeIndex) -> TimeAssignment {
        let assignment: &mut TimeAssignment = self.base_times.get_mut(&base).unwrap();
        let ret = TimeAssignment {
            source: assignment.source,
            time: assignment.time,
            prev: assignment.prev.take(),
        };
        assignment.time.increment();
        ret
    }

    fn buffer_transaction(&mut self, m: Box<Packet>) {
        let (at, base, past_prev) = match *m {
            Packet::VtMessage {
                state:
                    TransactionState::VtCommitted {
                        ref at,
                        prev: None,
                        base,
                    },
                ..
            } => (Some(at.clone()), Some(base), true),
            Packet::VtMessage {
                state:
                    TransactionState::VtCommitted {
                        ref at,
                        prev: Some(ref p),
                        base,
                    },
                ..
            } => (Some(at.clone()), Some(base), self.ts >= p),
            Packet::StartMigration {
                ref at, ref prev, ..
            } => (None, None, self.ts >= prev),
            Packet::CompleteMigration {
                ref at, ref prev, ..
            } => (None, None, self.ts >= prev),
            Packet::ReplayPiece {
                transaction_state: Some(ReplayTransactionState { ref at, ref prev }),
                ..
            } => (None, None, self.ts >= prev),
            _ => unreachable!(),
        };

        if past_prev && self.next_transaction.is_none() && base.is_none() {
            match m {
                box Packet::StartMigration { ref at, .. }
                | box Packet::CompleteMigration { ref at, .. }
                | box Packet::ReplayPiece {
                    transaction_state: Some(ReplayTransactionState { ref at, .. }),
                    ..
                } => {
                    self.ts.advance_to(at);
                }
                _ => unreachable!(),
            }

            self.next_transaction = Some(match (m,) {
                (box Packet::StartMigration { .. },) => Bundle::MigrationStart,
                (box Packet::CompleteMigration {
                    ingress_from_base,
                    egress_for_base,
                    ..
                },) => Bundle::MigrationEnd(ingress_from_base, egress_for_base),
                m @ (box Packet::ReplayPiece { .. },) => Bundle::Replay(m.0),
                _ => unreachable!(),
            });
        } else if past_prev && self.next_transaction.is_none()
            && *self.ingress_from_base
                .get(base.as_ref().unwrap().index())
                .unwrap_or(&1) == 1
        {
            let at = at.unwrap();
            assert_eq!(self.ts[at.1].next(), at.0);
            self.ts[at.1] = at.0;
            self.next_transaction = Some(Bundle::Messages(vec![m]));
        } else if base.is_some() {
            let (prev, base) = if let box Packet::VtMessage {
                state: TransactionState::VtCommitted { ref prev, base, .. },
                ..
            } = m
            {
                (prev.clone(), base)
            } else {
                unreachable!()
            };
            let at = at.unwrap();
            let buf = self.message_buffer.entry(at.1).or_insert_with(Vec::new);
            if buf.is_empty() || at.0 == buf[buf.len() - 1].at.next() {
                buf.push(BufferedMessage {
                    at: at.0,
                    prev,
                    base,
                    packets: vec![m],
                });
            } else {
                let i = at.0.difference(buf[0].at) as usize;
                buf[i].packets.push(m);
            }
        } else {
            if let m @ box Packet::ReplayPiece { .. } = m {
                let (at, prev) = if let box Packet::ReplayPiece {
                    transaction_state: Some(ReplayTransactionState { ref at, ref prev }),
                    ..
                } = m
                {
                    (at.clone(), prev.clone())
                } else {
                    unreachable!()
                };

                self.special_buffer.push(BufferedSpecial {
                    at,
                    prev,
                    special: SpecialEntry::Replay(m),
                });
                return;
            }

            let (at, prev, special) = match (m,) {
                (box Packet::StartMigration { at, prev },) => {
                    (at, prev, SpecialEntry::MigrationStart)
                }
                (box Packet::CompleteMigration {
                    at,
                    prev,
                    ingress_from_base,
                    egress_for_base,
                },) => (
                    at,
                    prev,
                    SpecialEntry::MigrationEnd(ingress_from_base, egress_for_base),
                ),
                _ => unreachable!(),
            };
            self.special_buffer
                .push(BufferedSpecial { at, prev, special });
        }
    }

    pub fn handle(&mut self, m: Box<Packet>) {
        self.buffer_transaction(m);
    }

    fn update_next_transaction(&mut self) {
        assert!(self.next_transaction.is_none());

        let mut next = None;
        for (source, ref messages) in self.message_buffer.iter() {
            if !messages.is_empty()
                && (messages[0].prev.is_none() || self.ts >= messages[0].prev.as_ref().unwrap())
                && messages[0].packets.len() == self.ingress_from_base[messages[0].base.index()]
            {
                next = Some(source);
                break;
            }
        }
        if let Some(source) = next {
            let m = self.message_buffer[source].remove(0);
            assert_eq!(self.ts[source].next(), m.at);
            self.ts[source] = m.at;
            self.next_transaction = Some(Bundle::Messages(m.packets));
            return;
        }

        for i in 0..self.special_buffer.len() {
            if self.ts >= self.special_buffer[i].prev {
                let special = self.special_buffer.remove(i);
                self.ts.advance_to(&special.at);
                self.next_transaction = Some(match special.special {
                    SpecialEntry::MigrationStart => Bundle::MigrationStart,
                    SpecialEntry::MigrationEnd(ingress_from_base, egress_for_base) => {
                        Bundle::MigrationEnd(ingress_from_base, egress_for_base)
                    }
                    SpecialEntry::Replay(packet) => Bundle::Replay(packet),
                    SpecialEntry::SeedReplay(tag, key, rts) => Bundle::SeedReplay(tag, key, rts),
                });
                break;
            }
        }
    }

    pub fn get_next_event(&mut self) -> Event {
        if self.next_transaction.is_none() {
            return Event::None;
        }

        match self.next_transaction.take().unwrap() {
            Bundle::MigrationStart => {
                self.update_next_transaction();
                Event::StartMigration
            }
            Bundle::MigrationEnd(ingress_from_base, egress_for_base) => {
                let max_index = ingress_from_base
                    .keys()
                    .map(|ni| ni.index())
                    .max()
                    .unwrap_or(0);
                self.ingress_from_base = vec![0; max_index + 1];
                for (ni, count) in ingress_from_base.into_iter() {
                    self.ingress_from_base[ni.index()] = count;
                }
                self.egress_for_base = egress_for_base;

                self.update_next_transaction();
                Event::CompleteMigration
            }
            Bundle::Messages(v) => {
                self.update_next_transaction();
                Event::Transaction(v)
            }
            Bundle::Replay(packet) => {
                self.update_next_transaction();
                Event::Replay(packet)
            }
            Bundle::SeedReplay(tag, key, rts) => {
                self.update_next_transaction();
                Event::SeedReplay(tag, key, rts)
            }
        }
    }

    pub fn schedule_replay(&mut self, tag: Tag, key: Vec<DataType>) {
        let (at, prev) = unimplemented!(); // TODO(jbehrens)

        if self.ts >= prev && self.next_transaction.is_none() {
            self.ts.advance_to(&at);
            self.next_transaction = Some(Bundle::SeedReplay(
                tag,
                key,
                ReplayTransactionState { at, prev },
            ));
        } else {
            // Clones are kind of sad...
            self.special_buffer.push(BufferedSpecial {
                at: at.clone(),
                prev: prev.clone(),
                special: SpecialEntry::SeedReplay(tag, key, ReplayTransactionState { at, prev }),
            });
        }
    }
}
