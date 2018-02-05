use petgraph;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[cfg(debug_assertions)]
use backtrace::Backtrace;
use channel;
use checktable;
use debug::{DebugEvent, DebugEventType};
use domain;
use node;
use statistics;
use prelude::*;

use std::fmt;
use std::collections::{HashMap, HashSet};
use std::time;
use std::net::SocketAddr;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub src: LocalNodeIndex,
    pub dst: LocalNodeIndex,
}

impl Link {
    pub fn new(src: LocalNodeIndex, dst: LocalNodeIndex) -> Self {
        Link { src: src, dst: dst }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub partial_key: Option<usize>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(domain::Index, usize),
    Local(Vec<usize>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum InitialState {
    PartialLocal(Vec<(Vec<usize>, Vec<Tag>)>),
    IndexedLocal(HashSet<Vec<usize>>),
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: usize,
        trigger_domain: (domain::Index, usize),
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: usize,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ReplayPieceContext {
    Partial {
        for_keys: HashSet<Vec<DataType>>,
        ignore: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TransactionState {
    Committed(
        i64,
        petgraph::graph::NodeIndex,
        Option<Box<HashMap<domain::Index, i64>>>,
    ),
    Pending(checktable::Token, SocketAddr),
    WillCommit,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplayTransactionState {
    pub ts: i64,
    pub prevs: Option<Box<HashMap<domain::Index, i64>>>,
}

/// Different events that can occur as a packet is being processed.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum PacketEvent {
    /// The packet has been pulled off the input channel.
    ExitInputChannel,
    /// The packet has been received by some domain, and is being handled.
    Handle,
    /// The packet is being processed at some node.
    Process,
    /// The packet has reached some reader node.
    ReachedReader,
    /// The packet has been merged with another, and will no longer trigger events.
    Merged(u64),
}

pub type Tracer = Option<(u64, Option<channel::TraceSender<DebugEvent>>)>;
pub type IngressFromBase = HashMap<petgraph::graph::NodeIndex, usize>;
pub type EgressForBase = HashMap<petgraph::graph::NodeIndex, Vec<LocalNodeIndex>>;

#[derive(Clone, Serialize, Deserialize)]
pub enum Packet {
    // Data messages
    //
    /// Regular data-flow update.
    Message {
        link: Link,
        data: Records,
        tracer: Tracer,
    },

    /// Transactional data-flow update.
    Transaction {
        link: Link,
        data: Records,
        state: TransactionState,
        tracer: Tracer,
    },

    /// Update that is part of a tagged data-flow replay path.
    ReplayPiece {
        link: Link,
        tag: Tag,
        data: Records,
        nshards: usize,
        context: ReplayPieceContext,
        transaction_state: Option<ReplayTransactionState>,
    },

    //
    // Internal control
    //
    Finish(Tag, LocalNodeIndex),

    // Control messages
    //
    /// Add a new node to this domain below the given parents.
    AddNode {
        node: Node,
        parents: Vec<LocalNodeIndex>,
    },

    /// Add a new column to an existing `Base` node.
    AddBaseColumn {
        node: LocalNodeIndex,
        field: String,
        default: DataType,
    },

    /// Drops an existing column from a `Base` node.
    DropBaseColumn {
        node: LocalNodeIndex,
        column: usize,
    },

    /// Update Egress node.
    UpdateEgress {
        node: LocalNodeIndex,
        new_tx: Option<(NodeIndex, LocalNodeIndex, ReplicaAddr)>,
        new_tag: Option<(Tag, NodeIndex)>,
    },

    /// Add a shard to a Sharder node.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    UpdateSharder {
        node: LocalNodeIndex,
        new_txs: (LocalNodeIndex, Vec<ReplicaAddr>),
    },

    /// Add a streamer to an existing reader node.
    AddStreamer {
        node: LocalNodeIndex,
        new_streamer: channel::StreamSender<Vec<node::StreamUpdate>>,
    },

    /// Set up a fresh, empty state for a node, indexed by a particular column.
    ///
    /// This is done in preparation of a subsequent state replay.
    PrepareState {
        node: LocalNodeIndex,
        state: InitialState,
    },

    /// Probe for the number of records in the given node's state
    StateSizeProbe {
        node: LocalNodeIndex,
    },

    /// Inform domain about a new replay path.
    SetupReplayPath {
        tag: Tag,
        source: Option<LocalNodeIndex>,
        path: Vec<ReplayPathSegment>,
        notify_done: bool,
        trigger: TriggerEndpoint,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestPartialReplay {
        tag: Tag,
        key: Vec<DataType>,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestReaderReplay {
        node: LocalNodeIndex,
        col: usize,
        key: Vec<DataType>,
    },

    /// Instruct domain to replay the state of a particular node along an existing replay path.
    StartReplay {
        tag: Tag,
        from: LocalNodeIndex,
    },

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready {
        node: LocalNodeIndex,
        index: HashSet<Vec<usize>>,
    },

    /// Notification from Blender for domain to terminate
    Quit,

    /// A packet used solely to drive the event loop forward.
    Spin,

    /// Signal that a base node's domain should start replaying logs.
    StartRecovery {
        snapshot_id: u64,
    },

    /// Initiate the snapshotting process. Domains should send ACKs
    /// to the controller containing the given ID.
    TakeSnapshot {
        link: Link,
        snapshot_id: u64,
    },

    // Transaction time messages
    //
    /// Instruct domain to flush pending transactions and notify upon completion. `prev_ts` is the
    /// timestamp of the last transaction sent to the domain prior to at.
    ///
    /// This allows a migration to ensure all transactions happen strictly *before* or *after* a
    /// migration in timestamp order.
    StartMigration {
        at: i64,
        prev_ts: i64,
    },

    /// Notify a domain about a completion timestamp for an ongoing migration.
    ///
    /// Once this message is received, the domain may continue processing transactions with
    /// timestamps following the given one.
    ///
    /// The update also includes the new ingress_from_base counts and egress_from_base map the
    /// domain should use going forward.
    CompleteMigration {
        at: i64,
        ingress_from_base: IngressFromBase,
        egress_for_base: EgressForBase,
    },

    /// Request that a domain send usage statistics on the control reply channel.
    GetStatistics,

    /// The packet was captured awaiting the receipt of other replays.
    Captured,

    /// The packet is being sent locally, so a pointer is sent to avoid
    /// serialization/deserialization costs.
    Local(LocalBypass<Packet>),

    /// Notify downstream replica what our index is
    Hey(domain::Index, usize),
}

impl Packet {
    pub fn link(&self) -> &Link {
        match *self {
            Packet::Message { ref link, .. } => link,
            Packet::Transaction { ref link, .. } => link,
            Packet::ReplayPiece { ref link, .. } => link,
            Packet::TakeSnapshot { ref link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::Transaction { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::TakeSnapshot { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::Transaction { ref data, .. } => data.is_empty(),
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    pub fn map_data<F>(&mut self, map: F)
    where
        F: FnOnce(&mut Records),
    {
        match *self {
            Packet::Message { ref mut data, .. }
            | Packet::Transaction { ref mut data, .. }
            | Packet::ReplayPiece { ref mut data, .. } => {
                map(data);
            }
            _ => {
                unreachable!();
            }
        }
    }

    pub fn is_regular(&self) -> bool {
        match *self {
            Packet::Message { .. } => true,
            Packet::Transaction { .. } => true,
            _ => false,
        }
    }

    pub fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub fn snapshot_id(&self) -> u64 {
        match *self {
            Packet::TakeSnapshot { snapshot_id, .. } => snapshot_id,
            Packet::StartRecovery { snapshot_id, .. } => snapshot_id,
            _ => unreachable!(),
        }
    }

    pub fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::Transaction { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    pub fn swap_data(&mut self, new_data: Records) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::Transaction { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, new_data)
    }

    pub fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::Transaction { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    pub fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                ref link,
                ref data,
                ref tracer,
            } => Packet::Message {
                link: link.clone(),
                data: data.clone(),
                tracer: tracer.clone(),
            },
            Packet::Transaction {
                ref link,
                ref data,
                ref state,
                ref tracer,
            } => Packet::Transaction {
                link: link.clone(),
                data: data.clone(),
                state: state.clone(),
                tracer: tracer.clone(),
            },
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ref nshards,
                ref context,
                ref transaction_state,
            } => Packet::ReplayPiece {
                link: link.clone(),
                tag: tag.clone(),
                data: data.clone(),
                nshards: *nshards,
                context: context.clone(),
                transaction_state: transaction_state.clone(),
            },
            Packet::TakeSnapshot {
                snapshot_id,
                ref link,
            } => Packet::TakeSnapshot {
                snapshot_id,
                link: link.clone(),
            },
            _ => unreachable!(),
        }
    }

    pub fn trace(&self, event: PacketEvent) {
        match *self {
            Packet::Message {
                tracer: Some((tag, Some(ref sender))),
                ..
            }
            | Packet::Transaction {
                tracer: Some((tag, Some(ref sender))),
                ..
            } => {
                sender
                    .send(DebugEvent {
                        instant: time::Instant::now(),
                        event: DebugEventType::PacketEvent(event, tag),
                    })
                    .unwrap();
            }
            _ => {}
        }
    }

    pub fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } | Packet::Transaction { ref mut tracer, .. } => {
                Some(tracer)
            }
            _ => None,
        }
    }

    /// If self is `Packet::Local` then replace with the packet pointed to.
    pub fn extract_local(&mut self) -> Option<Box<Self>> {
        if let Packet::Local(..) = *self {
            use std::mem;
            if let Packet::Local(local) = mem::replace(self, Packet::Spin) {
                Some(unsafe { local.take() })
            } else {
                unreachable!();
            }
        } else {
            None
        }
    }

    pub fn make_local(self: Box<Self>) -> Box<Self> {
        Box::new(Packet::Local(LocalBypass::make(self)))
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::Transaction {
                ref link,
                ref state,
                ..
            } => match *state {
                TransactionState::Committed(ts, ..) => {
                    write!(f, "Packet::Transaction({:?}, {})", link, ts)
                }
                TransactionState::Pending(..) => {
                    write!(f, "Packet::Transaction({:?}, pending)", link)
                }
                TransactionState::WillCommit => write!(f, "Packet::Transaction({:?}, ?)", link),
            },
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => write!(
                f,
                "Packet::ReplayPiece({:?}, {}, {} records)",
                link,
                tag.id(),
                data.len()
            ),
            Packet::Local(ref lp) => {
                use std::mem;
                let lp = unsafe { Box::from_raw(lp.0) };
                let s = write!(f, "local {:?}", lp)?;
                mem::forget(lp);
                Ok(s)
            }
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlReplyPacket {
    #[cfg(debug_assertions)] Ack(Backtrace),
    #[cfg(not(debug_assertions))] Ack(()),
    StateSize(usize),
    Statistics(
        statistics::DomainStats,
        HashMap<petgraph::graph::NodeIndex, statistics::NodeStats>,
    ),
    Booted(usize, SocketAddr),
}

impl ControlReplyPacket {
    #[cfg(debug_assertions)]
    pub fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(Backtrace::new())
    }

    #[cfg(not(debug_assertions))]
    pub fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(())
    }
}

pub struct LocalBypass<T>(*mut T);

impl<T> LocalBypass<T> {
    pub fn make(t: Box<T>) -> Self {
        LocalBypass(Box::into_raw(t))
    }

    pub unsafe fn take(self) -> Box<T> {
        Box::from_raw(self.0)
    }
}

unsafe impl<T> Send for LocalBypass<T> {}
impl<T> Serialize for LocalBypass<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (self.0 as usize).serialize(serializer)
    }
}
impl<'de, T> Deserialize<'de> for LocalBypass<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        usize::deserialize(deserializer).map(|p| LocalBypass(p as *mut T))
    }
}
impl<T> Clone for LocalBypass<T> {
    fn clone(&self) -> LocalBypass<T> {
        panic!("LocalPacket cannot be cloned");
    }
}
