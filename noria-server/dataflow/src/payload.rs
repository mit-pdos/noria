use petgraph;
use serde::{Deserialize, Serialize};

#[cfg(debug_assertions)]
use backtrace::Backtrace;
use domain;
use node;
use noria;
use noria::channel;
use noria::internal::LocalOrNot;
use prelude::*;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::time;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub partial_key: Option<Vec<usize>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum SourceSelection {
    /// Query only the shard of the source that matches the key.
    ///
    /// Value is the number of shards.
    KeyShard(usize),
    /// Query the same shard of the source as the destination.
    SameShard,
    /// Query all shards of the source.
    ///
    /// Value is the number of shards.
    AllShards(usize),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(SourceSelection, domain::Index),
    Local(Vec<usize>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum InitialState {
    PartialLocal(Vec<(Vec<usize>, Vec<Tag>)>),
    IndexedLocal(HashSet<Vec<usize>>),
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
        trigger_domain: (domain::Index, usize),
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SourceChannelIdentifier {
    pub token: usize,
    pub tag: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    // Data messages
    //
    Input {
        inner: LocalOrNot<Input>,
        src: Option<SourceChannelIdentifier>,
        senders: Vec<SourceChannelIdentifier>,
    },

    /// Regular data-flow update.
    Message {
        id: Option<ProvenanceUpdate>,
        link: Link,
        data: Records,
        tracer: Tracer,
    },

    /// Update that is part of a tagged data-flow replay path.
    ReplayPiece {
        id: Option<ProvenanceUpdate>,
        link: Link,
        tag: Tag,
        data: Records,
        context: ReplayPieceContext,
    },

    /// Trigger an eviction from the target node.
    Evict {
        node: Option<LocalNodeIndex>,
        num_bytes: usize,
    },

    /// Evict the indicated keys from the materialization targed by the replay path `tag` (along
    /// with any other materializations below it).
    EvictKeys {
        id: Option<ProvenanceUpdate>,
        link: Link,
        tag: Tag,
        keys: Vec<Vec<DataType>>,
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

    /// Direct domain to remove some nodes.
    RemoveNodes {
        nodes: Vec<LocalNodeIndex>,
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
        ack: bool,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestPartialReplay {
        id: Option<ProvenanceUpdate>,
        tag: Tag,
        key: Vec<DataType>,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestReaderReplay {
        node: LocalNodeIndex,
        cols: Vec<usize>,
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
        purge: bool,
        index: HashSet<Vec<usize>>,
    },

    /// Notification from Blender for domain to terminate
    Quit,

    /// A packet used solely to drive the event loop forward.
    Spin,

    /// Request that a domain send usage statistics on the control reply channel.
    /// Argument specifies if we wish to get the full state size or just the partial nodes.
    GetStatistics,

    /// Ask domain to log its state size
    UpdateStateSize,

    // Recovery messages
    //
    /// Tell the node to become a full node operator, if appropriate, and to get rid of its
    /// replica status
    MakeRecovery {
        node: LocalNodeIndex,
    },

    /// Tell the node to stop sending messages to this child
    RemoveChild {
        child: NodeIndex,
        domain: DomainIndex,
    },

    /// Remove the replay path corresponding to the tag in the domain
    RemoveTag {
        old_tag: Tag,
        new_state: Option<(LocalNodeIndex, Tag)>,
    },

    /// Notify downstream nodes of an incoming connection to replace an existing one
    NewIncoming {
        old: DomainIndex,
        new: DomainIndex,
    },

    /// Notify the domain to resume sending messages to its children from the given packet labels
    ResumeAt {
        child_labels: Vec<(NodeIndex, usize)>,
    },
}

impl Packet {
    crate fn id(&self) -> &Option<ProvenanceUpdate> {
        match *self {
            Packet::Message { ref id, .. } => id,
            Packet::ReplayPiece { ref id, .. } => id,
            Packet::EvictKeys { ref id, .. } => id,
            _ => unreachable!(),
        }
    }

    crate fn id_mut(&mut self) -> &mut Option<ProvenanceUpdate> {
        match *self {
            Packet::Message { ref mut id, .. } => id,
            Packet::ReplayPiece { ref mut id, .. } => id,
            Packet::EvictKeys { ref mut id, .. } => id,
            _ => unreachable!(),
        }
    }

    crate fn src(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => {
                // inputs come "from" the base table too
                unsafe { inner.deref() }.dst
            }
            Packet::Message { ref link, .. } => link.src,
            Packet::ReplayPiece { ref link, .. } => link.src,
            _ => unreachable!(),
        }
    }

    crate fn dst(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => unsafe { inner.deref() }.dst,
            Packet::Message { ref link, .. } => link.dst,
            Packet::ReplayPiece { ref link, .. } => link.dst,
            _ => unreachable!(),
        }
    }

    crate fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::EvictKeys { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    crate fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    crate fn map_data<F>(&mut self, map: F)
    where
        F: FnOnce(&mut Records),
    {
        match *self {
            Packet::Message { ref mut data, .. } | Packet::ReplayPiece { ref mut data, .. } => {
                map(data);
            }
            _ => {
                unreachable!();
            }
        }
    }

    crate fn is_regular(&self) -> bool {
        match *self {
            Packet::Message { .. } => true,
            _ => false,
        }
    }

    crate fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            Packet::EvictKeys { tag, .. } => Some(tag),
            _ => None,
        }
    }

    crate fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    crate fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    crate fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                ref id,
                link,
                ref data,
                ref tracer,
            } => Packet::Message {
                id: id.clone(),
                link,
                data: data.clone(),
                tracer: tracer.clone(),
            },
            Packet::ReplayPiece {
                ref id,
                link,
                tag,
                ref data,
                ref context,
            } => Packet::ReplayPiece {
                id: id.clone(),
                link,
                tag,
                data: data.clone(),
                context: context.clone(),
            },
            _ => unreachable!(),
        }
    }

    crate fn size_of_id(&self) -> u64 {
        match *self {
            Packet::Message { ref id, .. } => {
                // A provenance struct has 16 for the root, 8 for the label, and 40 for the hashmap.
                // Each entry in the hashmap is 16 for the key, 8 for the provenance pointer, and
                // 64 for each provenance struct. Total is 64+88n where n is the number of edges.
                if let Some(id) = id {
                    64 + 88 * id.edges().len() as u64
                } else {
                    unreachable!()
                }
            },
            _ => unreachable!(),
        }
    }

    crate fn size_of_data(&self) -> u64 {
        match *self {
            Packet::Message { ref data, .. } => {
                // Records(Vec<Record>) is 24 for the Vec. Each Record is 24+8=32 because 24 is
                // the Vec<DataType> and 8 is to distinguish which enum it is, positive or
                // negative. Each DataType is 16. Let k=3 be the number of data types in each
                // record. Thus each Records is 24+(32+16k)n=24+80n
                24 + 80 * data.len() as u64
            },
            _ => unreachable!(),
        }
    }

    crate fn trace(&self, event: PacketEvent) {
        if let Packet::Message {
            tracer: Some((tag, Some(ref sender))),
            ..
        } = *self
        {
            use noria::debug::trace::{Event, EventType};
            sender
                .send(Event {
                    instant: time::Instant::now(),
                    event: EventType::PacketEvent(event, tag),
                })
                .unwrap();
        }
    }

    crate fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } => Some(tracer),
            _ => None,
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Input { .. } => write!(f, "Packet::Input"),
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::RequestReaderReplay { ref key, .. } => {
                write!(f, "Packet::RequestReaderReplay({:?})", key)
            }
            Packet::RequestPartialReplay { ref tag, .. } => {
                write!(f, "Packet::RequestPartialReplay({:?})", tag)
            }
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => write!(
                f,
                "Packet::ReplayPiece({:?}, tag {}, {} records)",
                link,
                tag.id(),
                data.len()
            ),
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlReplyPacket {
    #[cfg(debug_assertions)]
    Ack(Backtrace),
    #[cfg(not(debug_assertions))]
    Ack(()),
    /// (number of rows, size in bytes)
    StateSize(usize, u64),
    Statistics(
        noria::debug::stats::DomainStats,
        HashMap<petgraph::graph::NodeIndex, noria::debug::stats::NodeStats>,
    ),
    Booted(usize, SocketAddr),
}

impl ControlReplyPacket {
    #[cfg(debug_assertions)]
    crate fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(Backtrace::new())
    }

    #[cfg(not(debug_assertions))]
    crate fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(())
    }
}
