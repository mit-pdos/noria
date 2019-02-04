use domain;
use ops;
use petgraph;
use prelude::*;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

mod process;
pub use self::process::materialize;

pub mod special;
pub use self::special::StreamUpdate;

mod ntype;
pub use self::ntype::NodeType;

mod replica;
pub use self::replica::ReplicaType;

mod debug;

#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
    name: String,
    index: Option<IndexPair>,
    domain: Option<domain::Index>,

    fields: Vec<String>,
    children: Vec<LocalNodeIndex>,
    inner: NodeType,
    taken: bool,

    sharded_by: Sharding,
    replica: Option<ReplicaType>,
    /// The last packet received and processed from each parent
    pub last_packet_received: HashMap<NodeIndex, u32>,
    /// The next packet to send to each child, starts at 1
    pub next_packet_to_send: HashMap<NodeIndex, u32>,
    /// The packet buffer with the payload and list of to-nodes, starts at 1
    buffer: Vec<HashSet<NodeIndex>>,
}

// constructors
impl Node {
    pub fn new<S1, FS, S2, NT>(name: S1, fields: FS, inner: NT) -> Node
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        NT: Into<NodeType>,
    {
        Node {
            name: name.to_string(),
            index: None,
            domain: None,

            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            children: Vec::new(),
            inner: inner.into(),
            taken: false,

            sharded_by: Sharding::None,
            replica: None,
            last_packet_received: HashMap::new(),
            next_packet_to_send: HashMap::new(),
            buffer: Vec::new(),
        }
    }

    pub fn mirror<NT: Into<NodeType>>(&self, n: NT) -> Node {
        Self::new(&*self.name, &self.fields, n)
    }

    pub fn named_mirror<NT: Into<NodeType>>(&self, n: NT, name: String) -> Node {
        Self::new(name, &self.fields, n)
    }
}

#[must_use]
pub struct DanglingDomainNode(Node);

impl DanglingDomainNode {
    pub fn finalize(self, graph: &Graph) -> Node {
        let mut n = self.0;
        let ni = n.global_addr();
        let dm = n.domain();
        n.children = graph
            .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
            .filter(|&c| graph[c].domain() == dm)
            .map(|ni| graph[ni].local_addr())
            .collect();
        n
    }
}

// events
impl Node {
    pub fn take(&mut self) -> DanglingDomainNode {
        assert!(!self.taken);
        assert!(
            (!self.is_internal() && !self.is_base()) || self.domain.is_some(),
            "tried to take unassigned node"
        );

        let inner = self.inner.take();
        let mut n = self.mirror(inner);
        n.index = self.index;
        n.domain = self.domain;
        self.taken = true;

        DanglingDomainNode(n)
    }

    pub fn remove(&mut self) {
        self.inner = NodeType::Dropped;
    }

    /// Set this node's sharding property.
    pub fn shard_by(&mut self, s: Sharding) {
        self.sharded_by = s;
    }

    pub fn on_commit(&mut self, remap: &HashMap<NodeIndex, IndexPair>) {
        // this is *only* overwritten for these asserts.
        assert!(!self.taken);
        if let NodeType::Internal(ref mut i) = self.inner {
            i.on_commit(self.index.unwrap().as_global(), remap)
        }
    }
}

// derefs
impl Node {
    pub fn with_sharder_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut special::Sharder),
    {
        match self.inner {
            NodeType::Sharder(ref mut s) => f(s),
            _ => unreachable!(),
        }
    }

    pub fn with_sharder<'a, F, R>(&'a self, f: F) -> Option<R>
    where
        F: FnOnce(&'a special::Sharder) -> R,
        R: 'a,
    {
        match self.inner {
            NodeType::Sharder(ref s) => Some(f(s)),
            _ => None,
        }
    }

    pub fn with_egress_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut special::Egress),
    {
        match self.inner {
            NodeType::Egress(Some(ref mut e)) => f(e),
            _ => unreachable!(),
        }
    }

    pub fn with_reader_mut<'a, F, R>(&'a mut self, f: F) -> Result<R, ()>
    where
        F: FnOnce(&'a mut special::Reader) -> R,
        R: 'a,
    {
        match self.inner {
            NodeType::Reader(ref mut r) => Ok(f(r)),
            _ => Err(()),
        }
    }

    pub fn with_reader<'a, F, R>(&'a self, f: F) -> Result<R, ()>
    where
        F: FnOnce(&'a special::Reader) -> R,
        R: 'a,
    {
        match self.inner {
            NodeType::Reader(ref r) => Ok(f(r)),
            _ => Err(()),
        }
    }

    pub fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        match self.inner {
            NodeType::Internal(ref i) => i.suggest_indexes(n),
            NodeType::Base(ref b) => b.suggest_indexes(n),
            _ => HashMap::new(),
        }
    }
}

impl Deref for Node {
    type Target = ops::NodeOperator;
    fn deref(&self) -> &Self::Target {
        match self.inner {
            NodeType::Internal(ref i) => i,
            _ => unreachable!(),
        }
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        assert!(!self.taken);
        match self.inner {
            NodeType::Internal(ref mut i) => i,
            _ => unreachable!(),
        }
    }
}

// children
impl Node {
    pub fn children(&self) -> &[LocalNodeIndex] {
        &self.children[..]
    }

    pub fn child(&self, i: usize) -> &LocalNodeIndex {
        &self.children[i]
    }

    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    pub fn nchildren(&self) -> usize {
        self.children.len()
    }
}

// attributes
impl Node {
    pub fn sharded_by(&self) -> Sharding {
        self.sharded_by
    }

    pub fn add_child(&mut self, child: LocalNodeIndex) {
        self.children.push(child);
    }

    pub fn try_remove_child(&mut self, child: LocalNodeIndex) -> bool {
        for i in 0..self.children.len() {
            if self.children[i] == child {
                self.children.swap_remove(i);
                return true;
            }
        }
        false
    }

    pub fn add_column(&mut self, field: &str) -> usize {
        self.fields.push(field.to_string());
        self.fields.len() - 1
    }

    pub fn name(&self) -> &str {
        &*self.name
    }

    pub fn fields(&self) -> &[String] {
        &self.fields[..]
    }

    pub fn has_domain(&self) -> bool {
        self.domain.is_some()
    }

    pub fn domain(&self) -> domain::Index {
        match self.domain {
            Some(domain) => domain,
            None => {
                unreachable!(
                    "asked for unset domain for {:?} {}",
                    self,
                    self.global_addr().index()
                );
            }
        }
    }

    pub fn local_addr(&self) -> LocalNodeIndex {
        match self.index {
            Some(idx) if idx.has_local() => *idx,
            Some(_) | None => unreachable!("asked for unset addr for {:?}", self),
        }
    }

    pub fn global_addr(&self) -> NodeIndex {
        match self.index {
            Some(ref index) => index.as_global(),
            None => {
                unreachable!("asked for unset index for {:?}", self);
            }
        }
    }

    pub fn get_index(&self) -> &IndexPair {
        self.index.as_ref().unwrap()
    }

    pub fn get_base(&self) -> Option<&special::Base> {
        if let NodeType::Base(ref b) = self.inner {
            Some(b)
        } else {
            None
        }
    }

    pub fn get_base_mut(&mut self) -> Option<&mut special::Base> {
        if let NodeType::Base(ref mut b) = self.inner {
            Some(b)
        } else {
            None
        }
    }

    pub fn is_base(&self) -> bool {
        if let NodeType::Base(..) = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_localized(&self) -> bool {
        self.index
            .as_ref()
            .map(|idx| idx.has_local())
            .unwrap_or(false)
            && self.domain.is_some()
    }

    pub fn add_to(&mut self, domain: domain::Index) {
        assert_eq!(self.domain, None);
        assert!(!self.is_dropped());
        self.domain = Some(domain);
    }

    pub fn set_finalized_addr(&mut self, addr: IndexPair) {
        self.index = Some(addr);
    }
}

// replication
impl Node {
    pub fn replica_type(&self) -> Option<ReplicaType> {
        self.replica.clone()
    }

    pub fn set_replica_type(&mut self, rt: ReplicaType) {
        self.replica = Some(rt);
    }

    /// Receive a packet from the given node.
    ///
    /// This node keeps track of the latest packet received from each parent so that if the parent
    /// were to crash, we can tell the parent's replacement where to resume sending messages.
    pub fn receive_packet(&mut self, from: NodeIndex, label: u32) {
        let me = self.global_addr().index();
        println!( "{} RECEIVE #{} from {:?}", me, label, from);
        let current_label = self.last_packet_received.entry(from).or_insert(0);
        assert!(label > *current_label);
        *current_label = label;
    }

    /// Stores the outgoing packet payload and target to-nodes in the buffer.
    ///
    /// We track this information in case we need to resend messages on recovery. To-nodes must be
    /// children and labels must increase sequentially. Returns a subset of target to-nodes, which
    /// is the list of nodes to actually send to. If a node is not listed, we must wait for a
    /// ResumeAt message from that node to resume sending messages.
    ///
    /// Note that it's ok for next packet to send to be ahead of the packets that have actually
    /// been sent. Either this information is nulled in anticipation of a ResumeAt message, or
    /// it is lost anyway on crash.
    pub fn send_packet(&mut self, to_nodes: HashSet<NodeIndex>, label: u32) -> HashSet<NodeIndex> {
        let me = self.global_addr().index();
        for ni in &to_nodes {
            let current_label = self.next_packet_to_send.entry(*ni).or_insert(1);
            println!("{} SEND #{} to {:?}", me, *current_label, ni);
            // intermediate messages aren't send to this node
            for i in *current_label..label {
                assert!(!self.buffer[i as usize - 1].contains(ni));
            }
            assert_eq!(label, *current_label);
            *current_label += 1;
        }

        self.buffer.push(to_nodes.clone());
        to_nodes
    }

    /// The id to be assigned to the next outgoing packet.
    pub fn next_packet_id(&self) -> PacketId {
        let me = self.global_addr();
        let label = (self.buffer.len() as u32) + 1;
        PacketId::new(label, me)
    }
}

// is this or that?
impl Node {
    pub fn is_source(&self) -> bool {
        if let NodeType::Source { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_dropped(&self) -> bool {
        if let NodeType::Dropped = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_egress(&self) -> bool {
        if let NodeType::Egress { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_sharder(&self) -> bool {
        if let NodeType::Sharder { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_reader(&self) -> bool {
        if let NodeType::Reader { .. } = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_ingress(&self) -> bool {
        if let NodeType::Ingress = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_internal(&self) -> bool {
        if let NodeType::Internal(..) = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_aggregator(&self) -> bool {
        if let NodeType::Internal(ops::NodeOperator::Sum(..)) = self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_sender(&self) -> bool {
        match self.inner {
            NodeType::Egress { .. } | NodeType::Sharder(..) => true,
            _ => false,
        }
    }

    pub fn is_shard_merger(&self) -> bool {
        if let NodeType::Internal(NodeOperator::Union(ref u)) = self.inner {
            u.is_shard_merger()
        } else {
            false
        }
    }

    /// A node is considered to be an output node if changes to its state are visible outside of
    /// its domain.
    pub fn is_output(&self) -> bool {
        match self.inner {
            NodeType::Egress { .. } | NodeType::Reader(..) | NodeType::Sharder(..) => true,
            _ => false,
        }
    }
}
