use fnv::FnvHashMap;
use petgraph::graph::NodeIndex;

/// The upstream branch of nodes and message labels that was updated to produce the current
/// message, starting at the node above the payload's "from" node. The number of nodes in the
/// update is linear in the depth of the update.
pub type ProvenanceUpdate = Vec<(NodeIndex, usize)>;

/// The history of message labels that correspond to the production of the current message.
/// TODO(ygina): doesn't double-count nodes that split then merge
pub type Provenance = FnvHashMap<NodeIndex, usize>;
