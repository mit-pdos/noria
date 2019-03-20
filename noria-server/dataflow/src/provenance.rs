use fnv::FnvHashMap;
use petgraph::graph::NodeIndex;

/// The upstream branch of nodes and message labels that was updated to produce the current
/// message, starting at the node above the payload's "from" node. The number of nodes in the
/// update is linear in the depth of the update.
pub type ProvenanceUpdate = Vec<(NodeIndex, usize)>;

/// The history of message labels that correspond to the production of the current message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provenance {
    root: NodeIndex,
    label: usize,
    edges: FnvHashMap<NodeIndex, Box<Provenance>>,
}

impl Provenance {
    /// Initializes the view of the provenance from the root node up to the given depth
    pub fn new(graph: &Graph, root: NodeIndex, depth: usize) -> Provenance {
        let mut edges = FnvHashMap::default();
        if depth > 0 {
            let children = graph.neighbors_directed(root, petgraph::EdgeDirection::Incoming);
            for child in children {
                let provenance = box Provenance::new(graph, child, depth - 1);
                edges.insert(child, provenance);
            }
        }

        Provenance {
            root,
            edges,
            label: 0,
        }
    }

    pub fn label(&self) -> usize {
        self.label
    }

    pub fn set_label(&mut self, label: usize) {
        self.label = label;
    }

    /// Apply a single provenance update cached from the message payloads. In general, this method
    /// is called only when the full provenance is necessary, as in when we need to make recovery.
    pub fn apply_update(&mut self, update: &ProvenanceUpdate) {
        self.label += 1;
        let mut provenance = self;
        for (node, label) in update {
            if let Some(p) = provenance.edges.get_mut(node) {
                p.set_label(*label);
                provenance = p;
            } else {
                break;
            }
        }
    }

    pub fn apply_updates(&mut self, updates: &[ProvenanceUpdate]) {
        for update in updates {
            self.apply_update(update);
        }
    }

    /// Subgraph of this provenance graph with the given node as the new root. The new root must be
    /// an ancestor (stateless domain recovery) or grand-ancestor (stateful domain recovery) of the
    /// given node. There's no reason we should obtain any other subgraph in the recovery protocol.
    pub fn subgraph(&self, new_root: NodeIndex) -> &Box<Provenance> {
        if let Some(p) = self.edges.get(&new_root) {
            return p;
        }
        // replicas
        for (_, p) in &self.edges {
            if let Some(p) = p.edges.get(&new_root){
                return p;
            }
        }
        unreachable!("must be ancestor or grand-ancestor");
    }
}
