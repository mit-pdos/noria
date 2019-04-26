use fnv::FnvHashMap;
use prelude::*;

/// The upstream branch of domains and message labels that was updated to produce the current
/// message, starting at the node above the payload's "from" node. The number of nodes in the
/// update is linear in the depth of the update.
pub type ProvenanceUpdate = Vec<(DomainIndex, usize)>;

/// The history of message labels that correspond to the production of the current message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provenance {
    root: DomainIndex,
    label: usize,
    edges: FnvHashMap<DomainIndex, Box<Provenance>>,
}

impl Default for Provenance {
    // TODO(ygina): it doesn't really make sense to have a provenance for imaginary domain index 0,
    // so maybe we should use options here. this is hacky and gross. the reason we have a default
    // implementation is hack to intiialize the provenance graph in the egress AFTER it has
    // been otherwise initialized and fit into the graph.
    fn default() -> Provenance {
        Provenance {
            root: 0.into(),
            edges: Default::default(),
            label: 0,
        }
    }
}

impl Provenance {
    /// Initializes the provenance graph from the root node up to the given depth.
    /// Typically called on a default Provenance struct, compared to an empty one.
    pub fn init(&mut self, graph: &Graph, root: NodeIndex, depth: usize) {
        self.root = graph[root].domain();
        if depth > 0 {
            // TODO(ygina): operate on domain level instead of ingress/egress level
            let mut egresses = Vec::new();
            let mut queue = Vec::new();
            queue.push(root);
            while queue.len() > 0 {
                let ni = queue.pop().unwrap();
                if graph[ni].is_egress() && ni != root {
                    egresses.push(ni);
                    continue;
                }

                let mut children = graph
                    .neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
                    .collect::<Vec<_>>();
                queue.append(&mut children);
            }

            for egress in egresses {
                let mut provenance = Provenance::default();
                provenance.init(graph, egress, depth - 1);
                self.edges.insert(graph[egress].domain(), box provenance);
            }
        }
    }

    pub fn root(&self) -> DomainIndex {
        self.root
    }

    pub fn label(&self) -> usize {
        self.label
    }

    pub fn edges(&self) -> &FnvHashMap<DomainIndex, Box<Provenance>> {
        &self.edges
    }

    pub fn set_label(&mut self, label: usize) {
        self.label = label;
    }

    fn apply_update(&mut self, update: &ProvenanceUpdate) {
        let mut provenance = self;
        for (domain, label) in update {
            if let Some(p) = provenance.edges.get_mut(domain) {
                p.set_label(*label);
                provenance = p;
            } else {
                break;
            }
        }
    }

    /// Apply all provenance updates to the provenance graph, and increment the base graph's label
    /// by the number of packets in the payloads buffer.
    pub fn apply_updates(&mut self, updates: &[ProvenanceUpdate], num_payloads: usize) {
        self.label += num_payloads;
        for update in updates {
            self.apply_update(update);
        }
    }

    /// Returns whether a replica failed. :P
    pub fn new_incoming(&mut self, old: DomainIndex, new: DomainIndex) -> bool {
        let mut provenance = self.edges.remove(&old).expect("old connection should exist");

        if let Some(new_p) = provenance.edges.remove(&new){
            // check if a replica failed. if so, make the grand-ancestor an ancestor
            assert!(provenance.edges.is_empty());
            self.edges.insert(new, new_p);
            true
        }  else {
            // otherwise, just replace the domain index
            provenance.root = new;
            self.edges.insert(new, provenance);
            false
        }
    }

    /// Subgraph of this provenance graph with the given domain as the new root. The new root must
    /// be an ancestor (stateless domain recovery) or grand-ancestor (stateful domain recovery) of
    /// the given node. There's no reason we should obtain any other subgraph in the protocol.
    pub fn subgraph(&self, new_root: DomainIndex) -> &Box<Provenance> {
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
