use fnv::FnvHashMap;
use prelude::*;
use std::fmt;

/// The upstream branch of domains and message labels that was updated to produce the current
/// message, starting at the node above the payload's "from" node. The number of nodes in the
/// update is linear in the depth of the update.
pub type ProvenanceUpdate = Provenance;

impl fmt::Debug for ProvenanceUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let edges = self.edges.iter().map(|(_, p)| p).collect::<Vec<_>>();
        if edges.is_empty() {
            write!(f, "D{}:{}", self.root.index(), self.label)
        } else {
            write!(f, "D{}:{} {:?}", self.root.index(), self.label, edges)
        }
    }
}

impl ProvenanceUpdate {
    pub fn new(root: DomainIndex, label: usize) -> ProvenanceUpdate {
        ProvenanceUpdate {
            root,
            label,
            edges: Default::default(),
        }
    }

    pub fn new_with(root: DomainIndex, label: usize, children: &[ProvenanceUpdate]) -> ProvenanceUpdate {
        let mut p = ProvenanceUpdate::new(root, label);
        for child in children {
            p.add_child(child.clone());
        }
        p
    }

    pub fn add_child(&mut self, child: ProvenanceUpdate) {
        self.edges.insert(child.root, box child);
    }

    pub fn diff(&self, other: &ProvenanceUpdate) -> Option<ProvenanceUpdate> {
        assert_eq!(self.root, other.root);
        if self.label == other.label {
            None
        } else if self.label < other.label {
            let mut diff = ProvenanceUpdate::new(other.root, other.label);
            for (domain, p_other) in &other.edges {
                if let Some(p_self) = self.edges.get(domain) {
                    if let Some(p_diff) = p_self.diff(p_other) {
                        diff.add_child(p_diff);
                    }
                }
            }
            Some(diff)
        } else {
            unreachable!();
        }
    }
}

/// The history of message labels that correspond to the production of the current message.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
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
        if depth > 1 {
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

    /// The diff must have the same root and label as the provenance it's being applied to.
    /// The diff should strictly be ahead in time in comparison.
    pub fn apply_update(&mut self, update: &ProvenanceUpdate) {
        // assert_eq!(self.root, update.root);
        assert!(self.label <= update.label);
        self.label = update.label;

        for (domain, p_diff) in &update.edges {
            if let Some(p) = self.edges.get_mut(domain) {
                p.apply_update(p_diff);
            }
        }
    }

    /// Returns whether a replica failed. :P
    pub fn new_incoming(&mut self, old: DomainIndex, new: DomainIndex) -> bool {
        assert_eq!(old, new);
        false
        /*
        let mut provenance = self.edges.remove(&old).expect("old connection should exist");

        if let Some(new_p) = provenance.edges.remove(&new){
            // check if a replica failed. if so, make the grand-ancestor an ancestor
            /*
            assert!(provenance.edges.is_empty());
            self.edges.insert(new, new_p);
            true
            */
            unimplemented!();
        }  else {
            // otherwise, just replace the domain index
            provenance.root = new;
            self.edges.insert(new, provenance);
            false
        }
        */
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

    pub fn into_debug(&self) -> noria::debug::stats::Provenance {
        let mut p = noria::debug::stats::Provenance::new(self.label);
        for (&domain, domain_p) in self.edges.iter() {
            p.edges.insert(domain, box domain_p.into_debug());
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use node;

    /// 0     1
    ///  \   /
    ///    2
    ///  /   \
    /// 3     4
    ///  \   /
    ///    5
    fn default_graph() -> Graph {
        // nodes
        let mut g = Graph::new();
        let mut nodes = vec![];
        for i in 0..6 {
            let node = g.add_node(node::Node::new(
                "a",
                &["because-type-inference"],
                node::special::Egress::default(),
            ));
            g[node].set_finalized_addr(IndexPair::from(node));
            g[node].add_to(i.into());
            nodes.push(node);
        }

        // edges
        g.add_edge(nodes[0], nodes[2], ());
        g.add_edge(nodes[1], nodes[2], ());
        g.add_edge(nodes[2], nodes[3], ());
        g.add_edge(nodes[2], nodes[4], ());
        g.add_edge(nodes[3], nodes[5], ());
        g.add_edge(nodes[4], nodes[5], ());
        g
    }

    /// Full provenance for the test graph
    fn default_provenance() -> Provenance {
        let p0 = Provenance::new(0.into(), 0);
        let p1 = Provenance::new(1.into(), 0);
        let p2_left = Provenance::new_with(2.into(), 0, &[p0.clone(), p1.clone()]);
        let p2_right = Provenance::new_with(2.into(), 0, &[p0, p1]);
        let p3 = Provenance::new_with(3.into(), 0, &[p2_left]);
        let p4 = Provenance::new_with(4.into(), 0, &[p2_right]);
        Provenance::new_with(5.into(), 0, &[p3, p4])
    }

    const MAX_DEPTH: usize = 10;

    #[test]
    fn test_graph_init_bases() {
        let g = default_graph();

        let expected0 = Provenance::new(0.into(), 0);
        let expected1 = Provenance::new(1.into(), 0);

        let mut p = Provenance::default();
        p.init(&g, 0.into(), MAX_DEPTH);
        assert_eq!(p, expected0);
        let mut p = Provenance::default();
        p.init(&g, 1.into(), MAX_DEPTH);
        assert_eq!(p, expected1);
        let mut p = Provenance::default();
        p.init(&g, 0.into(), 1);
        assert_eq!(p, expected0);
        let mut p = Provenance::default();
        p.init(&g, 1.into(), 1);
        assert_eq!(p, expected1);
    }

    #[test]
    fn test_graph_init_leaf() {
        let g = default_graph();
        let mut p5 = default_provenance();

        // max depth and depth 4 should have a path for each branch
        let mut p = Provenance::default();
        p.init(&g, 5.into(), MAX_DEPTH);
        assert_eq!(p, p5);
        let mut p = Provenance::default();
        p.init(&g, 5.into(), 4);
        assert_eq!(p, p5);

        // depth 3 should have one less layer
        let mut p = Provenance::default();
        p.init(&g, 5.into(), 3);
        p5
            .edges.get_mut(&DomainIndex::from(3)).unwrap()
            .edges.get_mut(&DomainIndex::from(2)).unwrap()
            .edges.clear();
        p5
            .edges.get_mut(&DomainIndex::from(4)).unwrap()
            .edges.get_mut(&DomainIndex::from(2)).unwrap()
            .edges.clear();
        assert_eq!(p, p5);

        // depth 2 should have even one less layer
        let mut p = Provenance::default();
        p.init(&g, 5.into(), 2);
        p5
            .edges.get_mut(&DomainIndex::from(3)).unwrap()
            .edges.clear();
        p5
            .edges.get_mut(&DomainIndex::from(4)).unwrap()
            .edges.clear();
        assert_eq!(p, p5);

        // depth 1 should be domain 5 by itself
        let mut p = Provenance::default();
        p.init(&g, 5.into(), 1);
        p5.edges.clear();
        assert_eq!(p, p5);
    }

    /// 0*    1
    ///  \*  /
    ///    2*
    ///  /   \*
    /// 3     4*
    ///  \   /*
    ///    5*
    #[test]
    fn test_linear_diff() {
        let mut original = default_provenance();
        let mut expected = original.clone();
        expected.label = 1;
        expected
            .edges.get_mut(&DomainIndex::from(4)).unwrap().label = 2;
        expected
            .edges.get_mut(&DomainIndex::from(4)).unwrap()
            .edges.get_mut(&DomainIndex::from(2)).unwrap().label = 3;
        expected
            .edges.get_mut(&DomainIndex::from(4)).unwrap()
            .edges.get_mut(&DomainIndex::from(2)).unwrap()
            .edges.get_mut(&DomainIndex::from(0)).unwrap().label = 4;

        let p0 = Provenance::new(0.into(), 4);
        let p2 = Provenance::new_with(2.into(), 3, &[p0]);
        let p4 = Provenance::new_with(4.into(), 2, &[p2]);
        let diff = Provenance::new_with(5.into(), 1, &[p4]);

        // expected - original = diff
        // original + diff = expected
        assert_eq!(original.diff(&expected).unwrap(), diff);
        original.apply_update(&diff);
        assert_eq!(original, expected);
    }

    /// 0     1
    ///  \   /
    ///    2*
    ///  /*  \
    /// 3*    4*
    ///  \*  /*
    ///    5*
    #[test]
    fn test_partial_diff() {
        let mut original = default_provenance();
        let mut expected = original.clone();
        expected.label = 3;
        expected
            .edges.get_mut(&DomainIndex::from(3)).unwrap().label = 2;
        expected
            .edges.get_mut(&DomainIndex::from(4)).unwrap().label = 4;
        expected
            .edges.get_mut(&DomainIndex::from(3)).unwrap()
            .edges.get_mut(&DomainIndex::from(2)).unwrap().label = 5;

        let p2 = Provenance::new(2.into(), 5);
        let p4 = Provenance::new(4.into(), 4);
        let p3 = Provenance::new_with(3.into(), 2, &[p2]);
        let diff = Provenance::new_with(5.into(), 3, &[p3, p4]);

        // expected - original = diff
        // original + diff = expected
        assert_eq!(original.diff(&expected).unwrap(), diff);
        original.apply_update(&diff);
        assert_eq!(original, expected);
    }
}
