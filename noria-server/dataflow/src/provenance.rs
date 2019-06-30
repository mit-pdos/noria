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
            write!(f, "D{}.{}:{}", self.root.0.index(), self.root.1, self.label)
        } else {
            write!(f, "D{}.{}:{} {:?}", self.root.0.index(), self.root.1, self.label, edges)
        }
    }
}

impl ProvenanceUpdate {
    pub fn new(root: ReplicaAddr, label: usize) -> ProvenanceUpdate {
        ProvenanceUpdate {
            root,
            label,
            edges: Default::default(),
        }
    }

    pub fn new_with(root: ReplicaAddr, label: usize, children: &[ProvenanceUpdate]) -> ProvenanceUpdate {
        let mut p = ProvenanceUpdate::new(root, label);
        for child in children {
            p.add_child(child.clone());
        }
        p
    }

    pub fn add_child(&mut self, child: ProvenanceUpdate) {
        self.edges.insert(child.root, box child);
    }

    /// Trim the provenance tree to the given depth.
    pub fn trim(&mut self, depth: usize) {
        assert!(depth > 0);
        if depth == 1 {
            self.edges.clear();
            return;
        }
        for (_, p) in self.edges.iter_mut() {
            p.trim(depth - 1);
        }
    }

    pub fn diff(&self, other: &ProvenanceUpdate) -> ProvenanceUpdate {
        assert_eq!(self.root, other.root);
        if self.label == other.label {
            ProvenanceUpdate::new(self.root, self.label)
        } else if self.label < other.label {
            let mut diff = ProvenanceUpdate::new(other.root, other.label);
            for (domain, p_other) in &other.edges {
                if let Some(p_self) = self.edges.get(domain) {
                    if p_self.label < p_other.label {
                        diff.add_child(p_self.diff(p_other));
                    }
                }
            }
            diff
        } else {
            unreachable!();
        }
    }

    pub fn zero(&mut self) {
        self.label = 0;
        for (_, p) in self.edges.iter_mut() {
            p.zero();
        }
    }
}

/// The history of message labels that correspond to the production of the current message.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    root: ReplicaAddr,
    label: usize,
    edges: FnvHashMap<ReplicaAddr, Box<Provenance>>,
}

impl Default for Provenance {
    // TODO(ygina): it doesn't really make sense to have a provenance for imaginary domain index 0,
    // so maybe we should use options here. this is hacky and gross. the reason we have a default
    // implementation is hack to intiialize the provenance graph in the egress AFTER it has
    // been otherwise initialized and fit into the graph.
    fn default() -> Provenance {
        Provenance {
            root: (0.into(), 0),
            edges: Default::default(),
            label: 0,
        }
    }
}

impl Provenance {
    /// Initializes the provenance graph from the root domain/shard up to the given depth.
    /// Typically called on a default Provenance struct, compared to an empty one.
    pub fn init(
        &mut self,
        graph: &DomainGraph,
        root: ReplicaAddr,
        root_ni: NodeIndex,
        depth: usize,
    ) {
        assert_eq!(graph[root_ni], root);
        self.root = root;
        if depth > 1 {
            let children = graph
                .neighbors_directed(root_ni, petgraph::EdgeDirection::Incoming)
                .collect::<Vec<_>>();
            for child_ni in children {
                let mut provenance = Provenance::default();
                provenance.init(graph, graph[child_ni], child_ni, depth - 1);
                self.edges.insert(graph[child_ni], box provenance);
            }
        }
    }

    pub fn root(&self) -> ReplicaAddr {
        self.root
    }

    pub fn label(&self) -> usize {
        self.label
    }

    pub fn edges(&self) -> &FnvHashMap<ReplicaAddr, Box<Provenance>> {
        &self.edges
    }

    pub fn set_shard(&mut self, shard: usize) {
        self.root.1 = shard;
    }

    pub fn set_label(&mut self, label: usize) {
        self.label = label;
    }

    /// The diff must have the same root and label as the provenance it's being applied to.
    /// The diff should strictly be ahead in time in comparison.
    pub fn apply_update(&mut self, update: &ProvenanceUpdate) {
        assert_eq!(self.root, update.root);
        // Ignore the assertion below in the very specific case that a stateless domain with
        // multiple parents is reconstructed but without being able to recover its lost provenance
        // information. We could theoretically reconstruct this provenance by waiting for a message
        // from each parent, but it shouldn't actually matter when losing multi-parent stateless
        // domains since the result of one message shouldn't depend on the results of previous
        // messages. For multi-parent stateful domain cases, the provenance information should
        // have been replicated along with the materialized rows.
        //
        // We should be able to add this assertion back once we optimize how much provenance
        // we send per message.
        if self.label > update.label {
            println!("gonna panic because applying update {:?} to self {:?}", update, self);
        }
        assert!(self.label <= update.label);
        if self.label >= update.label {
            // short circuit since all domain-label combinations mean the same thing everywhere,
            // and labels farther in the future contain all information from previous labels
            return;
        }

        self.label = update.label;

        for (domain, p_diff) in &update.edges {
            if let Some(p) = self.edges.get_mut(domain) {
                p.apply_update(p_diff);
            }
        }
    }

    pub fn union(&mut self, other: Provenance) {
        assert_eq!(self.root, other.root);
        assert_eq!(self.label, other.label);
        for (child, other_p) in other.edges.into_iter() {
            if let Some(p) = self.edges.get_mut(&child) {
                p.union(*other_p);
            } else {
                self.edges.insert(child, other_p);
            }
        }
    }

    /// Returns whether a replica failed. :P
    pub fn new_incoming(&mut self, old: ReplicaAddr, new: ReplicaAddr) -> bool {
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
    }

    /// Subgraph of this provenance graph with the given domain as the new root. The new root must
    /// be an ancestor (stateless domain recovery) or grand-ancestor (stateful domain recovery) of
    /// the given node. There's no reason we should obtain any other subgraph in the protocol...
    /// Actually there is. We may be getting the subgraph of an update rather than the total graph.
    pub fn subgraph(&self, new_root: ReplicaAddr) -> Option<&Box<Provenance>> {
        if let Some(p) = self.edges.get(&new_root) {
            return Some(p);
        }
        // replicas
        for (_, p) in &self.edges {
            if let Some(p) = p.edges.get(&new_root){
                return Some(p);
            }
        }
        None
        // unreachable!("must be ancestor or grand-ancestor");
    }

    pub fn into_debug(&self) -> noria::debug::stats::Provenance {
        let mut p = noria::debug::stats::Provenance::new(self.label);
        for (&replica, replica_p) in self.edges.iter() {
            p.edges.insert(replica, box replica_p.into_debug());
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 0     1
    ///  \   /
    ///    2
    ///  /   \
    /// 3     4
    ///  \   /
    ///    5
    fn default_graph() -> DomainGraph {
        // nodes
        let mut g = petgraph::Graph::new();
        let mut nodes = vec![];
        for i in 0..6 {
            let node = g.add_node(addr(i));
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

    fn addr(x: usize) -> ReplicaAddr {
        (x.into(), 0)
    }

    /// Full provenance for the test graph
    fn default_provenance() -> Provenance {
        let p0 = Provenance::new(addr(0), 0);
        let p1 = Provenance::new(addr(1), 0);
        let p2_left = Provenance::new_with(addr(2), 0, &[p0.clone(), p1.clone()]);
        let p2_right = Provenance::new_with(addr(2), 0, &[p0, p1]);
        let p3 = Provenance::new_with(addr(3), 0, &[p2_left]);
        let p4 = Provenance::new_with(addr(4), 0, &[p2_right]);
        Provenance::new_with(addr(5), 0, &[p3, p4])
    }

    const MAX_DEPTH: usize = 10;

    #[test]
    fn test_graph_init_bases() {
        let g = default_graph();

        let expected0 = Provenance::new(addr(0), 0);
        let expected1 = Provenance::new(addr(1), 0);

        let mut p = Provenance::default();
        p.init(&g, addr(0), 0.into(), MAX_DEPTH);
        assert_eq!(p, expected0);
        let mut p = Provenance::default();
        p.init(&g, addr(1), 1.into(), MAX_DEPTH);
        assert_eq!(p, expected1);
        let mut p = Provenance::default();
        p.init(&g, addr(0), 0.into(), 1);
        assert_eq!(p, expected0);
        let mut p = Provenance::default();
        p.init(&g, addr(1), 1.into(), 1);
        assert_eq!(p, expected1);
    }

    #[test]
    fn test_graph_init_leaf() {
        let g = default_graph();
        let mut p5 = default_provenance();

        // max depth and depth 4 should have a path for each branch
        let mut p = Provenance::default();
        p.init(&g, addr(5), 5.into(), MAX_DEPTH);
        assert_eq!(p, p5);
        let mut p = Provenance::default();
        p.init(&g, addr(5), 5.into(), 4);
        assert_eq!(p, p5);

        // depth 3 should have one less layer
        let mut p = Provenance::default();
        p.init(&g, addr(5), 5.into(), 3);
        p5
            .edges.get_mut(&addr(3)).unwrap()
            .edges.get_mut(&addr(2)).unwrap()
            .edges.clear();
        p5
            .edges.get_mut(&addr(4)).unwrap()
            .edges.get_mut(&addr(2)).unwrap()
            .edges.clear();
        assert_eq!(p, p5);

        // depth 2 should have even one less layer
        let mut p = Provenance::default();
        p.init(&g, addr(5), 5.into(), 2);
        p5
            .edges.get_mut(&addr(3)).unwrap()
            .edges.clear();
        p5
            .edges.get_mut(&addr(4)).unwrap()
            .edges.clear();
        assert_eq!(p, p5);

        // depth 1 should be domain 5 by itself
        let mut p = Provenance::default();
        p.init(&g, addr(5), 5.into(), 1);
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
            .edges.get_mut(&addr(4)).unwrap().label = 2;
        expected
            .edges.get_mut(&addr(4)).unwrap()
            .edges.get_mut(&addr(2)).unwrap().label = 3;
        expected
            .edges.get_mut(&addr(4)).unwrap()
            .edges.get_mut(&addr(2)).unwrap()
            .edges.get_mut(&addr(0)).unwrap().label = 4;

        let p0 = Provenance::new(addr(0), 4);
        let p2 = Provenance::new_with(addr(2), 3, &[p0]);
        let p4 = Provenance::new_with(addr(4), 2, &[p2]);
        let diff = Provenance::new_with(addr(5), 1, &[p4]);

        // expected - original = diff
        // original + diff = expected
        assert_eq!(original.diff(&expected), diff);
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
            .edges.get_mut(&addr(3)).unwrap().label = 2;
        expected
            .edges.get_mut(&addr(4)).unwrap().label = 4;
        expected
            .edges.get_mut(&addr(3)).unwrap()
            .edges.get_mut(&addr(2)).unwrap().label = 5;

        let p2 = Provenance::new(addr(2), 5);
        let p4 = Provenance::new(addr(4), 4);
        let p3 = Provenance::new_with(addr(3), 2, &[p2]);
        let diff = Provenance::new_with(addr(5), 3, &[p3, p4]);

        // expected - original = diff
        // original + diff = expected
        assert_eq!(original.diff(&expected), diff);
        original.apply_update(&diff);
        assert_eq!(original, expected);
    }

    #[test]
    fn test_trim() {
        let mut p = default_provenance();

        // depth 3
        p.trim(3);
        assert!(p
            .edges.get_mut(&addr(3)).unwrap()
            .edges.get_mut(&addr(2)).unwrap()
            .edges.is_empty());
        assert!(p
            .edges.get_mut(&addr(4)).unwrap()
            .edges.get_mut(&addr(2)).unwrap()
            .edges.is_empty());

        // depth 2
        p.trim(2);
        assert!(p
            .edges.get_mut(&addr(3)).unwrap()
            .edges.is_empty());
        assert!(p
            .edges.get_mut(&addr(4)).unwrap()
            .edges.is_empty());

        // depth 1
        p.trim(1);
        assert!(p.edges.is_empty());
    }
}
