use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use prelude::*;

pub mod base;
pub mod grouped;
pub mod join;
pub mod latest;
pub mod project;
pub mod union;
pub mod identity;
pub mod filter;
pub mod topk;
pub mod trigger;
pub mod rewrite;

#[derive(Clone, Serialize, Deserialize)]
pub enum NodeOperator {
    Base(base::Base),
    Sum(grouped::GroupedOperator<grouped::aggregate::Aggregator>),
    Extremum(grouped::GroupedOperator<grouped::extremum::ExtremumOperator>),
    Concat(grouped::GroupedOperator<grouped::concat::GroupConcat>),
    Join(join::Join),
    Latest(latest::Latest),
    Project(project::Project),
    Union(union::Union),
    Identity(identity::Identity),
    Filter(filter::Filter),
    TopK(topk::TopK),
    Trigger(trigger::Trigger),
    Rewrite(rewrite::Rewrite),
}

macro_rules! nodeop_from_impl {
    ($variant:path, $type:ty) => {
        impl From<$type> for NodeOperator {
            fn from(other: $type) -> Self {
                $variant(other)
            }
        }
    }
}

nodeop_from_impl!(NodeOperator::Base, base::Base);
nodeop_from_impl!(
    NodeOperator::Sum,
    grouped::GroupedOperator<grouped::aggregate::Aggregator>
);
nodeop_from_impl!(
    NodeOperator::Extremum,
    grouped::GroupedOperator<grouped::extremum::ExtremumOperator>
);
nodeop_from_impl!(
    NodeOperator::Concat,
    grouped::GroupedOperator<grouped::concat::GroupConcat>
);
nodeop_from_impl!(NodeOperator::Join, join::Join);
nodeop_from_impl!(NodeOperator::Latest, latest::Latest);
nodeop_from_impl!(NodeOperator::Project, project::Project);
nodeop_from_impl!(NodeOperator::Union, union::Union);
nodeop_from_impl!(NodeOperator::Identity, identity::Identity);
nodeop_from_impl!(NodeOperator::Filter, filter::Filter);
nodeop_from_impl!(NodeOperator::TopK, topk::TopK);
nodeop_from_impl!(NodeOperator::Trigger, trigger::Trigger);
nodeop_from_impl!(NodeOperator::Rewrite, rewrite::Rewrite);

macro_rules! impl_ingredient_fn_mut {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Base(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Sum(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Join(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Project(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Union(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref mut i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Trigger(ref mut i) => i.$fn($($arg),*),
            NodeOperator::Rewrite(ref mut i) => i.$fn($($arg),*),
        }
    }
}

macro_rules! impl_ingredient_fn_ref {
    ($self:ident, $fn:ident, $( $arg:ident ),* ) => {
        match *$self {
            NodeOperator::Base(ref i) => i.$fn($($arg),*),
            NodeOperator::Sum(ref i) => i.$fn($($arg),*),
            NodeOperator::Extremum(ref i) => i.$fn($($arg),*),
            NodeOperator::Concat(ref i) => i.$fn($($arg),*),
            NodeOperator::Join(ref i) => i.$fn($($arg),*),
            NodeOperator::Latest(ref i) => i.$fn($($arg),*),
            NodeOperator::Project(ref i) => i.$fn($($arg),*),
            NodeOperator::Union(ref i) => i.$fn($($arg),*),
            NodeOperator::Identity(ref i) => i.$fn($($arg),*),
            NodeOperator::Filter(ref i) => i.$fn($($arg),*),
            NodeOperator::TopK(ref i) => i.$fn($($arg),*),
            NodeOperator::Trigger(ref i) => i.$fn($($arg),*),
            NodeOperator::Rewrite(ref i) => i.$fn($($arg),*),
        }
    }
}

impl Ingredient for NodeOperator {
    fn take(&mut self) -> NodeOperator {
        impl_ingredient_fn_mut!(self, take,)
    }
    fn ancestors(&self) -> Vec<NodeIndex> {
        impl_ingredient_fn_ref!(self, ancestors,)
    }
    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        impl_ingredient_fn_ref!(self, must_replay_among,)
    }
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        impl_ingredient_fn_ref!(self, suggest_indexes, you)
    }
    fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>> {
        impl_ingredient_fn_ref!(self, resolve, i)
    }
    fn get_base(&self) -> Option<&base::Base> {
        impl_ingredient_fn_ref!(self, get_base,)
    }
    fn get_base_mut(&mut self) -> Option<&mut base::Base> {
        impl_ingredient_fn_mut!(self, get_base_mut,)
    }
    fn is_join(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_join,)
    }
    fn description(&self) -> String {
        impl_ingredient_fn_ref!(self, description,)
    }
    fn on_connected(&mut self, graph: &Graph) {
        impl_ingredient_fn_mut!(self, on_connected, graph)
    }
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        impl_ingredient_fn_mut!(self, on_commit, you, remap)
    }
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        replay_key_col: Option<usize>,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> ProcessingResult {
        impl_ingredient_fn_mut!(
            self,
            on_input,
            from,
            data,
            tracer,
            replay_key_col,
            domain,
            states
        )
    }
    fn on_input_raw(
        &mut self,
        from: LocalNodeIndex,
        data: Records,
        tracer: &mut Tracer,
        replay: &ReplayContext,
        domain: &DomainNodes,
        states: &StateMap,
    ) -> RawProcessingResult {
        impl_ingredient_fn_mut!(
            self,
            on_input_raw,
            from,
            data,
            tracer,
            replay,
            domain,
            states
        )
    }
    fn on_eviction(&mut self, key_columns: &[usize], keys: &[Vec<DataType>]) {
        impl_ingredient_fn_mut!(self, on_eviction, key_columns, keys)
    }
    fn can_query_through(&self) -> bool {
        impl_ingredient_fn_ref!(self, can_query_through,)
    }
    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType,
        states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        impl_ingredient_fn_ref!(self, query_through, columns, key, states)
    }
    fn lookup<'a>(
        &self,
        parent: LocalNodeIndex,
        columns: &[usize],
        key: &KeyType,
        domain: &DomainNodes,
        states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        impl_ingredient_fn_ref!(self, lookup, parent, columns, key, domain, states)
    }
    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        impl_ingredient_fn_ref!(self, parent_columns, column)
    }
    fn is_selective(&self) -> bool {
        impl_ingredient_fn_ref!(self, is_selective,)
    }
    fn requires_full_materialization(&self) -> bool {
        impl_ingredient_fn_ref!(self, requires_full_materialization,)
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::cell;

    use prelude::*;
    use node;

    use petgraph::graph::NodeIndex;

    pub struct MockGraph {
        graph: Graph,
        source: NodeIndex,
        nut: Option<IndexPair>, // node under test
        states: StateMap,
        nodes: DomainNodes,
        remap: HashMap<NodeIndex, IndexPair>,
    }

    impl MockGraph {
        pub fn new() -> MockGraph {
            let mut graph = Graph::new();
            let source = graph.add_node(Node::new(
                "source",
                &["because-type-inference"],
                node::NodeType::Source,
                true,
            ));
            MockGraph {
                graph: graph,
                source: source,
                nut: None,
                states: StateMap::new(),
                nodes: DomainNodes::default(),
                remap: HashMap::new(),
            }
        }

        pub fn add_base(&mut self, name: &str, fields: &[&str]) -> IndexPair {
            self.add_base_defaults(name, fields, vec![])
        }

        pub fn add_base_defaults(
            &mut self,
            name: &str,
            fields: &[&str],
            defaults: Vec<DataType>,
        ) -> IndexPair {
            use ops::base::Base;
            let mut i = Base::new(defaults);
            i.on_connected(&self.graph);
            let i: NodeOperator = i.into();
            let global = self.graph.add_node(Node::new(name, fields, i, false));
            self.graph.add_edge(self.source, global, ());
            let mut remap = HashMap::new();
            let local = unsafe { LocalNodeIndex::make(self.remap.len() as u32) };
            let mut ip: IndexPair = global.into();
            ip.set_local(local);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .set_finalized_addr(ip);
            remap.insert(global, ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .on_commit(&remap);
            self.states.insert(local, State::default());
            self.remap.insert(global, ip);
            ip
        }

        pub fn set_op<I>(&mut self, name: &str, fields: &[&str], mut i: I, materialized: bool)
        where
            I: Ingredient + Into<NodeOperator>,
        {
            use petgraph;
            assert!(self.nut.is_none(), "only one node under test is supported");

            i.on_connected(&self.graph);
            let parents = i.ancestors();
            assert!(!parents.is_empty(), "node under test should have ancestors");

            let i: NodeOperator = i.into();
            let global = self.graph.add_node(Node::new(name, fields, i, false));
            let local = unsafe { LocalNodeIndex::make(self.remap.len() as u32) };
            if materialized {
                self.states.insert(local, State::default());
            }
            for parent in parents {
                self.graph.add_edge(parent, global, ());
            }
            let mut ip: IndexPair = global.into();
            ip.set_local(local);
            self.remap.insert(global, ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .set_finalized_addr(ip);
            self.graph
                .node_weight_mut(global)
                .unwrap()
                .on_commit(&self.remap);

            // we need to set the indices for all the base tables so they *actually* store things.
            let idx = self.graph[global].suggest_indexes(global);
            for (tbl, (col, _)) in idx {
                if let Some(ref mut s) = self.states.get_mut(self.graph[tbl].local_addr()) {
                    s.add_key(&col[..], None);
                }
            }
            // and get rid of states we don't need
            let unused: Vec<_> = self.remap
                .values()
                .filter_map(|ni| {
                    let ni = *self.graph[ni.as_global()].local_addr();
                    self.states.get(&ni).map(move |s| (ni, !s.is_useful()))
                })
                .filter(|&(_, x)| x)
                .collect();
            for (ni, _) in unused {
                self.states.remove(&ni);
            }

            // we're now committing to testing this op
            // add all nodes to the same domain
            for node in self.graph.node_weights_mut() {
                if node.is_source() {
                    continue;
                }
                node.add_to(0.into());
            }
            // store the id
            self.nut = Some(ip);
            // and also set up the node list
            let mut nodes = vec![];
            let mut topo = petgraph::visit::Topo::new(&self.graph);
            while let Some(node) = topo.next(&self.graph) {
                if node == self.source {
                    continue;
                }
                let n = self.graph[node].take();
                let n = n.finalize(&self.graph);
                nodes.push((node, n));
            }

            self.nodes = nodes
                .into_iter()
                .map(|(_, n)| {
                    use std::cell;
                    (*n.local_addr(), cell::RefCell::new(n))
                })
                .collect();
        }

        pub fn seed(&mut self, base: IndexPair, data: Vec<DataType>) {
            assert!(self.nut.is_some(), "seed must happen after set_op");

            // base here is some identifier that was returned by Self::add_base.
            // which means it's a global address (and has to be so that it will correctly refer to
            // ancestors pre on_commit). we need to translate it into a local address.
            // since we set up the graph, we actually know that the NodeIndex is simply one greater
            // than the local index (since bases are added first, and assigned local + global
            // indices in order, but global ids are prefixed by the id of the source node).

            // no need to call on_input since base tables just forward anyway

            // if the base node has state, keep it
            if let Some(ref mut state) = self.states.get_mut(&*base) {
                match data.into() {
                    Record::Positive(r) => state.insert(r, None),
                    Record::Negative(_) => unreachable!(),
                    Record::DeleteRequest(..) => unreachable!(),
                };
            } else {
                assert!(
                    false,
                    "unnecessary seed value for {} (never used by any node)",
                    base.as_global().index()
                );
            }
        }

        pub fn unseed(&mut self, base: IndexPair) {
            assert!(self.nut.is_some(), "unseed must happen after set_op");
            self.states.get_mut(&*base).unwrap().clear();
        }

        pub fn one<U: Into<Records>>(&mut self, src: IndexPair, u: U, remember: bool) -> Records {
            assert!(self.nut.is_some());
            assert!(!remember || self.states.contains_key(&*self.nut.unwrap()));

            let mut u = {
                let id = self.nut.unwrap();
                let mut n = self.nodes[&*id].borrow_mut();
                let m = n.on_input(*src, u.into(), &mut None, None, &self.nodes, &self.states);
                assert_eq!(m.misses, vec![]);
                m.results
            };

            if !remember || !self.states.contains_key(&*self.nut.unwrap()) {
                return u;
            }

            node::materialize(&mut u, None, self.states.get_mut(&*self.nut.unwrap()));
            u
        }

        pub fn one_row<R: Into<Record>>(
            &mut self,
            src: IndexPair,
            d: R,
            remember: bool,
        ) -> Records {
            self.one::<Record>(src, d.into(), remember)
        }

        pub fn narrow_one<U: Into<Records>>(&mut self, u: U, remember: bool) -> Records {
            let src = self.narrow_base_id();
            self.one::<Records>(src, u.into(), remember)
        }

        pub fn narrow_one_row<R: Into<Record>>(&mut self, d: R, remember: bool) -> Records {
            self.narrow_one::<Record>(d.into(), remember)
        }

        pub fn node(&self) -> cell::Ref<Node> {
            self.nodes[&*self.nut.unwrap()].borrow()
        }

        pub fn narrow_base_id(&self) -> IndexPair {
            assert_eq!(self.remap.len(), 2 /* base + nut */);
            *self.remap
                .values()
                .skip_while(|&n| n.as_global() == self.nut.unwrap().as_global())
                .next()
                .unwrap()
        }
    }
}
