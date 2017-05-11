use std::collections::{HashSet, HashMap};
use std::sync::Arc;

use ops::base::Base;
use flow::prelude;

#[derive(PartialEq, Eq, Debug)]
pub struct Miss {
    pub node: prelude::LocalNodeIndex,
    pub key: Vec<prelude::DataType>,
}

pub struct ProcessingResult {
    pub results: prelude::Records,
    pub misses: Vec<Miss>,
}

pub enum RawProcessingResult {
    Regular(ProcessingResult),
    ReplayPiece(prelude::Records),
    Captured,
}

pub trait Ingredient
    where Self: Send
{
    /// Construct a new node from this node that will be given to the domain running this node.
    /// Whatever is left behind in self is what remains observable in the graph.
    fn take(&mut self) -> Box<Ingredient>;

    fn ancestors(&self) -> Vec<prelude::NodeAddress>;
    fn should_materialize(&self) -> bool;

    /// May return a set of nodes such that *one* of the given ancestors *must* be the one to be
    /// replayed if this node's state is to be initialized.
    fn must_replay_among(&self) -> Option<HashSet<prelude::NodeAddress>> {
        None
    }

    /// Should return true if this ingredient will ever query the state of an ancestor.
    fn will_query(&self, materialized: bool) -> bool;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    ///
    /// Note that a vector of length > 1 for any one node means that that node should be given a
    /// *compound* key, *not* that multiple columns should be independently indexed.
    fn suggest_indexes(&self,
                       you: prelude::NodeAddress)
                       -> HashMap<prelude::NodeAddress, Vec<usize>>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, i: usize) -> Option<Vec<(prelude::NodeAddress, usize)>>;

    /// Returns a reference to the underlying Base node (if any)
    fn get_base(&self) -> Option<&Base> {
        None
    }

    /// Returns a mutable reference to the underlying Base node (if any)
    fn get_base_mut(&mut self) -> Option<&mut Base> {
        None
    }

    fn is_join(&self) -> bool {
        false
    }

    /// Produce a compact, human-readable description of this node.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    fn description(&self) -> String;

    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet. Only addresses of the type `prelude::NodeAddress::Global` may be used.
    fn on_connected(&mut self, graph: &prelude::Graph);

    /// Called when a domain is finalized and is about to be booted.
    ///
    /// The provided arguments give mappings from global to local addresses. After this method has
    /// been invoked (and crucially, in `Ingredient::on_input`) only addresses of the type
    /// `prelude::NodeAddress::Local` may be used.
    fn on_commit(&mut self,
                 you: prelude::NodeAddress,
                 remap: &HashMap<prelude::NodeAddress, prelude::NodeAddress>);

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    ///
    /// Only addresses of the type `prelude::NodeAddress::Local` may be used in this function.
    fn on_input(&mut self,
                from: prelude::NodeAddress,
                data: prelude::Records,
                tracer: &mut prelude::Tracer,
                domain: &prelude::DomainNodes,
                states: &prelude::StateMap)
                -> ProcessingResult;

    fn on_input_raw(&mut self,
                    from: prelude::NodeAddress,
                    data: prelude::Records,
                    tracer: &mut prelude::Tracer,
                    is_replay_of: Option<(usize, prelude::DataType)>,
                    domain: &prelude::DomainNodes,
                    states: &prelude::StateMap)
                    -> RawProcessingResult {
        let _ = is_replay_of;
        RawProcessingResult::Regular(self.on_input(from, data, tracer, domain, states))
    }

    fn can_query_through(&self) -> bool {
        false
    }

    fn query_through<'a>
        (&self,
         _columns: &[usize],
         _key: &prelude::KeyType<prelude::DataType>,
         _states: &'a prelude::StateMap)
         -> Option<Option<Box<Iterator<Item = &'a Arc<Vec<prelude::DataType>>> + 'a>>> {
        None
    }

    /// Look up the given key in the given parent's state, falling back to query_through if
    /// necessary. The return values signifies:
    ///
    ///  - `None` => no materialization of the parent state exists
    ///  - `Some(None)` => materialization exists, but lookup got a miss
    ///  - `Some(Some(rs))` => materialization exists, and got results rs
    fn lookup<'a>(&self,
                  parent: prelude::NodeAddress,
                  columns: &[usize],
                  key: &prelude::KeyType<prelude::DataType>,
                  domain: &prelude::DomainNodes,
                  states: &'a prelude::StateMap)
                  -> Option<Option<Box<Iterator<Item = &'a Arc<Vec<prelude::DataType>>> + 'a>>> {
        states
            .get(parent.as_local())
            .and_then(move |state| match state.lookup(columns, key) {
                          prelude::LookupResult::Some(rs) => {
                              Some(Some(Box::new(rs.iter()) as Box<_>))
                          }
                          prelude::LookupResult::Missing => Some(None),
                      })
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                let parent = domain.get(parent.as_local()).unwrap().borrow();
                if parent.is_internal() {
                    parent.query_through(columns, key, states)
                } else {
                    None
                }
            })
    }

    // Translate a column in this ingredient into the corresponding column(s) in
    // parent ingredients. None for the column means that the parent doesn't
    // have an associated column. Similar to resolve, but does not depend on
    // materialization, and returns results even for computed columns.
    fn parent_columns(&self, column: usize) -> Vec<(prelude::NodeAddress, Option<usize>)>;

    /// Performance hint: should return true if this operator reduces the size of its input
    fn is_selective(&self) -> bool {
        false
    }

    fn into_serializable(&self) -> SerializableIngredient;
}

#[derive(Serialize, Deserialize)]
pub enum SerializableGrouped {
    Aggregation(grouped::aggregate::Aggregator),
    Extremum(grouped::extremum::ExtremumOperator),
    GroupConcat(grouped::concat::GroupConcat),
}

use ops;
use ops::grouped;
#[derive(Serialize, Deserialize)]
pub enum SerializableIngredient {
    Filter(ops::filter::Filter),
    Identity(ops::identity::Identity),
    Join(ops::join::Join),
    Latest(ops::latest::Latest),
    Permute(ops::permute::Permute),
    Project(ops::project::Project),
    Union(ops::union::Union),
    TopK(ops::topk::TopK),

    Grouped {
        src: prelude::NodeAddress,
        inner: SerializableGrouped,

        us: Option<prelude::NodeAddress>,
        cols: usize,

        group_by: Vec<usize>,
        out_key: Vec<usize>,
        colfix: Vec<usize>,
    },

    Base {
        defaults: Vec<prelude::DataType>,
        primary_key: Option<Vec<usize>>,
        durability: Option<ops::base::BaseDurabilityLevel>,
        us: prelude::NodeAddress,
    },
}

impl SerializableIngredient {
    pub fn into_ingredient(self) -> Box<Ingredient> {
        use ops;
        use self::SerializableGrouped::*;
        use ops::grouped::*;
        match self {
            SerializableIngredient::Identity(i) => Box::new(i),
            SerializableIngredient::Join(i) => Box::new(i),
            SerializableIngredient::Latest(i) => Box::new(i),
            SerializableIngredient::Permute(i) => Box::new(i),
            SerializableIngredient::Project(i) => Box::new(i),
            SerializableIngredient::Union(i) => Box::new(i),
            SerializableIngredient::Filter(i) => Box::new(i),
            SerializableIngredient::TopK(i) => Box::new(i),
            g @ SerializableIngredient::Grouped { inner: Aggregation(..), .. } => {
                Box::new(GroupedOperator::<grouped::aggregate::Aggregator>::from_serialized(g))
            }
            g @ SerializableIngredient::Grouped { inner: GroupConcat(..), .. } => {
                Box::new(GroupedOperator::<grouped::concat::GroupConcat>::from_serialized(g))
            }
            g @ SerializableIngredient::Grouped { inner: Extremum(..), .. } => {
                Box::new(GroupedOperator::<grouped::extremum::ExtremumOperator>::from_serialized(g))
            }
            SerializableIngredient::Base {
                defaults,
                primary_key,
                durability,
                us,
            } => {
                let mut base = ops::base::Base::new(defaults);
                if primary_key.is_some() {
                    base = base.with_key(primary_key.unwrap());
                }
                if durability.is_some() {
                    base = base.with_durability(durability.unwrap());
                }
                base.on_commit(us, &HashMap::new());
                Box::new(base)
            }
        }
    }
}
