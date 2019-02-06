mod mir;
mod passes;
mod query_graph;
mod query_signature;
mod query_utils;
pub mod reuse;
pub mod security;

use self::mir::{MirNodeRef, SqlToMirConverter};
use self::query_graph::{to_query_graph, QueryGraph};
use self::query_signature::Signature;
use self::reuse::{ReuseConfig, ReuseConfigType};
use ::mir::query::{MirQuery, QueryFlowParts};
use ::mir::reuse as mir_reuse;
use ::mir::Column;
use crate::controller::mir_to_flow::mir_query_to_flow_parts;
use crate::controller::Migration;
use dataflow::prelude::DataType;
use nom_sql::parser as sql_parser;
use nom_sql::{ArithmeticBase, CreateTableStatement, SqlQuery};
use nom_sql::{CompoundSelectOperator, CompoundSelectStatement, SelectStatement};
use petgraph::graph::NodeIndex;

use slog;
use std::collections::HashMap;
use std::str;
use std::vec::Vec;

pub type UniverseId = (DataType, Option<DataType>);

#[derive(Clone, Debug)]
enum QueryGraphReuse {
    ExactMatch(MirNodeRef),
    ExtendExisting(Vec<(u64, UniverseId)>),
    /// (node, columns to re-project if necessary, parameters)
    ReaderOntoExisting(MirNodeRef, Option<Vec<Column>>, Vec<Column>),
    None,
}

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the Soup graph `grap`.
/// The incorporator shares the lifetime of the flow graph it is associated with.
#[derive(Clone, Debug)]
pub struct SqlIncorporator {
    log: slog::Logger,
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<String, NodeIndex>,

    named_queries: HashMap<String, u64>,
    query_graphs: HashMap<u64, QueryGraph>,
    base_mir_queries: HashMap<String, MirQuery>,
    mir_queries: HashMap<(u64, UniverseId), MirQuery>,
    num_queries: usize,

    base_schemas: HashMap<String, CreateTableStatement>,
    view_schemas: HashMap<String, Vec<String>>,

    schema_version: usize,

    reuse_type: ReuseConfigType,

    /// Active universes mapped to the group they belong to.
    /// If an user universe, mapped to None.
    universes: HashMap<Option<DataType>, Vec<UniverseId>>,
}

impl Default for SqlIncorporator {
    fn default() -> Self {
        SqlIncorporator {
            log: slog::Logger::root(slog::Discard, o!()),
            mir_converter: SqlToMirConverter::default(),
            leaf_addresses: HashMap::default(),

            named_queries: HashMap::default(),
            query_graphs: HashMap::default(),
            base_mir_queries: HashMap::default(),
            mir_queries: HashMap::default(),
            num_queries: 0,

            base_schemas: HashMap::default(),
            view_schemas: HashMap::default(),

            schema_version: 0,

            reuse_type: ReuseConfigType::Finkelstein,
            universes: HashMap::default(),
        }
    }
}

impl SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    pub fn new(log: slog::Logger) -> Self {
        let lc = log.clone();
        SqlIncorporator {
            log: log,
            mir_converter: SqlToMirConverter::with_logger(lc),
            ..Default::default()
        }
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub fn disable_reuse(&mut self) {
        self.reuse_type = ReuseConfigType::NoReuse;
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.reuse_type = reuse_type;
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a string that holds a parameterized SQL query, and the `name` argument supplies
    /// an optional name for the query. If no `name` is specified, the table name is used in the
    /// case of CREATE TABLE queries, and a deterministic, unique name is generated and returned
    /// otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeIndex`es representing the nodes added to support the query.
    #[allow(unused)]
    pub fn add_query(
        &mut self,
        query: &str,
        name: Option<String>,
        mut mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        query.to_flow_parts(self, name, &mut mig)
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a `SqlQuery` structure, and the `name` argument supplies an optional name for
    /// the query. If no `name` is specified, the table name is used in the case of CREATE TABLE
    /// queries, and a deterministic, unique name is generated and returned otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeIndex`es representing the nodes added to support the query.
    pub fn add_parsed_query(
        &mut self,
        query: SqlQuery,
        name: Option<String>,
        is_leaf: bool,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        match name {
            None => self.nodes_for_query(query, is_leaf, mig),
            Some(n) => self.nodes_for_named_query(query, n, is_leaf, mig),
        }
    }

    pub fn get_base_schema(&self, name: &str) -> Option<CreateTableStatement> {
        self.base_schemas.get(name).cloned()
    }

    pub fn get_view_schema(&self, name: &str) -> Option<Vec<String>> {
        self.view_schemas.get(name).cloned()
    }

    #[cfg(test)]
    fn get_flow_node_address(&self, name: &str, v: usize) -> Option<NodeIndex> {
        self.mir_converter.get_flow_node_address(name, v)
    }

    /// Retrieves the flow node associated with a given query's leaf view.
    #[allow(unused)]
    pub fn get_query_address(&self, name: &str) -> Option<NodeIndex> {
        match self.leaf_addresses.get(name) {
            None => self.mir_converter.get_leaf(name),
            Some(na) => Some(na.clone()),
        }
    }

    pub fn is_leaf_address(&self, ni: NodeIndex) -> bool {
        self.leaf_addresses.values().any(|nn| *nn == ni)
    }

    pub fn get_queries_for_node(&self, ni: NodeIndex) -> Vec<String> {
        self.leaf_addresses
            .iter()
            .filter_map(|(name, idx)| if *idx == ni { Some(name.clone()) } else { None })
            .collect()
    }

    fn consider_query_graph(
        &mut self,
        query_name: &str,
        universe: UniverseId,
        st: &SelectStatement,
    ) -> (QueryGraph, QueryGraphReuse) {
        debug!(self.log, "Making QG for \"{}\"", query_name);
        trace!(self.log, "Query \"{}\": {:#?}", query_name, st);

        let mut qg = match to_query_graph(st) {
            Ok(qg) => qg,
            Err(e) => panic!(e),
        };

        trace!(self.log, "QG for \"{}\": {:#?}", query_name, qg);

        // if reuse is disabled, we're done
        if self.reuse_type == ReuseConfigType::NoReuse {
            return (qg, QueryGraphReuse::None);
        }

        // Do we already have this exact query or a subset of it in the same universe?
        // TODO(malte): make this an O(1) lookup by QG signature
        let qg_hash = qg.signature().hash;
        match self.mir_queries.get(&(qg_hash, universe.clone())) {
            None => (),
            Some(ref mir_query) => {
                let existing_qg = self
                    .query_graphs
                    .get(&qg_hash)
                    .expect("query graph should be present");
                // note that this also checks the *order* in which parameters are specified; a
                // different order means that we cannot simply reuse the existing reader.
                if existing_qg.signature() == qg.signature()
                    && existing_qg.parameters() == qg.parameters()
                    && existing_qg.exact_hash() == qg.exact_hash()
                {
                    // we already have this exact query, down to the exact same reader key columns
                    // in exactly the same order
                    info!(
                        self.log,
                        "An exact match for query \"{}\" already exists in universe \"{}\", reusing it",
                        query_name,
                        universe.0.to_string(),
                    );

                    trace!(
                        self.log,
                        "Reusing MirQuery {} with QueryGraph {:#?}",
                        mir_query.name,
                        existing_qg,
                    );

                    return (qg, QueryGraphReuse::ExactMatch(mir_query.leaf.clone()));
                } else if existing_qg.signature() == qg.signature()
                    && existing_qg.parameters() != qg.parameters()
                {
                    use self::query_graph::OutputColumn;

                    // the signatures match, but this comparison has only given us an inexact result:
                    // we know that both queries mention the same columns, but not that they
                    // actually do the same comparisons or have the same literals. Hence, we need
                    // to scan the predicates here and ensure that for each predicate in the
                    // incoming QG, we have a matching predicate in the existing one.
                    // Since `qg.relations.predicates` only contains comparisons between columns
                    // and literals (col/col is a join predicate and associated with the join edge,
                    // col/param is stored in qg.params), we will not be inhibited by the fact that
                    // the queries have different parameters.
                    let mut predicates_match = true;
                    for (r, n) in qg.relations.iter() {
                        for p in n.predicates.iter() {
                            if !existing_qg.relations.contains_key(r)
                                || !existing_qg.relations[r].predicates.contains(p)
                            {
                                predicates_match = false;
                            }
                        }
                    }

                    // if any of our columns are grouped expressions, we can't reuse here, since
                    // the difference in parameters means that there is a difference in the implied
                    // GROUP BY clause
                    let no_grouped_columns = qg.columns.iter().all(|c| match *c {
                        OutputColumn::Literal(_) => true,
                        OutputColumn::Arithmetic(ref ac) => {
                            let mut is_function = false;
                            if let ArithmeticBase::Column(ref c) = ac.expression.left {
                                is_function = is_function || c.function.is_some();
                            }

                            if let ArithmeticBase::Column(ref c) = ac.expression.right {
                                is_function = is_function || c.function.is_some();
                            }

                            !is_function
                        }
                        OutputColumn::Data(ref dc) => dc.function.is_none(),
                    });

                    if predicates_match && no_grouped_columns {
                        // QGs are identical, except for parameters (or their order)
                        info!(
                            self.log,
                            "Query '{}' has an exact match modulo parameters in {}, \
                             so making a new reader",
                            query_name,
                            mir_query.name,
                        );

                        // We want to hang the new leaf off the last non-leaf node of the query that
                        // has the parameter columns we need, so backtrack until we find this place.
                        // Typically, this unwinds only two steps, above the final projection.
                        // However, there might be cases in which a parameter column needed is not
                        // present in the query graph (because a later migration added the column to
                        // a base schema after the query was added to the graph). In this case, we
                        // move on to other reuse options.
                        let params = qg
                            .parameters()
                            .into_iter()
                            .map(|c| Column::from(c))
                            .collect();
                        match mir_reuse::rewind_until_columns_found(mir_query.leaf.clone(), &params)
                        {
                            Some(mn) => {
                                use ::mir::node::MirNodeType;
                                let project_columns = match mn.borrow().inner {
                                    MirNodeType::Project { .. } => None,
                                    _ => {
                                        // N.B.: we can't just add an identity here, since we might
                                        // have backtracked above a projection in order to get the
                                        // new parameter column(s). In this case, we need to add a
                                        // new projection that includes the same columns as the one
                                        // for the existing query, but also additional parameter
                                        // columns. The latter get added later; here we simply
                                        // extract the columns that need reprojecting and pass them
                                        // along with the reuse instruction.
                                        let existing_projection = mir_query
                                            .leaf
                                            .borrow()
                                            .ancestors()
                                            .iter()
                                            .next()
                                            .unwrap()
                                            .clone();
                                        let project_columns = existing_projection
                                            .borrow()
                                            .columns()
                                            .into_iter()
                                            .cloned()
                                            .collect();
                                        Some(project_columns)
                                    }
                                };
                                return (
                                    qg,
                                    QueryGraphReuse::ReaderOntoExisting(
                                        mn,
                                        project_columns,
                                        params,
                                    ),
                                );
                            }
                            None => (),
                        }
                    }
                }
            }
        }

        let reuse_config = ReuseConfig::new(self.reuse_type.clone());

        // Find a promising set of query graphs
        let reuse_candidates = reuse_config.reuse_candidates(&mut qg, &self.query_graphs);

        if reuse_candidates.len() > 0 {
            info!(
                self.log,
                "Identified {} candidate QGs for reuse",
                reuse_candidates.len()
            );
            trace!(
                self.log,
                "This QG: {:#?}\nReuse candidates:\n{:#?}",
                qg,
                reuse_candidates
            );

            let mut mir_queries = Vec::new();
            for uid in reuse_config.reuse_universes(universe, &self.universes) {
                let mqs: Vec<_> = reuse_candidates
                    .iter()
                    .map(|c| {
                        let sig = (c.1).0;
                        (sig, uid.clone())
                    })
                    .collect();

                mir_queries.extend(mqs);
            }

            return (qg, QueryGraphReuse::ExtendExisting(mir_queries));
        } else {
            info!(self.log, "No reuse opportunity, adding fresh query");
        }

        (qg, QueryGraphReuse::None)
    }

    fn add_leaf_to_existing_query(
        &mut self,
        query_name: &str,
        params: &Vec<Column>,
        final_query_node: MirNodeRef,
        project_columns: Option<Vec<Column>>,
        mut mig: &mut Migration,
    ) -> QueryFlowParts {
        trace!(self.log, "Adding a new leaf below: {:?}", final_query_node);

        let mut mir = self.mir_converter.add_leaf_below(
            final_query_node,
            query_name,
            params,
            project_columns,
        );

        trace!(self.log, "Reused leaf node MIR: {}", mir);

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`.
        // Note that we don't need to optimize the MIR here, because the query is trivial.
        let qfp = mir_query_to_flow_parts(&mut mir, &mut mig, None);

        self.register_query(query_name, None, &mir, mig.universe());

        qfp
    }

    fn add_base_via_mir(
        &mut self,
        query_name: &str,
        query: &SqlQuery,
        mut mig: &mut Migration,
    ) -> QueryFlowParts {
        // first, compute the MIR representation of the SQL query
        let mut mir = self.mir_converter.named_base_to_mir(query_name, query);

        trace!(self.log, "Base node MIR: {:#?}", mir);

        // no optimization, because standalone base nodes can't be optimized

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir_query_to_flow_parts(&mut mir, &mut mig, None);

        // remember the schema in case we need it later
        // on base table schema change, we will overwrite the existing schema here.
        // TODO(malte): this means that requests for this will always return the *latest* schema
        // for a base.
        if let SqlQuery::CreateTable(ref ctq) = query {
            self.base_schemas.insert(query_name.to_owned(), ctq.clone());
        } else {
            unimplemented!();
        }

        self.register_query(query_name, None, &mir, mig.universe());

        qfp
    }

    fn add_compound_query(
        &mut self,
        query_name: &str,
        query: &CompoundSelectStatement,
        is_leaf: bool,
        mut mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        let subqueries: Result<Vec<_>, String> = query
            .selects
            .iter()
            .enumerate()
            .map(|(i, sq)| {
                Ok(self
                    .add_select_query(&format!("{}_csq_{}", query_name, i), &sq.1, false, mig)?
                    .1
                    .unwrap())
            })
            .collect();

        let mut combined_mir_query = self.mir_converter.compound_query_to_mir(
            query_name,
            subqueries?.iter().collect(),
            CompoundSelectOperator::Union,
            &query.order,
            &query.limit,
            is_leaf,
        );

        let qfp = mir_query_to_flow_parts(&mut combined_mir_query, &mut mig, None);

        self.register_query(query_name, None, &combined_mir_query, mig.universe());

        Ok(qfp)
    }

    /// Returns tuple of `QueryFlowParts` and an optional new `MirQuery`. The latter is only
    /// present if a new `MirQuery` was added.
    fn add_select_query(
        &mut self,
        query_name: &str,
        sq: &SelectStatement,
        is_leaf: bool,
        mig: &mut Migration,
    ) -> Result<(QueryFlowParts, Option<MirQuery>), String> {
        let (qg, reuse) = self.consider_query_graph(&query_name, mig.universe(), sq);
        Ok(match reuse {
            QueryGraphReuse::ExactMatch(mn) => {
                let flow_node = mn.borrow().flow_node.as_ref().unwrap().address();
                let qfp = QueryFlowParts {
                    name: String::from(query_name),
                    new_nodes: vec![],
                    reused_nodes: vec![flow_node],
                    query_leaf: flow_node,
                };
                (qfp, None)
            }
            QueryGraphReuse::ExtendExisting(mqs) => {
                let qfp = self.extend_existing_query(&query_name, sq, qg, mqs, is_leaf, mig)?;
                (qfp, None)
            }
            QueryGraphReuse::ReaderOntoExisting(mn, project_columns, params) => {
                let qfp =
                    self.add_leaf_to_existing_query(&query_name, &params, mn, project_columns, mig);
                (qfp, None)
            }
            QueryGraphReuse::None => {
                let (qfp, mir) = self.add_query_via_mir(&query_name, sq, qg, is_leaf, mig)?;
                (qfp, Some(mir))
            }
        })
    }

    fn add_query_via_mir(
        &mut self,
        query_name: &str,
        query: &SelectStatement,
        qg: QueryGraph,
        is_leaf: bool,
        mut mig: &mut Migration,
    ) -> Result<(QueryFlowParts, MirQuery), String> {
        use ::mir::visualize::GraphViz;
        let universe = mig.universe();
        // no QG-level reuse possible, so we'll build a new query.
        // first, compute the MIR representation of the SQL query
        let (sec, og_mir, table_mapping, base_name) = self.mir_converter.named_query_to_mir(
            query_name,
            query,
            &qg,
            is_leaf,
            universe.clone(),
        )?;

        trace!(self.log, "Unoptimized MIR:\n{}", og_mir.to_graphviz().unwrap());

        // run MIR-level optimizations
        let mut mir = og_mir.optimize(table_mapping.as_ref(), sec);

        trace!(self.log, "Optimized MIR:\n{}", mir.to_graphviz().unwrap());

        if sec {
            match table_mapping {
                Some(ref x) => {
                    mir = mir.make_universe_naming_consistent(x, base_name);
                }
                None => {
                    panic!("Missing table mapping when reconciling universe table names!");
                }
            }
        }

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir_query_to_flow_parts(&mut mir, &mut mig, None);

        // register local state
        self.register_query(query_name, Some(qg), &mir, universe);

        Ok((qfp, mir))
    }

    pub fn remove_query(&mut self, query_name: &str, mig: &Migration) -> Option<NodeIndex> {
        let nodeid = self
            .leaf_addresses
            .remove(query_name)
            .expect("tried to remove unknown query");

        let qg_hash = self.named_queries.remove(query_name).expect(&format!(
            "missing query hash for named query \"{}\"",
            query_name
        ));
        let mir = self.mir_queries.get(&(qg_hash, mig.universe())).unwrap();

        // traverse self.leaf__addresses
        if self
            .leaf_addresses
            .values()
            .find(|&id| *id == nodeid)
            .is_none()
        {
            // ok to remove

            // remove local state for query

            // traverse and remove MIR nodes
            // TODO(malte): implement this
            self.mir_converter.remove_query(query_name, mir);

            // clean up local state
            self.mir_queries.remove(&(qg_hash, mig.universe())).unwrap();
            self.query_graphs.remove(&qg_hash).unwrap();
            self.view_schemas.remove(query_name).unwrap();

            // trigger reader node removal
            Some(nodeid)
        } else {
            // more than one query uses this leaf
            // don't remove node yet!

            // TODO(malte): implement this
            self.mir_converter.remove_query(query_name, mir);

            // clean up state for this query
            self.mir_queries.remove(&(qg_hash, mig.universe())).unwrap();
            self.query_graphs.remove(&qg_hash).unwrap();
            self.view_schemas.remove(query_name).unwrap();

            None
        }
    }

    pub fn remove_base(&mut self, name: &str) {
        info!(self.log, "Removing base {} from SqlIncorporator", name);
        if self.base_schemas.remove(name).is_none() {
            warn!(
                self.log,
                "Attempted to remove non-existant base node {} from SqlIncorporator", name
            );
        }

        let mir = self
            .base_mir_queries
            .get(name)
            .expect(&format!("tried to remove unknown base {}", name));
        self.mir_converter.remove_base(name, mir)
    }

    fn register_query(
        &mut self,
        query_name: &str,
        qg: Option<QueryGraph>,
        mir: &MirQuery,
        universe: UniverseId,
    ) {
        // TODO(malte): we currently need to remember these for local state, but should figure out
        // a better plan (see below)
        let fields = mir
            .leaf
            .borrow()
            .columns()
            .into_iter()
            .map(|c| String::from(c.name.as_str()))
            .collect::<Vec<_>>();

        // TODO(malte): get rid of duplication and figure out where to track this state
        debug!(self.log, "registering query \"{}\"", query_name);
        self.view_schemas.insert(String::from(query_name), fields);

        // We made a new query, so store the query graph and the corresponding leaf MIR node.
        // TODO(malte): we currently store nothing if there is no QG (e.g., for compound queries).
        // This means we cannot reuse these queries.
        match qg {
            Some(qg) => {
                let qg_hash = qg.signature().hash;
                self.query_graphs.insert(qg_hash, qg);
                self.mir_queries.insert((qg_hash, universe), mir.clone());
                self.named_queries.insert(query_name.to_owned(), qg_hash);
            }
            None => {
                self.base_mir_queries
                    .insert(query_name.to_owned(), mir.clone());
            }
        }
    }

    fn extend_existing_query(
        &mut self,
        query_name: &str,
        query: &SelectStatement,
        qg: QueryGraph,
        reuse_mirs: Vec<(u64, UniverseId)>,
        is_leaf: bool,
        mut mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        use ::mir::reuse::merge_mir_for_queries;
        use ::mir::visualize::GraphViz;
        let universe = mig.universe();

        // no QG-level reuse possible, so we'll build a new query.
        // first, compute the MIR representation of the SQL query
        let (sec, new_query_mir, table_mapping, base_name) = self
            .mir_converter
            .named_query_to_mir(query_name, query, &qg, is_leaf, universe.clone())?;

        trace!(
            self.log,
            "Original MIR:\n{}",
            new_query_mir.to_graphviz().unwrap()
        );

        let new_opt_mir = new_query_mir.optimize(table_mapping.as_ref(), sec);

        trace!(
            self.log,
            "Optimized MIR:\n{}",
            new_opt_mir.to_graphviz().unwrap()
        );

        // compare to existing query MIR and reuse prefix
        let mut reused_mir = new_opt_mir.clone();
        let mut num_reused_nodes = 0;
        for m in reuse_mirs {
            if !self.mir_queries.contains_key(&m) {
                continue;
            }
            let mq = self.mir_queries.get(&m).unwrap();
            let res = merge_mir_for_queries(&self.log, &reused_mir, &mq);
            reused_mir = res.0;
            if res.1 > num_reused_nodes {
                num_reused_nodes = res.1;
            }
        }

        let mut post_reuse_opt_mir = reused_mir.optimize_post_reuse();

        // traverse universe subgraph and update table names for
        // internal consistency using the table mapping as guidance
        if sec {
            match table_mapping {
                Some(ref x) => {
                    post_reuse_opt_mir =
                        post_reuse_opt_mir.make_universe_naming_consistent(x, base_name);
                }
                None => {
                    panic!("Missing table mapping when reconciling universe table names!");
                }
            }
        }

        trace!(
            self.log,
            "Post-reuse optimized MIR:\n{}",
            post_reuse_opt_mir.to_graphviz().unwrap()
        );

        let qfp =
            mir_query_to_flow_parts(&mut post_reuse_opt_mir, &mut mig, table_mapping.as_ref());

        info!(
            self.log,
            "Reused {} nodes for {}", num_reused_nodes, query_name
        );

        // register local state
        self.register_query(query_name, Some(qg), &post_reuse_opt_mir, universe);

        Ok(qfp)
    }

    fn nodes_for_query(
        &mut self,
        q: SqlQuery,
        is_leaf: bool,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        let name = match q {
            SqlQuery::CreateTable(ref ctq) => ctq.table.name.clone(),
            SqlQuery::CreateView(ref cvq) => cvq.name.clone(),
            SqlQuery::Select(_) | SqlQuery::CompoundSelect(_) => format!("q_{}", self.num_queries),
            _ => panic!("only CREATE TABLE and SELECT queries can be added to the graph!"),
        };
        self.nodes_for_named_query(q, name, is_leaf, mig)
    }

    /// Runs some standard rewrite passes on the query.
    fn rewrite_query(&mut self, q: SqlQuery, mig: &mut Migration) -> Result<SqlQuery, String> {
        // TODO: make this not take &mut self

        use crate::controller::sql::passes::alias_removal::AliasRemoval;
        use crate::controller::sql::passes::count_star_rewrite::CountStarRewrite;
        use crate::controller::sql::passes::implied_tables::ImpliedTableExpansion;
        use crate::controller::sql::passes::key_def_coalescing::KeyDefinitionCoalescing;
        use crate::controller::sql::passes::negation_removal::NegationRemoval;
        use crate::controller::sql::passes::star_expansion::StarExpansion;
        use crate::controller::sql::passes::subqueries::SubQueries;
        use crate::controller::sql::query_utils::ReferredTables;

        // need to increment here so that each subquery has a unique name.
        // (subqueries call recursively into `nodes_for_named_query` via `add_parsed_query` below,
        // so we will end up incrementing this for every subquery.
        self.num_queries += 1;

        // flattens out the query by replacing subqueries for references
        // to existing views in the graph
        let mut fq = q.clone();
        for sq in fq.extract_subqueries() {
            use self::passes::subqueries::{
                field_with_table_name, query_from_condition_base, Subquery,
            };
            use nom_sql::{JoinRightSide, Table};
            match sq {
                Subquery::InComparison(cond_base) => {
                    let (sq, column) = query_from_condition_base(&cond_base);

                    let qfp = self
                        .add_parsed_query(sq, None, false, mig)
                        .expect("failed to add subquery");
                    *cond_base = field_with_table_name(qfp.name.clone(), column);
                }
                Subquery::InJoin(join_right_side) => {
                    *join_right_side = match *join_right_side {
                        JoinRightSide::NestedSelect(box ref ns, ref alias) => {
                            let qfp = self
                                .add_parsed_query(
                                    SqlQuery::Select(ns.clone()),
                                    alias.clone(),
                                    false,
                                    mig,
                                )
                                .expect("failed to add subquery in join");
                            JoinRightSide::Table(Table {
                                name: qfp.name.clone(),
                                alias: None,
                            })
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }

        // Check that all tables mentioned in the query exist.
        // This must happen before the rewrite passes are applied because some of them rely on
        // having the table schema available in `self.view_schemas`.
        match fq {
            // if we're just about to create the table, we don't need to check if it exists. If it
            // does, we will amend or reuse it; if it does not, we create it.
            SqlQuery::CreateTable(_) => (),
            SqlQuery::CreateView(_) => (),
            // other kinds of queries *do* require their referred tables to exist!
            ref q @ SqlQuery::CompoundSelect(_)
            | ref q @ SqlQuery::Select(_)
            | ref q @ SqlQuery::Set(_)
            | ref q @ SqlQuery::Update(_)
            | ref q @ SqlQuery::Delete(_)
            | ref q @ SqlQuery::DropTable(_)
            | ref q @ SqlQuery::Insert(_) => {
                for t in &q.referred_tables() {
                    if !self.view_schemas.contains_key(&t.name) {
                        return Err(format!("query refers to unknown table \"{}\"", t.name));
                    }
                }
            }
        }

        // Run some standard rewrite passes on the query. This makes the later work easier,
        // as we no longer have to consider complications like aliases.
        Ok(fq
            .expand_table_aliases(mig.context())
            .remove_negation()
            .coalesce_key_definitions()
            .expand_stars(&self.view_schemas)
            .expand_implied_tables(&self.view_schemas)
            .rewrite_count_star(&self.view_schemas))
    }

    fn nodes_for_named_query(
        &mut self,
        q: SqlQuery,
        query_name: String,
        is_leaf: bool,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        // short-circuit if we're dealing with a CreateView query; this avoids having to deal with
        // CreateView in all of our rewrite passes.
        if let SqlQuery::CreateView(cvq) = q {
            use nom_sql::SelectSpecification;
            let name = cvq.name.clone();
            match *cvq.definition {
                SelectSpecification::Compound(csq) => {
                    return self.nodes_for_named_query(
                        SqlQuery::CompoundSelect(csq),
                        name,
                        is_leaf,
                        mig,
                    )
                }
                SelectSpecification::Simple(sq) => {
                    return self.nodes_for_named_query(SqlQuery::Select(sq), name, is_leaf, mig)
                }
            }
        };

        let q = self.rewrite_query(q, mig)?;

        // TODO(larat): extend existing should handle policy nodes
        // if this is a selection, we compute its `QueryGraph` and consider the existing ones we
        // hold for reuse or extension
        let qfp = match q {
            SqlQuery::CompoundSelect(csq) => {
                // NOTE(malte): We can't currently reuse complete compound select queries, since
                // our reuse logic operates on `SqlQuery` structures. Their subqueries do get
                // reused, however.
                self.add_compound_query(&query_name, &csq, is_leaf, mig)
                    .unwrap()
            }
            SqlQuery::Select(sq) => self.add_select_query(&query_name, &sq, is_leaf, mig)?.0,
            ref q @ SqlQuery::CreateTable { .. } => self.add_base_via_mir(&query_name, &q, mig),
            ref q @ _ => panic!("unhandled query type in recipe: {:?}", q),
        };

        // record info about query
        self.leaf_addresses
            .insert(String::from(query_name.as_str()), qfp.query_leaf);

        Ok(qfp)
    }

    /// Upgrades the schema version that any nodes created for queries will be tagged with.
    /// `new_version` must be strictly greater than the current version in `self.schema_version`.
    pub fn upgrade_schema(&mut self, new_version: usize) {
        assert!(new_version > self.schema_version);
        info!(
            self.log,
            "Schema version advanced from {} to {}", self.schema_version, new_version
        );
        self.schema_version = new_version;
        self.mir_converter.upgrade_schema(new_version);
    }
}

/// Enables incorporation of a textual SQL query into a Soup graph.
pub trait ToFlowParts {
    /// Turn a SQL query into a set of nodes inserted into the Soup graph managed by
    /// the `SqlIncorporator` in the second argument. The query can optionally be named by the
    /// string in the `Option<String>` in the third argument.
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<String>,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<String>,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        self.as_str().to_flow_parts(inc, name, mig)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<String>,
        mig: &mut Migration,
    ) -> Result<QueryFlowParts, String> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(self);

        // if ok, manufacture a node for the query structure we got
        match parsed_query {
            Ok(q) => inc.add_parsed_query(q, name, true, mig),
            Err(e) => Err(String::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SqlIncorporator, ToFlowParts};
    use crate::controller::Migration;
    use crate::integration;
    use dataflow::prelude::*;
    use nom_sql::Column;
    use nom_sql::FunctionExpression;

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration, name: &str) -> &'a Node {
        let na = inc
            .get_flow_node_address(name, 0)
            .expect(&format!("No node named \"{}\" exists", name));
        mig.graph().node_weight(na).unwrap()
    }

    /// Helper to compute a query ID hash via the same method as in `QueryGraph::signature()`.
    /// Note that the argument slices must be ordered in the same way as &str and &Column are
    /// ordered by `Ord`.
    fn query_id_hash(relations: &[&str], attrs: &[&Column], columns: &[&Column]) -> u64 {
        use crate::controller::sql::query_graph::OutputColumn;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for r in relations.iter() {
            r.hash(&mut hasher);
        }
        for a in attrs.iter() {
            a.hash(&mut hasher);
        }
        for c in columns.iter() {
            OutputColumn::Data((*c).clone()).hash(&mut hasher);
        }
        hasher.finish()
    }

    #[test]
    fn it_parses() {
        // set up graph
        let mut g = integration::build_local("it_parses");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Must have a base node for type inference to work, so make one manually
            assert!(
                "CREATE TABLE users (id int, name varchar(40));"
                    .to_flow_parts(&mut inc, None, mig)
                    .is_ok()
            );

            // Should have two nodes: source and "users" base table
            let ncount = mig.graph().node_count();
            assert_eq!(ncount, 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");

            assert!(
                "SELECT users.id from users;"
                    .to_flow_parts(&mut inc, None, mig)
                    .is_ok()
            );
            // Should now have source, "users", a leaf projection node for the new selection, and
            // reader replicas
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 1);

            // Invalid query should fail parsing and add no nodes
            assert!(
                "foo bar from whatever;"
                    .to_flow_parts(&mut inc, None, mig)
                    .is_err()
            );
            // Should still only have source, "users" and the nodes for the above selection
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 1);
        });
    }

    #[test]
    fn it_incorporates_simple_join() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_simple_join");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type for "users"
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Establish a base write type for "articles"
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 3);
            assert_eq!(get_node(&inc, mig, "articles").name(), "articles");
            assert_eq!(
                get_node(&inc, mig, "articles").fields(),
                &["id", "author", "title"]
            );
            assert!(get_node(&inc, mig, "articles").is_base());

            // Try a simple equi-JOIN query
            let q = "SELECT users.name, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "users"],
                &[&Column::from("articles.author"), &Column::from("users.id")],
                &[&Column::from("users.name"), &Column::from("articles.title")],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(new_leaf_view.fields(), &["name", "title", "bogokey"]);
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_simple_selection");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Try a simple query
            let res = inc.add_query(
                "SELECT users.name FROM users WHERE users.id = 42;",
                None,
                mig,
            );
            assert!(res.is_ok());

            let qid = query_id_hash(
                &["users"],
                &[&Column::from("users.id")],
                &[&Column::from("users.name")],
            );
            // filter node
            let filter = get_node(&inc, mig, &format!("q_{:x}_n0_p0_f0", qid));
            assert_eq!(filter.fields(), &["id", "name"]);
            assert_eq!(filter.description(true), "σ[f0 = 42]");
            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["name", "bogokey"]);
            assert_eq!(edge.description(true), "π[1, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_aggregation() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_aggregation");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write types
            assert!(
                inc.add_query("CREATE TABLE votes (aid int, userid int);", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["aid", "userid"]);
            assert!(get_node(&inc, mig, "votes").is_base());

            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT COUNT(votes.userid) AS votes \
                 FROM votes GROUP BY votes.aid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation and the edge view, and reader replicas
            assert_eq!(mig.graph().node_count(), integration::DEFAULT_REPLICAS + 4);
            // check aggregation view
            let f = Box::new(FunctionExpression::Count(
                Column::from("votes.userid"),
                false,
            ));
            let qid = query_id_hash(
                &["computed_columns", "votes"],
                &[&Column::from("votes.aid")],
                &[&Column {
                    name: String::from("votes"),
                    alias: Some(String::from("votes")),
                    table: None,
                    function: Some(f),
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["aid", "votes"]);
            assert_eq!(agg_view.description(true), "|*| γ[0]");
            // check edge view
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["votes", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        });
    }

    #[test]
    fn it_does_not_reuse_if_disabled() {
        // set up graph
        let mut g = integration::build_local("it_does_not_reuse_if_disabled");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.disable_reuse();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add the same query again; this should NOT reuse here.
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT name, id FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            // should have added nodes for this query, too
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 2);
            // expect 3+ new nodes: filter, project, reader replicas
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 2);
            // should have ended up with a different leaf node
            assert_ne!(qfp.query_leaf, leaf);
        });
    }

    #[test]
    fn it_reuses_identical_query() {
        // set up graph
        let mut g = integration::build_local("it_reuses_identical_query");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add the same query again
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            // should have added no more nodes
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes, vec![]);
            assert_eq!(mig.graph().node_count(), ncount);
            // should have ended up with the same leaf node
            assert_eq!(qfp.query_leaf, leaf);

            // Add the same query again, but project columns in a different order
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT name, id FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            // should have added 2+ more nodes (project and reader replicas)
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 1);
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
        });
    }

    #[test]
    fn it_reuses_with_different_parameter() {
        // set up graph
        let mut g = integration::build_local("it_reuses_with_different_parameter");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(
                get_node(&inc, mig, "users").fields(),
                &["id", "name", "address"]
            );
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = ?;", None, mig);
            assert!(res.is_ok());

            // Add the same query again, but with a parameter on a different column.
            // Project the same columns, so we can reuse the projection that already exists and only
            // add an identity node.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // should have added 2+ more nodes: one identity node and reader replicas
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 1);
            // only the identity node is returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 1);
            assert_eq!(get_node(&inc, mig, &qfp.name).description(true), "≡");
            // we should be based off the identity as our leaf
            let id_node = qfp.new_nodes.iter().next().unwrap();
            assert_eq!(qfp.query_leaf, *id_node);

            // Do it again with a parameter on yet a different column.
            // Project different columns, so we need to add a new projection (not an identity).
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.address = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // should have added 2+ more nodes: one projection node and reader replicas
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 1);
            // only the projection node is returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 1);
            assert_eq!(
                get_node(&inc, mig, &qfp.name).description(true),
                "π[0, 1, 2]"
            );
            // we should be based off the new projection as our leaf
            let id_node = qfp.new_nodes.iter().next().unwrap();
            assert_eq!(qfp.query_leaf, *id_node);
        });
    }

    #[test]
    fn it_reuses_by_extending_existing_query() {
        use super::sql_parser;
        // set up graph
        let mut g = integration::build_local("it_reuses_by_extending_existing_query");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Add base tables
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (id int, title varchar(40));",
                    None,
                    mig
                )
                .is_ok()
            );
            assert!(
                inc.add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                    .is_ok()
            );
            // Should have source, "articles" and "votes" base tables
            assert_eq!(mig.graph().node_count(), 3);

            // Add a new query
            let res = inc.add_parsed_query(
                sql_parser::parse_query("SELECT COUNT(uid) AS vc FROM votes GROUP BY aid;")
                    .unwrap(),
                Some("votecount".into()),
                false,
                mig,
            );
            assert!(res.is_ok());

            // Add a query that can reuse votecount by extending it.
            let ncount = mig.graph().node_count();
            let res = inc.add_parsed_query(
                sql_parser::parse_query(
                    "SELECT COUNT(uid) AS vc FROM votes WHERE vc > 5 GROUP BY aid;",
                )
                .unwrap(),
                Some("highvotes".into()),
                true,
                mig,
            );
            assert!(res.is_ok());
            // should have added 3+ more nodes: a join, a projection, and reader replicas
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 2);
            // only the join and projection nodes are returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 2);
        });
    }

    #[test]
    fn it_incorporates_aggregation_no_group_by() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_aggregation_no_group_by");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query("CREATE TABLE votes (aid int, userid int);", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["aid", "userid"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function without a GROUP BY clause
            let res = inc.add_query("SELECT COUNT(votes.userid) AS count FROM votes;", None, mig);
            assert!(res.is_ok());
            // added the aggregation, a project helper, the edge view, and reader replicas
            assert_eq!(mig.graph().node_count(), integration::DEFAULT_REPLICAS + 5);
            // check project helper node
            let f = Box::new(FunctionExpression::Count(
                Column::from("votes.userid"),
                false,
            ));
            let qid = query_id_hash(
                &["computed_columns", "votes"],
                &[],
                &[&Column {
                    name: String::from("count"),
                    alias: Some(String::from("count")),
                    table: None,
                    function: Some(f),
                }],
            );
            let proj_helper_view = get_node(&inc, mig, &format!("q_{:x}_n0_prj_hlpr", qid));
            assert_eq!(proj_helper_view.fields(), &["userid", "grp"]);
            assert_eq!(proj_helper_view.description(true), "π[1, lit: 0]");
            // check aggregation view
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["grp", "count"]);
            assert_eq!(agg_view.description(true), "|*| γ[1]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["count", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_aggregation_count_star() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_aggregation_count_star");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query("CREATE TABLE votes (userid int, aid int);", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["userid", "aid"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function without a GROUP BY clause
            let res = inc.add_query(
                "SELECT COUNT(*) AS count FROM votes GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation, a project helper, the edge view, and reader replicas
            assert_eq!(mig.graph().node_count(), integration::DEFAULT_REPLICAS + 4);
            // check aggregation view
            let f = Box::new(FunctionExpression::Count(Column::from("votes.aid"), false));
            let qid = query_id_hash(
                &["computed_columns", "votes"],
                &[&Column::from("votes.userid")],
                &[&Column {
                    name: String::from("count"),
                    alias: Some(String::from("count")),
                    table: None,
                    function: Some(f),
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["userid", "count"]);
            assert_eq!(agg_view.description(true), "|*| γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["count", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_explicit_multi_join() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_explicit_multi_join");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (aid int, title varchar(255), author int);",
                    None,
                    mig
                )
                .is_ok()
            );

            // Try an explicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles
                 JOIN users ON (users.id = articles.author) \
                 JOIN votes ON (votes.aid = articles.aid);";

            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let _qid = query_id_hash(
                &["articles", "users", "votes"],
                &[
                    &Column::from("articles.aid"),
                    &Column::from("articles.author"),
                    &Column::from("users.id"),
                    &Column::from("votes.aid"),
                ],
                &[
                    &Column::from("users.name"),
                    &Column::from("articles.title"),
                    &Column::from("votes.uid"),
                ],
            );
            // XXX(malte): non-deterministic join ordering make it difficult to assert on the join
            // views
            // leaf view
            let leaf_view = get_node(&inc, mig, "q_3");
            assert_eq!(leaf_view.fields(), &["name", "title", "uid", "bogokey"]);
        });
    }

    #[test]
    fn it_incorporates_implicit_multi_join() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_implicit_multi_join");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (aid int, title varchar(255), author int);",
                    None,
                    mig
                )
                .is_ok()
            );

            // Try an implicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles, users, votes
                 WHERE users.id = articles.author \
                 AND votes.aid = articles.aid;";

            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            // XXX(malte): below over-projects into the final leaf, and is thus inconsistent
            // with the explicit JOIN case!
            let qid = query_id_hash(
                &["articles", "users", "votes"],
                &[
                    &Column::from("articles.aid"),
                    &Column::from("articles.author"),
                    &Column::from("users.id"),
                    &Column::from("votes.aid"),
                ],
                &[
                    &Column::from("users.name"),
                    &Column::from("articles.title"),
                    &Column::from("votes.uid"),
                ],
            );
            let join1_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            // articles join users
            assert_eq!(join1_view.fields(), &["aid", "title", "author", "name"]);
            let join2_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            // join1_view join vptes
            assert_eq!(
                join2_view.fields(),
                &["aid", "title", "author", "name", "uid"]
            );
            // leaf view
            let leaf_view = get_node(&inc, mig, "q_3");
            assert_eq!(leaf_view.fields(), &["name", "title", "uid", "bogokey"]);
        });
    }

    #[test]
    #[allow_fail]
    fn it_incorporates_join_projecting_join_columns() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_join_projecting_join_columns");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok()
            );
            let q = "SELECT users.id, users.name, articles.author, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "users"],
                &[&Column::from("articles.author"), &Column::from("users.id")],
                &[
                    &Column::from("users.id"),
                    &Column::from("users.name"),
                    &Column::from("articles.author"),
                    &Column::from("articles.title"),
                ],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(
                new_leaf_view.fields(),
                &["id", "name", "author", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[1, 3, 1, 2, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_self_join() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_self_join");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE friends (id int, friend int);", None, mig)
                    .is_ok()
            );

            // Try a friends-of-friends type computation via self join
            let q = "SELECT f1.id, f2.friend AS fof \
                     FROM friends AS f1 \
                     JOIN (SELECT * FROM friends) AS f2 ON (f1.friend = f2.id)
                     WHERE f1.id = ?;";

            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let leaf_view = get_node(&inc, mig, "q_1");
            assert_eq!(leaf_view.fields(), &["id", "fof"]);
        });
    }

    #[test]
    fn it_incorporates_literal_projection() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_literal_projection");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );

            let res = inc.add_query("SELECT users.name, 1 FROM users;", None, mig);
            assert!(res.is_ok());

            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["name", "1", "bogokey"]);
            assert_eq!(edge.description(true), "π[1, lit: 1, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_arithmetic_projection() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_arithmetic_projection");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, age int);", None, mig)
                    .is_ok()
            );

            let res = inc.add_query(
                "SELECT 2 * users.age, 2 * 10 as twenty FROM users;",
                None,
                mig,
            );
            assert!(res.is_ok());

            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["2 * users.age", "twenty", "bogokey"]);
            assert_eq!(
                edge.description(true),
                "π[(lit: 2) * 1, (lit: 2) * (lit: 10), lit: 0]"
            );
        });
    }

    #[test]
    fn it_incorporates_join_with_nested_query() {
        let mut g = integration::build_local("it_incorporates_join_with_nested_query");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            assert!(
                inc.add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok()
            );

            let q = "SELECT nested_users.name, articles.title \
                     FROM articles \
                     JOIN (SELECT * FROM users) AS nested_users \
                     ON (nested_users.id = articles.author);";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "nested_users"],
                &[
                    &Column::from("articles.author"),
                    &Column::from("nested_users.id"),
                ],
                &[
                    &Column::from("nested_users.name"),
                    &Column::from("articles.title"),
                ],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(new_leaf_view.fields(), &["name", "title", "bogokey"]);
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        });
    }

    #[test]
    fn it_incorporates_compound_selection() {
        // set up graph
        let mut g = integration::build_local("it_incorporates_compound_selection");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );

            let res = inc.add_query(
                "SELECT users.id, users.name FROM users \
                 WHERE users.id = 32 \
                 UNION \
                 SELECT users.id, users.name FROM users \
                 WHERE users.id = 42 AND users.name = 'bob';",
                None,
                mig,
            );
            assert!(res.is_ok());

            // the leaf of this query (node above the reader) is a union
            let union_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(union_view.fields(), &["id", "name"]);
            assert_eq!(union_view.description(true), "3:[0, 1] ⋃ 6:[0, 1]");
        });
    }

    #[test]
    fn it_distinguishes_predicates() {
        // set up graph
        let mut g = integration::build_local("it_distinguishes_predicates");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add query with a different predicate
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 50;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // should have added 3+ more nodes (filter, project and reader replicas)
            assert_eq!(mig.graph().node_count(), ncount + integration::DEFAULT_REPLICAS + 2);
        });
    }

    #[test]
    #[ignore]
    fn it_queries_over_aliased_view() {
        let mut g = integration::build_local("it_queries_over_aliased_view");
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(
                inc.add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                    .is_ok()
            );
            // Add first copy of new query, called "tq1"
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.id = 42;",
                Some("tq1".into()),
                mig,
            );
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add the same query again, this time as "tq2"
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.id = 42;",
                Some("tq2".into()),
                mig,
            );
            assert!(res.is_ok());
            // should have added no more nodes
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes, vec![]);
            assert_eq!(mig.graph().node_count(), ncount);
            // should have ended up with the same leaf node
            assert_eq!(qfp.query_leaf, leaf);

            // Add a query over tq2, which really is tq1
            let _res = inc.add_query("SELECT tq2.id FROM tq2;", Some("over_tq2".into()), mig);
            // should have added a projection and a reader
            assert_eq!(mig.graph().node_count(), ncount + 2);
        });
    }
}
