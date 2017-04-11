use petgraph;
use petgraph::graph::NodeIndex;
use checktable;
use ops::base::Base;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::time;
use std::thread;
use std::io;

use slog;

pub mod domain;
pub mod prelude;
pub mod node;
pub mod payload;
pub mod statistics;
pub mod keys;
pub mod core;
mod migrate;
mod transactions;
mod hook;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

pub type Edge = bool; // should the edge be materialized?

/// A `Mutator` is used to perform reads and writes to base nodes.
pub struct Mutator {
    src: core::NodeAddress,
    tx: mpsc::SyncSender<payload::Packet>,
    addr: core::NodeAddress,
    primary_key: Vec<usize>,
    tx_reply_channel: (mpsc::Sender<Result<i64, ()>>, mpsc::Receiver<Result<i64, ()>>),
}

impl Clone for Mutator {
    fn clone(&self) -> Self {
        Self {
            src: self.src.clone(),
            tx: self.tx.clone(),
            addr: self.addr.clone(),
            primary_key: self.primary_key.clone(),
            tx_reply_channel: mpsc::channel(),
        }
    }
}

impl Mutator {
    fn send(&self, r: prelude::Records) {
        let m = payload::Packet::Message {
            link: payload::Link::new(self.src, self.addr),
            data: r,
        };
        self.tx
            .clone()
            .send(m)
            .unwrap();
    }

    fn tx_send(&self, r: prelude::Records, t: checktable::Token) -> Result<i64, ()> {
        let send = self.tx_reply_channel.0.clone();
        let m = payload::Packet::Transaction {
            link: payload::Link::new(self.src, self.addr),
            data: r,
            state: payload::TransactionState::Pending(t, send),
        };
        self.tx
            .clone()
            .send(m)
            .unwrap();
        loop {
            match self.tx_reply_channel.1.try_recv() {
                Ok(r) => return r,
                Err(..) => thread::yield_now(),
            }
        }
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn put<V>(&self, u: V)
        where V: Into<Vec<prelude::DataType>>
    {
        self.send(vec![u.into()].into())
    }

    /// Perform a transactional write to the base node this Mutator was generated for.
    pub fn transactional_put<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<prelude::DataType>>
    {
        self.tx_send(vec![u.into()].into(), t)
    }

    /// Perform a non-transactional delete frome the base node this Mutator was generated for.
    pub fn delete<I>(&self, key: I)
        where I: Into<Vec<prelude::DataType>>
    {
        self.send(vec![prelude::Record::DeleteRequest(key.into())].into())
    }

    /// Perform a transactional delete from the base node this Mutator was generated for.
    pub fn transactional_delete<I>(&self, key: I, t: checktable::Token) -> Result<i64, ()>
        where I: Into<Vec<prelude::DataType>>
    {
        self.tx_send(vec![prelude::Record::DeleteRequest(key.into())].into(), t)
    }

    /// Perform a non-transactional update (delete followed by put) to the base node this Mutator
    /// was generated for.
    pub fn update<V>(&self, u: V)
        where V: Into<Vec<prelude::DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        self.send(vec![prelude::Record::DeleteRequest(self.primary_key
                                                          .iter()
                                                          .map(|&col| &u[col])
                                                          .cloned()
                                                          .collect()),
                       u.into()]
                          .into())
    }

    /// Perform a transactional update (delete followed by put) to the base node this Mutator was
    /// generated for.
    pub fn transactional_update<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<prelude::DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        let m = vec![prelude::Record::DeleteRequest(self.primary_key
                                                        .iter()
                                                        .map(|&col| &u[col])
                                                        .cloned()
                                                        .collect()),
                     u.into()]
                .into();
        self.tx_send(m, t)
    }
}

/// `Blender` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Blender`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be started using `Blender::start_migration`. Only one `Migration` can
/// occur at any given point in time.
pub struct Blender {
    ingredients: petgraph::Graph<node::Node, Edge>,
    source: NodeIndex,
    ndomains: usize,
    checktable: Arc<Mutex<checktable::CheckTable>>,

    txs: HashMap<domain::Index, mpsc::SyncSender<payload::Packet>>,
    domains: Vec<thread::JoinHandle<()>>,

    log: slog::Logger,
}

impl Default for Blender {
    fn default() -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new("source",
                                                &["because-type-inference"],
                                                node::Type::Source,
                                                true));
        Blender {
            ingredients: g,
            source: source,
            ndomains: 0,
            checktable: Arc::new(Mutex::new(checktable::CheckTable::new())),

            txs: HashMap::default(),
            domains: Vec::new(),

            log: slog::Logger::root(slog::Discard, None),
        }
    }
}

impl Blender {
    /// Construct a new, empty `Blender`
    pub fn new() -> Self {
        Blender::default()
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Start setting up a new `Migration`.
    pub fn start_migration(&mut self) -> Migration {
        info!(self.log, "starting migration");
        let miglog = self.log.new(None);
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
            readers: Default::default(),

            start: time::Instant::now(),
            log: miglog,
        }
    }

    /// Get a boxed function which can be used to validate tokens.
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable = self.checktable.clone();
        Box::new(move |t: &checktable::Token| checktable.lock().unwrap().validate_token(t))
    }

    #[cfg(test)]
    pub fn graph(&self) -> &prelude::Graph {
        &self.ingredients
    }

    /// Get references to all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    ///
    /// This function will only tell you which nodes are input nodes in the graph. To obtain a
    /// function for inserting writes, use `Blender::get_putter`.
    pub fn inputs(&self) -> Vec<(core::NodeAddress, &node::Node)> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .flat_map(|ingress| {
                          self.ingredients.neighbors_directed(ingress,
                                                              petgraph::EdgeDirection::Outgoing)
                      })
            .map(|n| (n, &self.ingredients[n]))
            .filter(|&(_, base)| base.is_internal() && base.is_base())
            .map(|(n, base)| (n.into(), &*base))
            .collect()
    }

    /// Get a reference to all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    ///
    /// This function will only tell you which nodes are output nodes in the graph. To obtain a
    /// function for performing reads, call `.get_reader()` on the returned reader.
    pub fn outputs(&self) -> Vec<(core::NodeAddress, &node::Node, &node::Reader)> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                use flow::node;
                if let node::Type::Reader(_, ref inner) = *self.ingredients[n] {
                    // we want to give the the node that is being materialized
                    // not the reader node itself
                    let src = self.ingredients
                        .neighbors_directed(n, petgraph::EdgeDirection::Incoming)
                        .next()
                        .unwrap();
                    Some((src.into(), &self.ingredients[src], inner))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Obtain a new function for querying a given (already maintained) reader node.
    pub fn get_getter(&self,
                      node: core::NodeAddress)
                      -> Option<Box<Fn(&prelude::DataType) -> Result<core::Datas, ()> + Send>> {

        // reader should be a child of the given node
        trace!(self.log, "creating reader"; "for" => node.as_global().index());
        let reader = self.ingredients
            .neighbors_directed(*node.as_global(), petgraph::EdgeDirection::Outgoing)
            .filter_map(|ni| if let node::Type::Reader(_, ref inner) = *self.ingredients[ni] {
                            Some(inner)
                        } else {
                            None
                        })
            .next(); // there should be at most one

        reader.and_then(|r| r.get_reader())
    }

    /// Obtain a new function for querying a given (already maintained) transactional reader node.
    pub fn get_transactional_getter
        (&self,
         node: core::NodeAddress)
-> Option<Box<Fn(&prelude::DataType) -> Result<(core::Datas, checktable::Token), ()> + Send>>{

        // reader should be a child of the given node
        trace!(self.log, "creating transactional reader"; "for" => node.as_global().index());
        let reader = self.ingredients
            .neighbors_directed(*node.as_global(), petgraph::EdgeDirection::Outgoing)
            .filter_map(|ni| if let node::Type::Reader(_, ref inner) = *self.ingredients[ni] {
                            Some(inner)
                        } else {
                            None
                        })
            .next(); // there should be at most one

        reader.map(|inner| {
            let arc = inner.state
                .as_ref()
                .unwrap()
                .clone();
            let generator = inner.token_generator.clone().unwrap();
            Box::new(move |q: &prelude::DataType| -> Result<(core::Datas, checktable::Token), ()> {
                arc.find_and(q,
                              |rs| rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>())
                    .map(|(res, ts)| {
                        let token = generator.generate(ts, q.clone());
                        (res.unwrap_or_else(Vec::new), token)
                    })
            }) as Box<_>
        })
    }

    /// Obtain a mutator that can be used to perform writes and deletes from the given base node.
    pub fn get_mutator(&self, base: core::NodeAddress) -> Mutator {
        let n = self.ingredients
            .neighbors_directed(*base.as_global(), petgraph::EdgeDirection::Incoming)
            .next()
            .unwrap();
        let node = &self.ingredients[n];
        let tx = self.txs[&node.domain()].clone();

        trace!(self.log, "creating mutator"; "for" => n.index());
        Mutator {
            src: self.source.into(),
            tx: tx,
            addr: node.addr(),
            primary_key: self.ingredients[*base.as_global()]
                .suggest_indexes(base)
                .remove(&base)
                .unwrap_or_else(Vec::new),
            tx_reply_channel: mpsc::channel(),
        }
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> statistics::GraphStats {
        // TODO: request stats from domains in parallel.
        let domains = self.txs
            .iter()
            .map(|(di, s)| {
                let (tx, rx) = mpsc::sync_channel(1);
                s.send(payload::Packet::GetStatistics(tx)).unwrap();

                let (domain_stats, node_stats) = rx.recv().unwrap();
                let node_map = node_stats.into_iter().map(|(ni, ns)| (ni.into(), ns)).collect();

                (*di, (domain_stats, node_map))
            })
            .collect();

        statistics::GraphStats { domains: domains }
    }
}

impl fmt::Display for Blender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.
        for index in self.ingredients.node_indices() {
            indentln(f)?;
            write!(f, "{}", index.index())?;
            self.ingredients[index].describe(f, index)?;
        }

        // Output edges.
        for (_, edge) in self.ingredients
                .raw_edges()
                .iter()
                .enumerate() {
            indentln(f)?;
            write!(f, "{} -> {}", edge.source().index(), edge.target().index())?;
            if !edge.weight {
                // not materialized
                writeln!(f, " [style=\"dashed\"]")?;
            } else {
                writeln!(f, "")?;
            }
        }

        // Output footer.
        write!(f, "}}")?;

        Ok(())
    }
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'a> {
    mainline: &'a mut Blender,
    added: HashMap<NodeIndex, Option<domain::Index>>,
    readers: HashMap<NodeIndex, NodeIndex>,
    materialize: HashSet<(NodeIndex, NodeIndex)>,

    start: time::Instant,
    log: slog::Logger,
}

impl<'a> Migration<'a> {
    /// Add a new (empty) domain to the graph
    pub fn add_domain(&mut self) -> domain::Index {
        trace!(self.log, "creating new domain"; "domain" => self.mainline.ndomains);
        self.mainline.ndomains += 1;
        (self.mainline.ndomains - 1).into()
    }

    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, i: I) -> core::NodeAddress
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>,
              I: Into<node::Type>
    {
        let mut i = i.into();
        i.on_connected(&self.mainline.ingredients);

        let parents = i.ancestors();

        let transactional = !parents.is_empty() &&
            parents.iter().all(|p| self.mainline.ingredients[*p.as_global()].is_transactional());

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(name.to_string(), fields, i, transactional));
        info!(self.log, "adding new node"; "node" => ni.index(), "type" => format!("{:?}", *self.mainline.ingredients[ni]));

        // keep track of the fact that it's new
        self.added.insert(ni, None);
        // insert it into the graph
        if parents.is_empty() {
            self.mainline.ingredients.add_edge(self.mainline.source, ni, false);
        } else {
            for parent in parents {
                self.mainline.ingredients.add_edge(*parent.as_global(), ni, false);
            }
        }
        // and tell the caller its id
        ni.into()
    }

    /// Add a transactional base node to the graph
    pub fn add_transactional_base<S1, FS, S2>(&mut self, name: S1, fields: FS, b: Base) -> core::NodeAddress
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2> {
        let mut i:node::Type = b.into();
        i.on_connected(&self.mainline.ingredients);

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(name.to_string(), fields, i, true));
        info!(self.log, "adding new node"; "node" => ni.index(), "type" => format!("{:?}", *self.mainline.ingredients[ni]));

        // keep track of the fact that it's new
        self.added.insert(ni, None);
        // insert it into the graph
        self.mainline.ingredients.add_edge(self.mainline.source, ni, false);
        // and tell the caller its id
        ni.into()
    }

    #[cfg(test)]
    pub fn graph(&self) -> &prelude::Graph {
        self.mainline.graph()
    }

    /// Mark the edge between `src` and `dst` in the graph as requiring materialization.
    ///
    /// The reason this is placed per edge rather than per node is that only some children of a
    /// node may require materialization of their inputs (i.e., only those that will query along
    /// this edge). Since we must materialize the output of a node in a foreign domain once for
    /// every receiving domain, this can save us some space if a child that doesn't require
    /// materialization is in its own domain. If multiple nodes in the same domain require
    /// materialization of the same parent, that materialized state will be shared.
    pub fn materialize(&mut self, src: core::NodeAddress, dst: core::NodeAddress) {
        // TODO
        // what about if a user tries to materialize a cross-domain edge that has already been
        // converted to an egress/ingress pair?
        let e = self.mainline
            .ingredients
            .find_edge(*src.as_global(), *dst.as_global())
            .expect("asked to materialize non-existing edge");

        debug!(self.log, "told to materialize"; "node" => src.as_global().index());

        let mut e = self.mainline
            .ingredients
            .edge_weight_mut(e)
            .unwrap();
        if !*e {
            *e = true;
            // it'd be nice if we could just store the EdgeIndex here, but unfortunately that's not
            // guaranteed by petgraph to be stable in the presence of edge removals (which we do in
            // commit())
            self.materialize.insert((*src.as_global(), *dst.as_global()));
        }
    }

    /// Assign the ingredient with identifier `n` to the thread domain `d`.
    ///
    /// `n` must be have been added in this migration.
    pub fn assign_domain(&mut self, n: core::NodeAddress, d: domain::Index) {
        // TODO: what if a node is added to an *existing* domain?
        debug!(self.log, "node manually assigned to domain"; "node" => n.as_global().index(), "domain" => d.index());
        assert_eq!(self.added.insert(*n.as_global(), Some(d)).unwrap(), None);
    }

    fn ensure_reader_for(&mut self, n: core::NodeAddress) {
        if !self.readers.contains_key(n.as_global()) {
            // make a reader
            let r = node::Type::Reader(None, Default::default());
            let r = self.mainline.ingredients[*n.as_global()].mirror(r);
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(*n.as_global(), r, false);
            self.readers.insert(*n.as_global(), r);
        }
    }

    fn ensure_token_generator(&mut self, n: core::NodeAddress, key: usize) {
        let ri = self.readers[n.as_global()];
        if let node::Type::Reader(_, ref mut inner) = *self.mainline.ingredients[ri] {
            if inner.token_generator.is_some() {
                return;
            }
        } else {
            unreachable!("tried to add token generator to non-reader node");
        }

        let base_columns: Vec<(_, Option<_>)> =
            self.mainline.ingredients[*n.as_global()].base_columns(key,
                                                                   &self.mainline.ingredients,
                                                                   *n.as_global());

        let coarse_parents = base_columns.iter()
            .filter_map(|&(ni, o)| if o.is_none() { Some(ni) } else { None })
            .collect();

        let granular_parents = base_columns.into_iter()
            .filter_map(|(ni, o)| if o.is_some() {
                            Some((ni, o.unwrap()))
                        } else {
                            None
                        })
            .collect();

        let token_generator = checktable::TokenGenerator::new(coarse_parents, granular_parents);
        self.mainline
            .checktable
            .lock()
            .unwrap()
            .track(&token_generator);

        if let node::Type::Reader(_, ref mut inner) = *self.mainline.ingredients[ri] {
            inner.token_generator = Some(token_generator);
        }
    }

    fn reader_for(&self, n: core::NodeAddress) -> &node::Reader {
        let ri = self.readers[n.as_global()];
        if let node::Type::Reader(_, ref inner) = *self.mainline.ingredients[ri] {
            &*inner
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// The returned function can be called with a `Query`, and any matching results will be
    /// returned.
    pub fn maintain(&mut self,
                    n: core::NodeAddress,
                    key: usize)
                    -> Box<Fn(&prelude::DataType) -> Result<core::Datas, ()> + Send> {
        self.ensure_reader_for(n);
        let ri = self.readers[n.as_global()];

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if let Some(ref s) = inner.state {
                assert_eq!(s.key(), key);
            } else {
                use backlog;
                let (r, w) = backlog::new(cols, key);
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            inner.get_reader().unwrap()
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Set up the given node such that its output can be efficiently queried, and the results can
    /// be used in transactions.
    ///
    /// The returned function can be called with a `Query`, and any matching results will be
    /// returned, along with a token.
    pub fn transactional_maintain
        (&mut self,
         n: core::NodeAddress,
         key: usize)
         -> Result<Box<Fn(&prelude::DataType) -> Result<(core::Datas, checktable::Token), ()> + Send>, ()> {
        self.ensure_reader_for(n);
        self.ensure_token_generator(n, key);
        let ri = self.readers[n.as_global()];

        if !self.mainline.ingredients[ri].is_transactional() {
            return Err(());
        }

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if let Some(ref s) = inner.state {
                assert_eq!(s.key(), key);
            } else {
                use backlog;
                let (r, w) = backlog::new(cols, key);
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            let arc = inner.state
                .as_ref()
                .unwrap()
                .clone();
            let generator = inner.token_generator.clone().unwrap();
            Ok(Box::new(move |q: &prelude::DataType| -> Result<(core::Datas, checktable::Token), ()> {
                arc.find_and(q,
                              |rs| rs.into_iter().map(|r|(&**r).clone()).collect())
                    .map(|(res, ts)| {
                        let token = generator.generate(ts, q.clone());
                        (res.unwrap_or_else(Vec::new), token)
                    })
            }))
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Obtain a channel that is fed by the output stream of the given node.
    ///
    /// As new updates are processed by the given node, its outputs will be streamed to the
    /// returned channel. Node that this channel is *not* bounded, and thus a receiver that is
    /// slower than the system as a hole will accumulate a large buffer over time.
    pub fn stream(&mut self, n: core::NodeAddress) -> mpsc::Receiver<Vec<node::StreamUpdate>> {
        self.ensure_reader_for(n);
        let (tx, rx) = mpsc::channel();
        self.reader_for(n)
            .streamers
            .lock()
            .unwrap()
            .push(tx);
        rx
    }

    /// Set up the given node such that its output is stored in Memcached.
    pub fn memcached_hook(&mut self,
                          n: core::NodeAddress,
                          name: String,
                          servers: &[(&str, usize)],
                          key: usize) -> io::Result<core::NodeAddress> {
        let h = try!(hook::Hook::new(name, servers, vec![key]));
        let h = node::Type::Hook(Some(h));
        let h = self.mainline.ingredients[*n.as_global()].mirror(h);
        let h = self.mainline.ingredients.add_node(h);
        self.mainline.ingredients.add_edge(*n.as_global(), h, false);
        Ok(h.into())
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(self) {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());
        let mut new = HashSet::new();

        let log = self.log;
        let start = self.start;
        let mainline = self.mainline;

        // Make sure all new nodes are assigned to a domain
        for (node, domain) in self.added {
            let domain = domain.unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                // NOTE: this is the same code as in add_domain(), but we can't use self here
                trace!(log, "node automatically added to domain"; "node" => node.index(), "domain" => mainline.ndomains);
                mainline.ndomains += 1;
                (mainline.ndomains - 1).into()

            });
            mainline.ingredients[node].add_to(domain);
            new.insert(node);
        }

        // Readers are nodes too.
        // And they should be assigned the same domain as their parents
        for (parent, reader) in self.readers {
            let domain = mainline.ingredients[parent].domain();
            mainline.ingredients[reader].add_to(domain);
            new.insert(reader);
        }

        // Set up ingress and egress nodes
        let mut swapped =
            migrate::routing::add(&log, &mut mainline.ingredients, mainline.source, &mut new);

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<_> =
            new.iter().map(|&ni| mainline.ingredients[ni].domain()).collect();
        let mut domain_nodes = mainline.ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .map(|ni| {
                     let domain = mainline.ingredients[ni].domain();
                     (domain, ni, new.contains(&ni))
                 })
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        let mut rxs = HashMap::new();

        // Set up input channels for new domains
        for domain in domain_nodes.keys() {
            if !mainline.txs.contains_key(domain) {
                let (tx, rx) = mpsc::sync_channel(10);
                rxs.insert(*domain, rx);
                mainline.txs.insert(*domain, tx);
            }
        }

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_nodes {
            // Number of pre-existing nodes
            let mut nnodes = nodes.iter().filter(|&&(_, new)| !new).count();

            if nnodes == nodes.len() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            for &(ni, new) in nodes.iter() {
                if new {
                    debug!(log, "assigning local index"; "type" => format!("{:?}", *mainline.ingredients[ni]), "node" => ni.index(), "local" => nnodes);
                    mainline.ingredients[ni]
                        .set_addr(unsafe { prelude::NodeAddress::make_local(nnodes) });
                    nnodes += 1;
                }
            }

            // Figure out all the remappings that have happened
            let mut remap = HashMap::new();
            // The global address of each node in this domain is now a local one
            for &(ni, _) in nodes.iter() {
                remap.insert(ni.into(), mainline.ingredients[ni].addr());
            }
            // Parents in other domains have been swapped for ingress nodes.
            // Those ingress nodes' indices are now local.
            for (from, to) in swapped.remove(domain).unwrap_or_else(HashMap::new) {
                remap.insert(from.into(), mainline.ingredients[to].addr());
            }

            // Initialize each new node
            for &(ni, new) in nodes.iter() {
                if new && mainline.ingredients[ni].is_internal() {
                    trace!(log, "initializing new node"; "node" => ni.index());
                    mainline.ingredients
                        .node_weight_mut(ni)
                        .unwrap()
                        .on_commit(&remap);
                }
            }
        }

        // at this point, we've hooked up the graph such that, for any given domain, the graph
        // looks like this:
        //
        //      o (egress)
        //     +.\......................
        //     :  o (ingress)
        //     :  |
        //     :  o-------------+
        //     :  |             |
        //     :  o             o
        //     :  |             |
        //     :  o (egress)    o (egress)
        //     +..|...........+.|..........
        //     :  o (ingress) : o (ingress)
        //     :  |\          :  \
        //     :  | \         :   o
        //
        // etc.
        // println!("{}", mainline);

        // Determine what nodes to materialize
        // NOTE: index will also contain the materialization information for *existing* domains
        debug!(log, "calculating materializations");
        let index = domain_nodes.iter()
            .map(|(domain, nodes)| {
                use self::migrate::materialization::{pick, index};
                debug!(log, "picking materializations"; "domain" => domain.index());
                let mat = pick(&log, &mainline.ingredients, &nodes[..]);
                debug!(log, "deriving indices"; "domain" => domain.index());
                let idx = index(&log, &mainline.ingredients, &nodes[..], mat);
                (*domain, idx)
            })
            .collect();

        let mut uninformed_domain_nodes = domain_nodes.clone();
        let ingresses_from_base = migrate::transactions::analyze_graph(&mainline.ingredients,
                                                                       mainline.source,
                                                                       domain_nodes);
        let (start_ts, end_ts, prevs) = mainline.checktable
            .lock()
            .unwrap()
            .perform_migration(&ingresses_from_base);

        info!(log, "migration claimed timestamp range"; "start" => start_ts, "end" => end_ts);

        // Boot up new domains (they'll ignore all updates for now)
        debug!(log, "booting new domains");
        for domain in changed_domains {
            if !rxs.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            // Start up new domain
            let jh = migrate::booting::boot_new(log.new(o!("domain" => domain.index())),
                                                domain.index().into(),
                                                &mut mainline.ingredients,
                                                uninformed_domain_nodes.remove(&domain).unwrap(),
                                                mainline.checktable.clone(),
                                                rxs.remove(&domain).unwrap(),
                                                start_ts);
            mainline.domains.push(jh);
        }
        drop(rxs);

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        migrate::augmentation::inform(&log,
                                      &mut mainline.ingredients,
                                      mainline.source,
                                      &mut mainline.txs,
                                      uninformed_domain_nodes,
                                      start_ts,
                                      prevs.unwrap());

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        migrate::routing::connect(&log, &mut mainline.ingredients, &mainline.txs, &new);

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        migrate::materialization::initialize(&log,
                                             &mainline.ingredients,
                                             mainline.source,
                                             &new,
                                             index,
                                             &mut mainline.txs);

        info!(log, "finalizing migration");
        migrate::transactions::finalize(ingresses_from_base, &log, &mut mainline.txs, end_ts);

        warn!(log, "migration completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
    }
}

impl Drop for Blender {
    fn drop(&mut self) {
        //println!("Blender started dropping.");

        for (_, tx) in &mut self.txs {
            // don't unwrap, because given domain may already have terminated
            drop(tx.send(payload::Packet::Quit));
        }
        for d in self.domains.drain(..) {
            //println!("Waiting for domain thread to join.");
            d.join().unwrap();
        }

        //println!("Blender is done dropping.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Blender without any domains gets dropped once it leaves the scope.
    #[test]
    fn it_works_default() {
        // Blender gets dropped. It doesn't have Domains, so we don't see any dropped.
        let b = Blender::default();
        assert_eq!(b.ndomains, 0);
    }

    // Blender with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration() {
        use Recipe;

        let r_txt = "INSERT INTO a (x, y, z) VALUES (?, ?, ?);\n
                     INSERT INTO b (r, s) VALUES (?, ?);\n";
        let mut r = Recipe::from_str(r_txt, None).unwrap();

        let mut b = Blender::new();
        {
            let mut mig = b.start_migration();
            assert!(r.activate(&mut mig).is_ok());
            mig.commit();
        }
    }
}
