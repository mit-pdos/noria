use crate::controller::domain_handle::{DomainHandle, DomainShardHandle};
use crate::controller::migrate::materialization::Materializations;
use crate::controller::recipe::Schema;
use crate::controller::schema;
use crate::controller::{ControllerState, Migration, Recipe};
use crate::controller::{Worker, WorkerIdentifier};
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use dataflow::prelude::*;
use dataflow::{node, payload::ControlReplyPacket, prelude::Packet, DomainBuilder, DomainConfig};
use hyper::{self, Method, StatusCode};
use mio::net::TcpListener;
use nom_sql::ColumnSpecification;
use noria::builders::*;
use noria::channel::tcp::{SendError, TcpSender};
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::debug::stats::{DomainStats, GraphStats, NodeStats};
use noria::ActivationResult;
use petgraph::visit::Bfs;
use slog::Logger;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cell, io, thread, time};
use tokio::prelude::*;

/// `Controller` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub(super) struct ControllerInner {
    pub(super) ingredients: Graph,
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    pub(super) domain_config: DomainConfig,

    /// Parameters for persistence code.
    pub(super) persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    recipe: Recipe,

    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,
    pub(super) debug_channel: Option<SocketAddr>,

    /// Map from worker address to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,

    /// State between migrations
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    pub(super) epoch: Epoch,

    pending_recovery: Option<(Vec<String>, usize)>,

    quorum: usize,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    last_checked_workers: Instant,

    log: slog::Logger,

    pub(in crate::controller) replies: DomainReplies,
}

pub(in crate::controller) struct DomainReplies(
    futures::sync::mpsc::UnboundedReceiver<ControlReplyPacket>,
);

impl DomainReplies {
    fn read_n_domain_replies(&mut self, n: usize) -> Vec<ControlReplyPacket> {
        let mut crps = Vec::with_capacity(n);

        // TODO
        // TODO: it's so stupid to spin here now...
        // TODO
        loop {
            match self.0.poll() {
                Ok(Async::NotReady) => thread::yield_now(),
                Ok(Async::Ready(Some(crp))) => {
                    crps.push(crp);
                    if crps.len() == n {
                        return crps;
                    }
                }
                Ok(Async::Ready(None)) => {
                    unreachable!("got unexpected EOF from domain reply channel");
                }
                Err(e) => {
                    unimplemented!("failed to read control reply packet: {:?}", e);
                }
            }
        }
    }

    pub(in crate::controller) fn wait_for_acks(&mut self, d: &DomainHandle) {
        for r in self.read_n_domain_replies(d.shards()) {
            match r {
                ControlReplyPacket::Ack(_) => {}
                r => unreachable!("got unexpected non-ack control reply: {:?}", r),
            }
        }
    }

    fn wait_for_statistics(
        &mut self,
        d: &DomainHandle,
    ) -> Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)> {
        let mut stats = Vec::with_capacity(d.shards());
        for r in self.read_n_domain_replies(d.shards()) {
            match r {
                ControlReplyPacket::Statistics(d, s) => stats.push((d, s)),
                r => unreachable!("got unexpected non-stats control reply: {:?}", r),
            }
        }
        stats
    }
}

pub(super) fn graphviz(
    graph: &Graph,
    detailed: bool,
    materializations: &Materializations,
) -> String {
    let mut s = String::new();

    let indentln = |s: &mut String| s.push_str("    ");

    // header.
    s.push_str("digraph {{\n");

    // global formatting.
    indentln(&mut s);
    if detailed {
        s.push_str("node [shape=record, fontsize=10]\n");
    } else {
        s.push_str("graph [ fontsize=24 fontcolor=\"#0C6fA9\", outputorder=edgesfirst ]\n");
        s.push_str("edge [ color=\"#0C6fA9\", style=bold ]\n");
        s.push_str("node [ color=\"#0C6fA9\", shape=box, style=\"rounded,bold\" ]\n");
    }

    // node descriptions.
    for index in graph.node_indices() {
        let node = &graph[index];
        let materialization_status = materializations.get_status(&index, node);
        indentln(&mut s);
        s.push_str(&format!("n{}", index.index()));
        s.push_str(&node.describe(index, detailed, materialization_status));
    }

    // edges.
    for (_, edge) in graph.raw_edges().iter().enumerate() {
        indentln(&mut s);
        s.push_str(&format!(
            "n{} -> n{} [ {} ]",
            edge.source().index(),
            edge.target().index(),
            if graph[edge.source()].is_egress() {
                "color=\"#CCCCCC\""
            } else if graph[edge.source()].is_source() {
                "style=invis"
            } else {
                ""
            }
        ));
        s.push_str("\n");
    }

    // footer.
    s.push_str("}}");

    s
}

impl ControllerInner {
    pub(super) fn external_request<A: Authority + 'static>(
        &mut self,
        method: hyper::Method,
        path: String,
        query: Option<String>,
        body: Vec<u8>,
        authority: &Arc<A>,
    ) -> Result<Result<String, String>, StatusCode> {
        use serde_json as json;

        match (&method, path.as_ref()) {
            (&Method::GET, "/simple_graph") => return Ok(Ok(self.graphviz(false))),
            (&Method::POST, "/simple_graphviz") => {
                return Ok(Ok(json::to_string(&self.graphviz(false)).unwrap()));
            }
            (&Method::GET, "/graph") => return Ok(Ok(self.graphviz(true))),
            (&Method::POST, "/graphviz") => {
                return Ok(Ok(json::to_string(&self.graphviz(true)).unwrap()));
            }
            (&Method::GET, "/get_statistics") => {
                return Ok(Ok(json::to_string(&self.get_statistics()).unwrap()));
            }
            _ => {}
        }

        if self.pending_recovery.is_some() || self.workers.len() < self.quorum {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

        match (method, path.as_ref()) {
            (Method::GET, "/flush_partial") => {
                Ok(Ok(json::to_string(&self.flush_partial()).unwrap()))
            }
            (Method::POST, "/inputs") => Ok(Ok(json::to_string(&self.inputs()).unwrap())),
            (Method::POST, "/outputs") => Ok(Ok(json::to_string(&self.outputs()).unwrap())),
            (Method::GET, "/instances") => Ok(Ok(json::to_string(&self.get_instances()).unwrap())),
            (Method::GET, "/nodes") => {
                // TODO(malte): this is a pretty yucky hack, but hyper doesn't provide easy access
                // to individual query variables unfortunately. We'll probably want to factor this
                // out into a helper method.
                let nodes = if let Some(query) = query {
                    let vars: Vec<_> = query.split("&").map(String::from).collect();
                    if let Some(n) = &vars.into_iter().find(|v| v.starts_with("w=")) {
                        self.nodes_on_worker(Some(&n[2..].parse().unwrap()))
                    } else {
                        self.nodes_on_worker(None)
                    }
                } else {
                    // all data-flow nodes
                    self.nodes_on_worker(None)
                };
                Ok(Ok(json::to_string(
                    &nodes
                        .into_iter()
                        .filter_map(|ni| {
                            let n = &self.ingredients[ni];
                            if n.is_internal() {
                                Some((ni, n.name(), n.description(true)))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap()))
            }
            (Method::POST, "/table_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| Ok(json::to_string(&self.table_builder(args)).unwrap())),
            (Method::POST, "/view_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| Ok(json::to_string(&self.view_builder(args)).unwrap())),
            (Method::POST, "/extend_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.extend_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/install_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.install_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/set_security_config") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.set_security_config(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/create_universe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.create_universe(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/remove_node") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.remove_nodes(vec![args].as_slice())
                        .map(|r| json::to_string(&r).unwrap())
                }),
            _ => Err(StatusCode::NOT_FOUND),
        }
    }

    pub(super) fn handle_register(
        &mut self,
        msg: &CoordinationMessage,
        remote: &SocketAddr,
        read_listen_addr: SocketAddr,
    ) -> Result<(), io::Error> {
        info!(
            self.log,
            "new worker registered from {:?}, which listens on {:?}", msg.source, remote
        );

        let sender = TcpSender::connect(remote)?;
        let ws = Worker::new(sender);
        self.workers.insert(msg.source.clone(), ws);
        self.read_addrs.insert(msg.source.clone(), read_listen_addr);

        if self.workers.len() >= self.quorum {
            if let Some((recipes, recipe_version)) = self.pending_recovery.take() {
                assert_eq!(self.workers.len(), self.quorum);
                assert_eq!(self.recipe.version(), 0);
                assert!(recipe_version + 1 >= recipes.len());

                info!(self.log, "Restoring graph configuration");
                self.recipe = Recipe::with_version(
                    recipe_version + 1 - recipes.len(),
                    Some(self.log.clone()),
                );
                for r in recipes {
                    self.apply_recipe(self.recipe.clone().extend(&r).unwrap())
                        .unwrap();
                }
            }
        }

        Ok(())
    }

    fn check_worker_liveness(&mut self) {
        let mut any_failed = false;

        // check if there are any newly failed workers
        if self.last_checked_workers.elapsed() > self.healthcheck_every {
            for (_addr, ws) in self.workers.iter() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 4 {
                    any_failed = true;
                }
            }
            self.last_checked_workers = Instant::now();
        }

        // if we have newly failed workers, iterate again to find all workers that have missed >= 3
        // heartbeats. This is necessary so that we correctly handle correlated failures of
        // workers.
        if any_failed {
            let mut failed = Vec::new();
            for (addr, ws) in self.workers.iter_mut() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 3 {
                    error!(self.log, "worker at {:?} has failed!", addr);
                    ws.healthy = false;
                    failed.push(addr.clone());
                }
            }
            self.handle_failed_workers(failed);
        }
    }

    /// Recovers a domain if its state can be reconstructed from the existing graph.
    /// Returns whether the domain was recovered.
    fn recover_domain(&mut self, domain: DomainIndex) -> bool {
        let nodes = self.nodes_in_domain(domain);

        // domains must be linear with one egress and possibly multiple ingress
        let mut ingress = Vec::new();
        let mut egress = None;
        let mut stateful = Vec::new();
        for &ni in &nodes {
            let node = &self.ingredients[ni];
            if node.is_ingress() {
                ingress.push(ni);
            }
            if node.is_egress() {
                assert!(egress.is_none());
                egress = Some(ni);
            }
            if node.is_internal() {
                let is_stateful = match **node {
                    NodeOperator::Sum(..)
                        | NodeOperator::Extremum(..)
                        | NodeOperator::Concat(..)
                        | NodeOperator::Replica(..)
                        | NodeOperator::TopK(..) => true,
                    NodeOperator::Join(..)
                        | NodeOperator::Latest(..)
                        | NodeOperator::Project(..)
                        | NodeOperator::Union(..)
                        | NodeOperator::Identity(..)
                        | NodeOperator::Filter(..)
                        | NodeOperator::Trigger(..)
                        | NodeOperator::Rewrite(..)
                        | NodeOperator::Distinct(..) => false,
                };
                if is_stateful {
                    stateful.push(ni);
                }
            }
            if node.is_source() || node.is_sharder() || node.is_base() || node.is_reader() {
                unimplemented!();
            }
            if node.is_dropped() {
                unreachable!();
            }
        }

        assert!(ingress.len() > 0);
        let egress = egress.unwrap();
        if stateful.len() == 1 {
            if self.ingredients[stateful[0]].replica_type().is_some() && nodes.len() == 3 {
                // exactly one ingress, one egress, and one stateful replica
                self.recover_hot_spare(stateful[0], ingress, egress);
                true
            } else {
                // the stateful node does not have a replica
                false
            }
        } else if stateful.len() == 0 {
            self.recover_stateless_domain(ingress, egress);
            true
        } else {
            // more than one stateful node
            false
        }
    }

    fn child(&self, ni: NodeIndex) -> NodeIndex {
        let mut nodes = self
                .ingredients
                .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing);
        let ni = nodes.next().unwrap();
        assert_eq!(nodes.count(), 0);
        ni
    }

    fn parent(&self, ni: NodeIndex) -> NodeIndex {
        let mut nodes = self
                .ingredients
                .neighbors_directed(ni, petgraph::EdgeDirection::Incoming);
        let ni = nodes.next().unwrap();
        assert_eq!(nodes.count(), 0);
        ni
    }

    /// Recovers a domain with only stateless nodes.
    /// TODO(ygina): handle disjoint node chains, merges and splits, source and sink
    fn recover_stateless_domain(
        &mut self,
        failed_ingress: Vec<NodeIndex>,
        failed_egress: NodeIndex,
    ) {
        let domain_b1 = self.ingredients[failed_egress].domain();
        warn!(
            self.log,
            "recovering stateless domain {}",
            domain_b1.index(),
        );

        // obtain the (assumed) linear path in the domain
        let mut path = failed_ingress.clone();
        let mut ni = self.child(failed_ingress[0]);
        for &ingress in &failed_ingress {
            // all ingress nodes are in the same domain and have the same child
            assert_eq!(domain_b1, self.ingredients[ingress].domain());
            assert_eq!(ni, self.child(ingress));
        };
        while ni != failed_egress {
            path.push(ni);
            ni = self.child(ni);
        }
        path.push(failed_egress);
        assert!(path.len() > 0);

        // prevent A from sending messages to B2 even once the connection is regenerated.
        // B2 won't send messages to C since it'll be a new domain.
        for &ingress in &failed_ingress {
            let egress_a = self.parent(ingress);
            let m = box Packet::RemoveChild {
                node: self.ingredients[egress_a].local_addr(),
                child: ingress,
                replace_with: None,
            };
            self.domains
                .get_mut(&self.ingredients[egress_a].domain())
                .unwrap()
                .send_to_healthy(m, &self.workers)
                .unwrap();
        }

        // do a migration that regenerates the nodes in domain B2, which has a different index
        // from domain B1. however, the nodes in B1 and B2 have the same indexes. the domains
        // can't start sending messages until replay paths have been updated. the network
        // connection between A and B2 initially exists due to how migrations work.
        //
        // dataflow graph: A ---> B2 -x-> C
        self.migrate(|mig| {
            let new_egress = mig.replicate_nodes(&path);
            assert_eq!(new_egress, failed_egress);
        });

        // set replay paths in B2 to the same as those that were in B1. then form the network
        // connections, even though the domains won't start sending messages to each other until
        // they receive ResumeAts.
        //
        // dataflow graph: A ---> B2 ---> C
        let egress_b2 = failed_egress;
        let domain_b2 = self.ingredients[egress_b2].domain();
        for segment in self.materializations.get_segments(domain_b1) {
            self.domains
                .get_mut(&domain_b2)
                .unwrap()
                .send_to_healthy(segment.into_packet(), &self.workers)
                .unwrap();
        }

        let ingress_cs = self.ingredients
            .neighbors_directed(failed_egress, petgraph::EdgeDirection::Outgoing)
            .collect::<Vec<NodeIndex>>();
        self.migrate(|mig| assert_eq!(mig.link_nodes(egress_b2, &ingress_cs).len(), 0));

        // tell C about the new incoming connection from B2 so that it can tell B2 where to resume
        // sending messages. B2 is in charge of realizing from that message that it also needs to
        // tell A where to resume sending messages.
        //
        // dataflow graph: A ---> B2 -o-> C
        for ingress_c in ingress_cs {
            self.send_new_incoming(ingress_c, egress_b2, Some(failed_egress), false);
        }
    }

    /// Recovers a domain with exactly one ingress, one egress, and one stateful replica.
    fn recover_hot_spare(
        &mut self,
        ni: NodeIndex,
        failed_ingress: Vec<NodeIndex>,
        failed_egress: NodeIndex,
    ) {
        let failed_ingress = failed_ingress[0];
        let failed_domain = self.ingredients[ni].domain();
        assert_eq!(failed_domain, self.ingredients[failed_ingress].domain());
        assert_eq!(failed_domain, self.ingredients[failed_egress].domain());

        // contact the remaining replica to start recovery
        let r = match self.ingredients[ni].replica_type() {
            Some(node::ReplicaType::Bottom{ top, .. }) => top,
            Some(node::ReplicaType::Top{ bottom, .. }) => bottom,
            None => unreachable!(),
        };

        let domain = self.ingredients[r].domain();
        let dh = self.domains.get_mut(&domain).unwrap();
        let m = box Packet::MakeRecovery {
            node: self.ingredients[r].local_addr(),
        };
        dh.send_to_healthy(m, &self.workers).unwrap();

        info!(
            self.log,
            "replacing failed replica";
            "old" => ni.index(),
            "new" => r.index(),
        );

        // update replay paths
        //
        // harmless in the case of losing a bottom replica (doesn't do anything)
        // TODO(ygina): should also _remove_ the failed domain from materializations
        // TODO(ygina): super hacky...assumes the domain being replaced and this domain
        // are exact copies down to the # of nodes and local node index assignment
        for segment in self.materializations.get_segments(failed_domain) {
            dh.send_to_healthy(segment.into_packet(), &self.workers).unwrap();
        }

        // TODO(ygina): multiple previous nodes
        let new_egress = self
            .ingredients
            .neighbors_directed(failed_ingress, petgraph::EdgeDirection::Incoming)
            .next()
            .unwrap();
        let ingress = self
            .ingredients
            .neighbors_directed(failed_egress, petgraph::EdgeDirection::Outgoing)
            .collect::<Vec<NodeIndex>>();

        // clean up state in egress
        let m = box Packet::RemoveChild {
            node: self.ingredients[new_egress].local_addr(),
            child: failed_ingress,
            replace_with: Some(ingress.clone()),
        };
        self.domains
            .get_mut(&self.ingredients[new_egress].domain())
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        let path = self.migrate(|mig| mig.link_nodes(new_egress, &ingress));
        self.remove_nodes(&path[..]).unwrap();

        for &ingress_ni in &ingress {
            self.send_new_incoming(ingress_ni, new_egress, Some(failed_egress), true);
        }
    }

    /// Generates a new connection between two domains via an egress and ingress node, sometimes
    /// replacing an old connection from a dropped egress node.
    ///
    /// Notifies the bottom nodes which node was replaced with which node (if applicable). The
    /// bottom nodes are responsible for sending a ResumeAt to these new incoming connections to
    /// start receiving messages. This means the egress won't pre-emptively send messages to the
    /// ingress even though there is a working connection until it receives a ResumeAt.
    fn send_new_incoming(
        &mut self,
        ingress: NodeIndex,
        new_egress: NodeIndex,
        old_egress: Option<NodeIndex>,
        complete: bool,
    ) {
        let old_egress = old_egress.unwrap_or(new_egress);

        debug!(
            self.log,
            "notifying {} of new incoming connection",
            ingress.index();
            "old" => old_egress.index(),
            "new" => new_egress.index(),
        );

        let domain = self.ingredients[ingress].domain();
        let dh = self.domains.get_mut(&domain).unwrap();
        let m = box Packet::NewIncoming {
            to: self.ingredients[ingress].local_addr(),
            old: self.ingredients[old_egress].global_addr(),
            new: self.ingredients[new_egress].global_addr(),
            complete,
        };
        dh.send_to_healthy(m, &self.workers).unwrap();
    }

    fn handle_failed_workers(&mut self, wis: Vec<WorkerIdentifier>) {
        let mut failed = Vec::new();
        for wi in wis {
            for (d, dh) in &self.domains {
                if dh.assigned_to_worker(&wi) {
                    failed.push(*d);
                }
            }
        }

        // first, detect which domains can be recovered without dropping downstream nodes.
        // generate a list of failed domains we still need to handle.
        let failed = failed
            .into_iter()
            .filter(|&d| !self.recover_domain(d))
            .collect::<Vec<DomainIndex>>();

        // next, translate from the affected workers to affected data-flow nodes
        let mut affected_nodes = Vec::new();
        for domain in failed {
            info!(self.log, "handling failure of domain {:?}", domain);
            affected_nodes.extend(self.get_failed_nodes(domain));
        }

        // then, figure out which queries are affected (and thus must be removed and added again in
        // a migration)
        let affected_queries = self.recipe.queries_for_nodes(affected_nodes);
        let (recovery, mut original) = self.recipe.make_recovery(affected_queries);

        // activate recipe
        self.apply_recipe(recovery.clone())
            .expect("failed to apply recovery recipe");

        // we must do this *after* the migration, since the migration itself modifies the recipe in
        // `recovery`, and we currently need to clone it here.
        let tmp = self.recipe.clone();
        original.set_prior(tmp.clone());
        // somewhat awkward, but we must replace the stale `SqlIncorporator` state in `original`
        original.set_sql_inc(tmp.sql_inc().clone());

        // back to original recipe, which should add the query again
        self.apply_recipe(original)
            .expect("failed to activate original recipe");
    }

    pub(super) fn handle_heartbeat(&mut self, msg: &CoordinationMessage) -> Result<(), io::Error> {
        match self.workers.get_mut(&msg.source) {
            None => crit!(
                self.log,
                "got heartbeat for unknown worker {:?}",
                msg.source
            ),
            Some(ref mut ws) => {
                ws.last_heartbeat = Instant::now();
            }
        }

        self.check_worker_liveness();
        Ok(())
    }

    pub(crate) fn handle_resume_at(
        &mut self,
        node: NodeIndex,
        child: NodeIndex,
        label: usize,
        complete: bool,
    ) {
        debug!(
            self.log,
            "controller received SendResumeAt coordination message to forward";
            "node" => node.index(),
            "child" => child.index(),
            "label" => label,
            "complete" => complete,
        );

        let domain = self.ingredients[node].domain();
        let dh = self.domains.get_mut(&domain).unwrap();
        let m = box Packet::ResumeAt {
            node: self.ingredients[node].local_addr(),
            child,
            label,
            complete,
        };
        dh.send_to_healthy(m, &self.workers).unwrap();
    }

    /// Construct `ControllerInner` with a specified listening interface
    pub(super) fn new(
        log: slog::Logger,
        state: ControllerState,
        drx: futures::sync::mpsc::UnboundedReceiver<ControlReplyPacket>,
    ) -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
        ));

        let mut materializations = Materializations::new(&log);
        if !state.config.partial_enabled {
            materializations.disable_partial()
        }

        let cc = Arc::new(ChannelCoordinator::new());
        assert_ne!(state.config.quorum, 0);

        let pending_recovery = if !state.recipes.is_empty() {
            Some((state.recipes, state.recipe_version))
        } else {
            None
        };

        let mut recipe = Recipe::blank(Some(log.clone()));
        recipe.enable_reuse(state.config.reuse);

        ControllerInner {
            ingredients: g,
            source,
            ndomains: 0,

            materializations,
            sharding: state.config.sharding,
            domain_config: state.config.domain_config,
            persistence: state.config.persistence,
            heartbeat_every: state.config.heartbeat_every,
            healthcheck_every: state.config.healthcheck_every,
            recipe,
            quorum: state.config.quorum,
            log,

            domains: Default::default(),
            channel_coordinator: cc,
            debug_channel: None,
            epoch: state.epoch,

            remap: HashMap::default(),

            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            pending_recovery,
            last_checked_workers: Instant::now(),

            replies: DomainReplies(drx),
        }
    }

    /// Create a global channel for receiving tracer events.
    ///
    /// Only domains created after this method is called will be able to send trace events.
    ///
    /// This function may only be called once because the receiving end it returned.
    #[allow(unused)]
    fn create_tracer_channel(&mut self) -> TcpListener {
        assert!(self.debug_channel.is_none());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();
        self.debug_channel = Some(listener.local_addr().unwrap());
        listener
    }

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is
    ///     deleted once the `Controller` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    ///
    /// Must be called before any domains have been created.
    #[allow(unused)]
    fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    // Assigns nodes to this domain, and shards the domain across multiple workers.
    //
    // Each worker identifier in `identifiers` corresponds to a single shard in `num_shards`,
    // thus the logic of which worker gets which shard is determined by the code that calls
    // this method. That code must also ensure the workers are healthy.
    pub(in crate::controller) fn place_domain(
        &mut self,
        idx: DomainIndex,
        identifiers: Vec<WorkerIdentifier>,
        num_shards: Option<usize>,
        log: &Logger,
        nodes: Vec<(NodeIndex, bool)>,
    ) -> DomainHandle {
        // TODO: can we just redirect all domain traffic through the worker's connection?
        let mut assignments = Vec::new();
        let mut nodes = Some(
            nodes
                .into_iter()
                .map(|(ni, _)| {
                    let node = self.ingredients.node_weight_mut(ni).unwrap().take();
                    node.finalize(&self.ingredients)
                })
                .map(|nd| (nd.local_addr(), cell::RefCell::new(nd)))
                .collect(),
        );

        // Send `AssignDomain` to each shard of the given domain
        for i in 0..num_shards.unwrap_or(1) {
            let nodes = if i == num_shards.unwrap_or(1) - 1 {
                nodes.take().unwrap()
            } else {
                nodes.clone().unwrap()
            };

            let domain = DomainBuilder {
                index: idx,
                shard: if num_shards.is_some() { Some(i) } else { None },
                nshards: num_shards.unwrap_or(1),
                config: self.domain_config.clone(),
                nodes,
                persistence_parameters: self.persistence.clone(),
            };

            // send domain to worker
            let identifier = identifiers.get(i)
                .expect("number of identifiers should match number of shards");
            let w = self.workers.get_mut(&identifier).unwrap();
            info!(
                log,
                "sending domain {}.{} to worker {:?}",
                domain.index.index(),
                domain.shard.unwrap_or(0),
                w.sender.peer_addr()
            );
            let src = w.sender.local_addr().unwrap();
            w.sender
                .send(CoordinationMessage {
                    epoch: self.epoch,
                    source: src,
                    payload: CoordinationPayload::AssignDomain(domain),
                })
                .unwrap();

            assignments.push(identifier);
        }

        // Wait for all the domains to acknowledge.
        let mut txs = HashMap::new();
        let mut announce = Vec::new();
        let replies = self.replies.read_n_domain_replies(num_shards.unwrap_or(1));
        for r in replies {
            match r {
                ControlReplyPacket::Booted(shard, addr) => {
                    self.channel_coordinator.insert_remote((idx, shard), addr);
                    announce.push(DomainDescriptor::new(idx, shard, addr));
                    txs.insert(
                        shard,
                        self.channel_coordinator
                            .builder_for(&(idx, shard))
                            .unwrap()
                            .build_sync()
                            .unwrap(),
                    );
                }
                crp => {
                    unreachable!("got unexpected control reply packet: {:?}", crp);
                }
            }
        }

        // Tell all workers about the new domain(s)
        // TODO(jon): figure out how much of the below is still true
        // TODO(malte): this is a hack, and not an especially neat one. In response to a
        // domain boot message, we broadcast information about this new domain to all
        // workers, which inform their ChannelCoordinators about it. This is required so
        // that domains can find each other when starting up.
        // Moreover, it is required for us to do this *here*, since this code runs on
        // the thread that initiated the migration, and which will query domains to ask
        // if they're ready. No domain will be ready until it has found its neighbours,
        // so by sending out the information here, we ensure that we cannot deadlock
        // with the migration waiting for a domain to become ready when trying to send
        // the information. (We used to do this in the controller thread, with the
        // result of a nasty deadlock.)
        for endpoint in self.workers.values_mut() {
            for &dd in &announce {
                endpoint
                    .sender
                    .send(CoordinationMessage {
                        epoch: self.epoch,
                        source: endpoint.sender.local_addr().unwrap(),
                        payload: CoordinationPayload::DomainBooted(dd),
                    })
                    .unwrap();
            }
        }

        let shards = assignments
            .into_iter()
            .enumerate()
            .map(|(i, &worker)| {
                let tx = txs.remove(&i).unwrap();
                DomainShardHandle { worker, tx }
            })
            .collect();

        DomainHandle {
            idx,
            shards,
            log: log.clone(),
        }
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    #[allow(unused)]
    fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
        self.materializations.set_logger(&self.log);
    }

    /// Adds a new user universe.
    /// User universes automatically enforce security policies.
    fn add_universe<F, T>(&mut self, context: HashMap<String, DataType>, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration: new soup universe");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            replicated: Default::default(),
            linked: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            replicas: Default::default(),
            context,
            start: time::Instant::now(),
            log: miglog,
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    /// Perform a new query schema migration.
    // crate viz for tests
    crate fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            replicated: Default::default(),
            linked: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            replicas: Default::default(),
            context: Default::default(),
            start: time::Instant::now(),
            log: miglog,
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    #[cfg(test)]
    crate fn graph(&self) -> &Graph {
        &self.ingredients
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Table`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .map(|n| {
                let base = &self.ingredients[n];
                assert!(base.is_base());
                (base.name().to_owned(), n.into())
            })
            .collect()
    }

    /// Get a Vec of all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    fn outputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let name = self.ingredients[n].name().to_owned();
                self.ingredients[n]
                    .with_reader(|r| {
                        // we want to give the the node address that is being materialized not that of
                        // the reader node itself.
                        (name, r.is_for())
                    })
                    .ok()
            })
            .collect()
    }

    fn find_view_for(&self, node: NodeIndex) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        let mut bfs = Bfs::new(&self.ingredients, node);
        let mut reader = None;
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child]
                .with_reader(|r| r.is_for() == node)
                .unwrap_or(false)
            {
                reader = Some(child);
                break;
            }
        }

        reader
    }

    /// Obtain a `ViewBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    fn view_builder(&self, name: &str) -> Option<ViewBuilder> {
        // first try to resolve the node via the recipe, which handles aliasing between identical
        // queries.
        let node = match self.recipe.node_addr_for(name) {
            Ok(ni) => ni,
            Err(_) => {
                // if the recipe doesn't know about this query, traverse the graph.
                // we need this do deal with manually constructed graphs (e.g., in tests).
                *self.outputs().get(name)?
            }
        };

        self.find_view_for(node).map(|r| {
            let domain = self.ingredients[r].domain();
            let columns = self.ingredients[r].fields().to_vec();
            let schema = self.view_schema(r);
            let shards = (0..self.domains[&domain].shards())
                .map(|i| self.read_addrs[&self.domains[&domain].assignment(i)].clone())
                .collect();

            ViewBuilder {
                node: r,
                columns,
                schema,
                shards,
            }
        })
    }

    fn view_schema(&self, view_ni: NodeIndex) -> Option<Vec<ColumnSpecification>> {
        let n = &self.ingredients[view_ni];
        let schema: Vec<_> = (0..n.fields().len())
            .into_iter()
            .map(|i| schema::column_schema(&self.ingredients, view_ni, &self.recipe, i, &self.log))
            .collect();

        if schema.iter().any(|cs| cs.is_none()) {
            None
        } else {
            Some(schema.into_iter().map(|cs| cs.unwrap()).collect())
        }
    }

    /// Obtain a TableBuild that can be used to construct a Table to perform writes and deletes
    /// from the given named base node.
    fn table_builder(&self, base: &str) -> Option<TableBuilder> {
        let ni = match self.recipe.node_addr_for(base) {
            Ok(ni) => ni,
            Err(_) => *self.inputs().get(base)?,
        };
        let node = &self.ingredients[ni];

        trace!(self.log, "creating table"; "for" => base);

        let mut key = self.ingredients[ni]
            .suggest_indexes(ni)
            .remove(&ni)
            .map(|(c, _)| c)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col, _) = self.ingredients[ni].sharded_by() {
                key = vec![col];
            }
        } else {
            is_primary = true;
        }

        let txs = (0..self.domains[&node.domain()].shards())
            .map(|i| {
                self.channel_coordinator
                    .get_addr(&(node.domain(), i))
                    .unwrap()
            })
            .collect();

        let base_operator = node
            .get_base()
            .expect("asked to get table for non-base node");
        let columns: Vec<String> = node
            .fields()
            .iter()
            .enumerate()
            .filter(|&(n, _)| !base_operator.get_dropped().contains_key(n))
            .map(|(_, s)| s.clone())
            .collect();
        assert_eq!(
            columns.len(),
            node.fields().len() - base_operator.get_dropped().len()
        );
        let schema = self.recipe.schema_for(base).map(|s| match s {
            Schema::Table(s) => s,
            _ => panic!("non-base schema {:?} returned for table '{}'", s, base),
        });

        Some(TableBuilder {
            txs,
            addr: node.local_addr().into(),
            key,
            key_is_primary: is_primary,
            dropped: base_operator.get_dropped(),
            table_name: node.name().to_owned(),
            columns,
            schema,
        })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    fn get_statistics(&mut self) -> GraphStats {
        let workers = &self.workers;
        let replies = &mut self.replies;
        // TODO: request stats from domains in parallel.
        let domains = self
            .domains
            .iter_mut()
            .flat_map(|(di, s)| {
                s.send_to_healthy(box Packet::GetStatistics, workers)
                    .unwrap();
                replies.wait_for_statistics(&s).into_iter().enumerate().map(
                    move |(i, (domain_stats, node_stats))| {
                        let node_map = node_stats
                            .into_iter()
                            .map(|(ni, ns)| (ni.into(), ns))
                            .collect();

                        ((di.clone(), i), (domain_stats, node_map))
                    },
                )
            })
            .collect();

        GraphStats { domains }
    }

    fn get_instances(&self) -> Vec<(WorkerIdentifier, bool, Duration)> {
        self.workers
            .iter()
            .map(|(&id, ref status)| (id, status.healthy, status.last_heartbeat.elapsed()))
            .collect()
    }

    fn flush_partial(&mut self) -> u64 {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let replies = &mut self.replies;
        let to_evict: Vec<_> = self
            .domains
            .iter_mut()
            .map(|(di, s)| {
                s.send_to_healthy(box Packet::GetStatistics, workers)
                    .unwrap();
                let to_evict: Vec<(NodeIndex, u64)> = replies
                    .wait_for_statistics(&s)
                    .into_iter()
                    .flat_map(move |(_, node_stats)| {
                        node_stats
                            .into_iter()
                            .filter_map(|(ni, ns)| match ns.materialized {
                                MaterializationStatus::Partial => Some((ni, ns.mem_size)),
                                _ => None,
                            })
                    })
                    .collect();
                (*di, to_evict)
            })
            .collect();

        let mut total_evicted = 0;
        for (di, nodes) in to_evict {
            for (ni, bytes) in nodes {
                let na = self.ingredients[ni].local_addr();
                self.domains
                    .get_mut(&di)
                    .unwrap()
                    .send_to_healthy(
                        box Packet::Evict {
                            node: Some(na),
                            num_bytes: bytes as usize,
                        },
                        workers,
                    )
                    .expect("failed to send domain flush message");
                total_evicted += bytes;
            }
        }

        warn!(
            self.log,
            "flushed {} bytes of partial domain state", total_evicted
        );

        total_evicted
    }

    pub(super) fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> Result<(), String> {
        let log = self.log.clone();
        let mut r = self.recipe.clone();
        let groups = self.recipe.security_groups();

        let mut universe_groups = HashMap::new();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        let uid = &[uid];
        if context.get("group").is_none() {
            let x = Arc::new(Mutex::new(HashMap::new()));
            for g in groups {
                // TODO: this should use external APIs through noria::ControllerHandle
                // TODO: can this move to the client entirely?
                let rgb: Option<ViewBuilder> = self.view_builder(&g);
                // TODO: is it even okay to use wait() here?
                let view = rgb.map(|rgb| rgb.build(x.clone()).wait().unwrap()).unwrap();
                let my_groups: Vec<DataType> = view
                    .lookup(uid, true)
                    .wait()
                    .unwrap()
                    .1
                    .iter()
                    .map(|v| v[1].clone())
                    .collect();
                universe_groups.insert(g, my_groups);
            }
        }

        self.add_universe(context.clone(), |mut mig| {
            r.next();
            match r.create_universe(&mut mig, universe_groups) {
                Ok(ar) => {
                    info!(log, "{} expressions added", ar.expressions_added);
                    info!(log, "{} expressions removed", ar.expressions_removed);
                    Ok(())
                }
                Err(e) => {
                    crit!(log, "failed to create universe: {:?}", e);
                    Err("failed to create universe".to_owned())
                }
            }
            .unwrap();
        });

        self.recipe = r;
        Ok(())
    }

    fn set_security_config(&mut self, p: String) -> Result<(), String> {
        self.recipe.set_security_config(&p);
        Ok(())
    }

    fn apply_recipe(&mut self, mut new: Recipe) -> Result<ActivationResult, String> {
        let r = self.migrate(|mig| {
            new.activate(mig)
                .map_err(|e| format!("failed to activate recipe: {}", e))
        });

        match r {
            Ok(ref ra) => {
                let (removed_bases, removed_other): (Vec<_>, Vec<_>) = ra
                    .removed_leaves
                    .iter()
                    .cloned()
                    .partition(|ni| self.ingredients[*ni].is_base());

                // first remove query nodes in reverse topological order
                let mut topo_removals = Vec::with_capacity(removed_other.len());
                let mut topo = petgraph::visit::Topo::new(&self.ingredients);
                while let Some(node) = topo.next(&self.ingredients) {
                    if removed_other.contains(&node) {
                        topo_removals.push(node);
                    }
                }
                topo_removals.reverse();

                for leaf in topo_removals {
                    self.remove_leaf(leaf)?;
                }

                // now remove bases
                for base in removed_bases {
                    // TODO(malte): support removing bases that still have children?
                    let children: Vec<NodeIndex> = self
                        .ingredients
                        .neighbors_directed(base, petgraph::EdgeDirection::Outgoing)
                        .collect();
                    // TODO(malte): what about domain crossings? can ingress/egress nodes be left
                    // behind?
                    assert_eq!(children.len(), 0);
                    debug!(
                        self.log,
                        "Removing base \"{}\"",
                        self.ingredients[base].name();
                        "node" => base.index(),
                    );
                    // now drop the (orphaned) base
                    self.remove_nodes(vec![base].as_slice()).unwrap();
                }

                self.recipe = new;
            }
            Err(ref e) => {
                crit!(self.log, "failed to apply recipe: {}", e);
                // TODO(malte): a little yucky, since we don't really need the blank recipe
                let recipe = mem::replace(&mut self.recipe, Recipe::blank(None));
                self.recipe = recipe.revert();
            }
        }

        r
    }

    fn extend_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        add_txt: String,
    ) -> Result<ActivationResult, String> {
        // needed because self.apply_recipe needs to mutate self.recipe, so can't have it borrowed
        let new = mem::replace(&mut self.recipe, Recipe::blank(None));
        match new.extend(&add_txt) {
            Ok(new) => {
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes.push(add_txt.clone());
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe extension".to_owned());
                }

                activation_result
            }
            Err((old, e)) => {
                // need to restore the old recipe
                crit!(self.log, "failed to extend recipe: {:?}", e);
                self.recipe = old;
                Err("failed to extend recipe".to_owned())
            }
        }
    }

    fn install_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        r_txt: String,
    ) -> Result<ActivationResult, String> {
        match Recipe::from_str(&r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let old = mem::replace(&mut self.recipe, Recipe::blank(None));
                let new = old.replace(r).unwrap();
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes = vec![r_txt.clone()];
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe installation".to_owned());
                }
                activation_result
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                Err("failed to parse recipe".to_owned())
            }
        }
    }

    fn graphviz(&self, detailed: bool) -> String {
        graphviz(&self.ingredients, detailed, &self.materializations)
    }

    fn remove_leaf(&mut self, mut leaf: NodeIndex) -> Result<(), String> {
        let mut removals = vec![];
        let start = leaf;
        assert!(!self.ingredients[leaf].is_source());

        info!(
            self.log,
            "Computing removals for removing node {}",
            leaf.index()
        );

        if self
            .ingredients
            .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
            .count()
            > 0
        {
            // This query leaf node has children -- typically, these are readers, but they can also
            // include egress nodes or other, dependent queries.
            let mut has_non_reader_children = false;
            let readers: Vec<_> = self
                .ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .filter(|ni| {
                    if self.ingredients[*ni].is_reader() {
                        true
                    } else {
                        has_non_reader_children = true;
                        false
                    }
                })
                .collect();
            if has_non_reader_children {
                // should never happen, since we remove nodes in reverse topological order
                crit!(
                    self.log,
                    "not removing node {} yet, as it still has non-reader children",
                    leaf.index()
                );
                unreachable!();
            }
            // nodes can have only one reader attached
            assert!(readers.len() <= 1);
            debug!(
                self.log,
                "Removing query leaf \"{}\"", self.ingredients[leaf].name();
                "node" => leaf.index(),
            );
            if !readers.is_empty() {
                removals.push(readers[0]);
                leaf = readers[0];
            } else {
                unreachable!();
            }
        }

        // `node` now does not have any children any more
        assert_eq!(
            self.ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .count(),
            0
        );

        let mut nodes = vec![leaf];
        while let Some(node) = nodes.pop() {
            let mut parents = self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .detach();
            while let Some(parent) = parents.next_node(&self.ingredients) {
                let edge = self.ingredients.find_edge(parent, node).unwrap();
                self.ingredients.remove_edge(edge);

                if !self.ingredients[parent].is_source()
                    && !self.ingredients[parent].is_base()
                    // ok to remove original start leaf
                    && (parent == start || !self.recipe.sql_inc().is_leaf_address(parent))
                    && self
                        .ingredients
                        .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                        .count() == 0
                {
                    nodes.push(parent);
                }
            }

            removals.push(node);
        }

        self.remove_nodes(removals.as_slice())
    }

    fn remove_nodes(&mut self, removals: &[NodeIndex]) -> Result<(), String> {
        // Remove node from controller local state
        let mut domain_removals: HashMap<DomainIndex, Vec<LocalNodeIndex>> = HashMap::default();
        for ni in removals {
            self.ingredients[*ni].remove();
            debug!(self.log, "Removed node {}", ni.index());
            domain_removals
                .entry(self.ingredients[*ni].domain())
                .or_insert(Vec::new())
                .push(self.ingredients[*ni].local_addr())
        }

        // Send messages to domains
        for (domain, nodes) in domain_removals {
            trace!(
                self.log,
                "Notifying domain {} of node removals",
                domain.index(),
            );

            match self
                .domains
                .get_mut(&domain)
                .unwrap()
                .send_to_healthy(box Packet::RemoveNodes { nodes }, &self.workers)
            {
                Ok(_) => (),
                Err(e) => match e {
                    SendError::IoError(ref ioe) => {
                        if ioe.kind() == io::ErrorKind::BrokenPipe
                            && ioe.get_ref().unwrap().description() == "worker failed"
                        {
                            // message would have gone to a failed worker, so ignore error
                        } else {
                            panic!("failed to remove nodes: {:?}", e);
                        }
                    }
                    _ => {
                        panic!("failed to remove nodes: {:?}", e);
                    }
                },
            }
        }

        Ok(())
    }

    fn get_failed_nodes(&self, lost_domain: DomainIndex) -> Vec<NodeIndex> {
        // Find nodes directly impacted by worker failure.
        let mut nodes: Vec<NodeIndex> = self.nodes_in_domain(lost_domain);

        // Add any other downstream nodes.
        let mut failed_nodes = Vec::new();
        while let Some(node) = nodes.pop() {
            failed_nodes.push(node);
            for child in self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            {
                if !nodes.contains(&child) {
                    nodes.push(child);
                }
            }
        }
        failed_nodes
    }

    /// NOTE(malte): this traverses all graph vertices in order to find those assigned to a
    /// domain. We do this to avoid keeping separate state that may get out of sync, but it
    /// could become a performance bottleneck in the future (e.g., when recovergin large
    /// graphs).
    fn nodes_in_domain(&self, i: DomainIndex) -> Vec<NodeIndex> {
        self.ingredients
            .node_indices()
            .filter(|&ni| ni != self.source)
            .filter(|&ni| !self.ingredients[ni].is_dropped())
            .filter(|&ni| self.ingredients[ni].domain() == i)
            .collect()
    }

    /// List data-flow nodes, on a specific worker if `worker` specified.
    fn nodes_on_worker(&self, worker: Option<&WorkerIdentifier>) -> Vec<NodeIndex> {
        if worker.is_some() {
            self.domains
                .values()
                .filter(|dh| dh.assigned_to_worker(worker.unwrap()))
                .fold(Vec::new(), |mut acc, dh| {
                    acc.extend(self.nodes_in_domain(dh.index()));
                    acc
                })
        } else {
            self.domains.values().fold(Vec::new(), |mut acc, dh| {
                acc.extend(self.nodes_in_domain(dh.index()));
                acc
            })
        }
    }
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        for (_, d) in &mut self.domains {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send_to_healthy(box Packet::Quit, &self.workers));
            }
        }
    }
}
