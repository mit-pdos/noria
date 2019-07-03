use crate::controller::domain_handle::{DomainHandle, DomainShardHandle};
use crate::controller::migrate::materialization::Materializations;
use crate::controller::recipe::Schema;
use crate::controller::schema;
use crate::controller::{ControllerState, Migration, Recipe};
use crate::controller::{Worker, WorkerIdentifier};
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use dataflow::payload::{ControlReplyPacket, TriggerEndpoint};
use dataflow::prelude::*;
use dataflow::{node, prelude::Packet, DomainBuilder, DomainConfig};
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
use std::collections::{BTreeMap, HashMap, HashSet};
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
    pub(super) domain_graph: DomainGraph,
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
    pub(in crate::controller) domain_nodes: HashMap<DomainIndex, Vec<NodeIndex>>,
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

    // Fault tolerance
    // TODO(ygina): assumes one recovery process is going on at any time
    /// Map from replica to its minimum provenance and provenance updates.
    provenance: HashMap<ReplicaAddr, (Provenance, Vec<ProvenanceUpdate>)>,
    /// Map from replica to a collection of replicas it is waiting to receive an ack from
    /// before we can send a ResumeAt message to the key.
    waiting_on: HashMap<ReplicaAddr, HashSet<ReplicaAddr>>,
    /// Map from replica to the children and labels that should be included in the ResumeAt
    /// message sent to the key.
    resume_ats: HashMap<ReplicaAddr, Vec<(ReplicaAddr, usize)>>,
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
        num_healthy: usize,
    ) -> Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)> {
        if num_healthy == 0 {
            return vec![];
        }
        let mut stats = Vec::with_capacity(num_healthy);
        for r in self.read_n_domain_replies(num_healthy) {
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
        let materialization_status = materializations.get_status(index, node);
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
    pub(in crate::controller) fn topo_order(&self, new: &HashSet<NodeIndex>) -> Vec<NodeIndex> {
        let mut topo_list = Vec::with_capacity(new.len());
        let mut topo = petgraph::visit::Topo::new(&self.ingredients);
        while let Some(node) = topo.next(&self.ingredients) {
            if node == self.source {
                continue;
            }
            if self.ingredients[node].is_dropped() {
                continue;
            }
            if !new.contains(&node) {
                continue;
            }
            topo_list.push(node);
        }
        topo_list
    }

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
                return Ok(Ok(format!("{:#?}", self.get_statistics())));
            }
            (&Method::POST, "/get_statistics") => {
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
                    let vars: Vec<_> = query.split('&').map(String::from).collect();
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
                            } else if n.is_base() {
                                Some((ni, n.name(), "Base table".to_owned()))
                            } else if n.is_reader() {
                                Some((ni, n.name(), "Leaf view".to_owned()))
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
            (Method::POST, "/shutdown_worker") => {
                json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.shutdown_worker(args)
                        .map(|r| json::to_string(&r).unwrap())
                })
            },
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
        self.workers.insert(msg.source, ws);
        self.read_addrs.insert(msg.source, read_listen_addr);

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
        let mut reader = Vec::new();
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
            if node.is_reader() {
                reader.push(ni);
            }
            if node.is_source() || node.is_base() {
                unimplemented!();
            }
            if node.is_dropped() {
                unreachable!();
            }
        }

        assert!(ingress.len() > 0);
        if reader.len() == 1 {
            // reader leaf
            assert_eq!(ingress.len(), 1);
            assert!(stateful.is_empty());
            assert!(egress.is_none());
            self.recover_reader(ingress[0], reader[0]);
            true
        } else if stateful.len() == 1 {
            if self.ingredients[stateful[0]].replica_type().is_some() && nodes.len() == 3 {
                // exactly one ingress, one egress, and one stateful replica
                // self.recover_hot_spare(stateful[0], ingress, egress.unwrap());
                // true
                unimplemented!();
            } else {
                // the stateful node does not have a replica
                false
            }
        } else if stateful.len() == 0 {
            // TODO(ygina): also include the shard
            self.recover_stateless_domain((domain, 0));
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
    fn recover_stateless_domain(&mut self, domain_b: ReplicaAddr) {
        warn!(
            self.log,
            "recovering stateless domain {}.{}",
            domain_b.0.index(),
            domain_b.1,
        );

        let nodes = self.nodes_in_domain(domain_b.0);
        let graph = &self.ingredients;

        // Prevent all shards of A from sending messages to B even once the connection is
        // regenerated. B won't send messages without receiving any messages from A first.
        // A will only start sending messages to B once it receives a ResumeAt.
        let mut domain_as = HashSet::new();
        let ingress_b1s = nodes.iter().filter(|&&ni| graph[ni].is_ingress());
        for &ingress_b1 in ingress_b1s {
            let egress_a = self.parent(ingress_b1);
            let domain_a = graph[egress_a].domain();
            for shard in 0..self.domains.get(&domain_a).unwrap().shards.len() {
                domain_as.insert((domain_a, shard));
            }

            let m = box Packet::RemoveChild { addr: domain_b };
            self.domains
                .get_mut(&domain_a)
                .unwrap()
                .send_to_healthy(m, &self.workers)
                .unwrap();
        }

        // Do a migration that regenerates the nodes from domain B in a new domain, which has the
        // same domain index as before. The nodes in the old and new domains also have the same
        // indexes. The domains can't start sending messages until tags and txs have been updated,
        // which the new domain B does not have.
        self.migrate(|mig| mig.replicate_domain(domain_b.0, domain_b.1, &nodes));

        // Set replay paths (tags) and information about which children the domain has (txs)
        // in B to the same as they were before.
        let graph = &self.ingredients;
        let exit_b = *nodes
            .iter()
            .filter(|&&ni| graph[ni].is_egress() || graph[ni].is_sharder())
            .next()
            .unwrap();
        if graph[exit_b].is_egress() {
            // TODO(ygina): shouldn't just be new_tag but also new_tx
            // TODO(ygina): also consider case where 2 sharded domains send to each other
            for m in self.materializations.get_update_egresses(domain_b.0) {
                self.domains
                    .get_mut(&domain_b.0)
                    .unwrap()
                    .send_to_healthy_shard(domain_b.1, m.clone().into_packet(), &self.workers)
                    .unwrap();
            }
        }
        let ingress_cs = self.ingredients
            .neighbors_directed(exit_b, petgraph::EdgeDirection::Outgoing)
            .collect::<Vec<_>>();
        if graph[exit_b].is_sharder() {
            for &ingress_c in &ingress_cs {
                let domain_c = graph[ingress_c].domain();
                let shards = self.domains.get(&domain_c).unwrap().shards.len();
                let txs = (0..shards)
                    .map(move |shard| (domain_c, shard))
                    .collect::<Vec<_>>();
                let m = box Packet::UpdateSharder {
                    node: graph[exit_b].local_addr(),
                    new_txs: (graph[ingress_c].local_addr(), txs),
                };
                // Sharders only have one shard themselves.
                self.domains
                    .get_mut(&domain_b.0)
                    .unwrap()
                    .send_to_healthy(m, &self.workers)
                    .unwrap();
            }
        }
        for m in self.materializations.get_setup_replay_paths(domain_b.0) {
            self.domains
                .get_mut(&domain_b.0)
                .unwrap()
                .send_to_healthy_shard(domain_b.1, m.clone().into_packet(), &self.workers)
                .unwrap();
        }

        // Initialize the waiting_on field in anticipation of getting ResumeAt messages to
        // forward. Then tell C about the NewIncoming connection from B so that B can tell
        // the controller all the necessary provenance information for recovery.
        let domain_cs = ingress_cs
            .iter()
            .map(|&ni| self.ingredients[ni].domain())
            .map(|domain| (domain, self.domains.get(&domain).unwrap().shards.len()))
            .flat_map(|(domain, shards)| (0..shards).map(move |shard| (domain, shard)))
            .collect::<HashSet<_>>();
        self.waiting_on.insert(domain_b, domain_cs.clone());
        for domain_a in domain_as {
            let mut waiting_on = HashSet::new();
            waiting_on.insert(domain_b);
            self.waiting_on.insert(domain_a, waiting_on);
        }
        for domain_c in domain_cs {
            self.send_new_incoming(domain_c, domain_b, Some(domain_b));
        }
    }

    /*
    /// Consider A -> B1 -> B2 -> C, where we lose B1.
    fn recover_lost_top_replica(
        &mut self,
        top: NodeIndex,
        ingress_b1: Vec<NodeIndex>,
        egress_b1: NodeIndex,
    ) {
        let ingress_b1 = ingress_b1[0];
        let domain_b1 = self.ingredients[top].domain();
        assert_eq!(domain_b1, self.ingredients[ingress_b1].domain());
        assert_eq!(domain_b1, self.ingredients[egress_b1].domain());

        // STEP 1: Contact the bottom replica B2 to become a node operator
        let bottom = match self.ingredients[top].replica_type() {
            Some(node::ReplicaType::Top{ bottom, .. }) => bottom,
            _ => unreachable!(),
        };
        let m = box Packet::MakeRecovery { node: self.ingredients[bottom].local_addr() };
        let domain_b2 = self.ingredients[bottom].domain();
        self.domains
            .get_mut(&domain_b2)
            .unwrap()
            .send_to_healthy(m, &self.workers).unwrap();

        // STEP 2: Clean up state in the egress of A about packets to B1.
        //
        // No node with multiple parents (UNION, JOIN) is stateful.
        let egress_a = self
            .ingredients
            .neighbors_directed(ingress_b1, petgraph::EdgeDirection::Incoming)
            .next()
            .unwrap();
        let m = box Packet::RemoveChild { child: ingress_b1 };
        let domain_a = self.ingredients[egress_a].domain();
        self.domains
            .get_mut(&domain_a)
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        // STEP 3: Link A and B2 together via a migration, which should create an egress tx from
        // A to B2 that doesn't have any tag information.
        //
        // Top replicas have exactly one child, the ingress of their bottom replica.
        let ingress_b2 = self
            .ingredients
            .neighbors_directed(egress_b1, petgraph::EdgeDirection::Outgoing)
            .next()
            .unwrap();
        let path = self.migrate(|mig| mig.link_nodes(egress_a, &vec![ingress_b2]));
        self.remove_nodes(&path[..]).unwrap();

        // STEP 4: Remove the tag that refers to the replay path from B1 to B2 from the domain
        // and the egress node. In memory state, replace it with the tag that refers to
        // the replay path from A to B1, since B2 has a new parent node.
        let mut old_tag = None;
        let mut new_tag = None;
        for m in self.materializations.get_setup_replay_paths(domain_b1) {
            match m.trigger {
                TriggerEndpoint::Start(..) => {
                    assert!(old_tag.is_none());
                    old_tag = Some(m.tag);
                },
                TriggerEndpoint::End(..) => {
                    assert!(new_tag.is_none());
                    new_tag = Some(m.tag);
                },
                _ => {},
            }
        }
        let m = box Packet::RemoveTag {
            old_tag: old_tag.unwrap(),
            new_state: Some((self.ingredients[bottom].local_addr(), new_tag.unwrap())),
        };
        self.domains
            .get_mut(&domain_b2)
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        // STEP 5: Resend any UpdateEgress messages to A that originally involved replay paths
        // through B1 and B2, but replace instances of the old tag with the new tag.
        for m in self.materializations.get_update_egresses(domain_b1) {
            let mut m = m.clone();
            if let Some((tag, _)) = &mut m.new_tag {
                assert!(m.new_tx.is_none());
                if *tag == old_tag.unwrap() {
                    *tag = new_tag.unwrap();
                }
                self.domains
                    .get_mut(&domain_a)
                    .unwrap()
                    .send_to_healthy(m.into_packet(), &self.workers)
                    .unwrap();
            }
        }

        // STEP 6: Resend any SetupReplayPath messages originally to B1 and that don't refer to
        // the tag of the replay path from B1 to B2.
        for m in self.materializations.get_setup_replay_paths(domain_b1) {
            if m.tag != old_tag.unwrap() {
                self.domains
                    .get_mut(&domain_b2)
                    .unwrap()
                    .send_to_healthy(m.clone().into_packet(), &self.workers)
                    .unwrap();
            }
        }

        // STEP 7: Send B2 a NewIncoming message and wait for ResumeAts to propagate.
        let mut resume_to = HashSet::new();
        self.send_new_incoming(domain_b2, domain_a, Some(domain_b1));
        resume_to.insert(domain_b2);
        self.waiting_on.insert(domain_a, resume_to);
    }

    /// Consider A -> B1 -> B2 -> C, where we lose B2.
    fn recover_lost_bottom_replica(
        &mut self,
        bottom: NodeIndex,
        ingress_b2: Vec<NodeIndex>,
        egress_b2: NodeIndex,
    ) {
        let ingress_b2 = ingress_b2[0];
        let domain_b2 = self.ingredients[bottom].domain();
        assert_eq!(domain_b2, self.ingredients[ingress_b2].domain());
        assert_eq!(domain_b2, self.ingredients[egress_b2].domain());
        // STEP 1: Contact the top replica B1 to no longer be a replica.
        let top = match self.ingredients[bottom].replica_type() {
            Some(node::ReplicaType::Bottom{ top, .. }) => top,
            _ => unreachable!(),
        };
        let m = box Packet::MakeRecovery { node: self.ingredients[top].local_addr() };
        let domain_b1 = self.ingredients[top].domain();
        self.domains
            .get_mut(&domain_b1)
            .unwrap()
            .send_to_healthy(m, &self.workers).unwrap();

        // STEP 2: Clean up state in the egress of B1 about packets to B2.
        //
        // Bottom replicas have exactly one parent, the egress of their top replica.
        let egress_b1 = self
            .ingredients
            .neighbors_directed(ingress_b2, petgraph::EdgeDirection::Incoming)
            .next()
            .unwrap();
        let m = box Packet::RemoveChild { child: ingress_b2 };
        let domain_b1 = self.ingredients[egress_b1].domain();
        self.domains
            .get_mut(&domain_b1)
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        // STEP 3: Link B1 and C together via a migration, which should create an egress tx from
        // B1 to C that doesn't have any tag information.
        let ingress_cs = self
            .ingredients
            .neighbors_directed(egress_b2, petgraph::EdgeDirection::Outgoing)
            .collect::<Vec<_>>();
        let path = self.migrate(|mig| mig.link_nodes(egress_b1, &ingress_cs));
        self.remove_nodes(&path[..]).unwrap();

        // STEP 4: Remove the tag that refers to the replay path from B1 to B2 from the domain
        // and the egress node. No replacement tag is necessary since B1 still has the same parent.
        //
        // TODO(ygina): If C is materialized, we might need to replace that tag in memory state?
        let mut old_tag = None;
        for m in self.materializations.get_setup_replay_paths(domain_b2) {
            match m.trigger {
                TriggerEndpoint::End(..) => {
                    assert!(old_tag.is_none());
                    old_tag = Some(m.tag);
                },
                _ => {},
            }
        }
        let m = box Packet::RemoveTag {
            old_tag: old_tag.unwrap(),
            new_state: None,
        };
        self.domains
            .get_mut(&domain_b1)
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        // STEP 5: Resend any UpdateEgress messages to B1 that originally involved replay paths
        // through B2 and C, since B1 will setup these replay paths.
        for m in self.materializations.get_update_egresses(domain_b2) {
            if let Some(_) = m.new_tag {
                assert!(m.new_tx.is_none());
                self.domains
                    .get_mut(&domain_b1)
                    .unwrap()
                    .send_to_healthy(m.clone().into_packet(), &self.workers)
                    .unwrap();
            }
        }

        // STEP 6: Resend any SetupReplayPath messages originally to B2 and that don't refer to
        // the tag of the replay path from B1 to B2. Use the cached replay paths to find the node
        // that triggers to B2.
        //
        // TODO(ygina): There may be a materialized node between B2 and last_domain.
        let mut tag_last_domain = vec![];
        for m in self.materializations.get_setup_replay_paths(domain_b2) {
            if m.tag != old_tag.unwrap() {
                match m.trigger {
                    TriggerEndpoint::Start(_) => {},
                    _ => unreachable!(),
                }
                let path = self.materializations.get_path(m.tag).unwrap();
                let last_node = path[path.len() - 1];
                let last_domain = self.ingredients[last_node].domain();
                tag_last_domain.push((m.tag, last_domain));

                self.domains
                    .get_mut(&domain_b1)
                    .unwrap()
                    .send_to_healthy(m.clone().into_packet(), &self.workers)
                    .unwrap();
            }
        }

        // STEP 6.5 (bottom replica case only): Update the trigger endpoint so upqueries to B2
        // contact B1 directly.
        for (tag, last_domain) in tag_last_domain {
            let mut sent = false;
            for m in self.materializations.get_setup_replay_paths(last_domain) {
                let mut m = m.clone();
                match m.trigger {
                    TriggerEndpoint::End(s, d) => {
                        if tag != m.tag {
                            continue;
                        }

                        assert!(!sent);
                        assert_eq!(d, domain_b2);
                        m.trigger = TriggerEndpoint::End(s, domain_b1);
                        sent = true;

                        self.domains
                            .get_mut(&last_domain)
                            .unwrap()
                            .send_to_healthy(m.into_packet(), &self.workers)
                            .unwrap();
                    },
                    _ => {},
                }
            }
            assert!(sent);
        }

        // STEP 7: Send all Cs a NewIncoming message and wait for ResumeAts to propagate.
        let mut resume_to = HashSet::new();
        for ingress_c in ingress_cs {
            let domain_c = self.ingredients[ingress_c].domain();
            self.send_new_incoming(domain_c, domain_b1, Some(domain_b2));
            resume_to.insert(domain_c);
        }
        self.waiting_on.insert(domain_b1, resume_to);
    }

    /// Recovers a domain with exactly one ingress, one egress, and one stateful replica.
    ///
    /// Consider A -> B1 -> B2 -> C. B1 and B2 are always materialized nodes (otherwise there
    /// is no reason to replicate them), and so there is always a replay path starting at B1 and
    /// ending at B2. Any domain on a replay path except the END domain will have a NodeIndex of
    /// an ingress node in the next domain in its egress. For example, A's egress will have the
    /// NodeIndex of B1's ingress on the reply path through A to B1. The END domain of any replay
    /// path will have a trigger endpoing to the START domain of that same path.
    fn recover_hot_spare(
        &mut self,
        ni: NodeIndex,
        failed_ingress: Vec<NodeIndex>,
        failed_egress: NodeIndex,
    ) {
        match self.ingredients[ni].replica_type() {
            Some(node::ReplicaType::Bottom { top, .. }) => {
                info!(
                    self.log,
                    "replacing failed bottom replica";
                    "bottom" => ni.index(),
                    "top" => top.index(),
                );
                self.recover_lost_bottom_replica(ni, failed_ingress, failed_egress);
            },
            Some(node::ReplicaType::Top { bottom, .. }) => {
                info!(
                    self.log,
                    "replacing failed top replica";
                    "bottom" => bottom.index(),
                    "top" => ni.index(),
                );
                self.recover_lost_top_replica(ni, failed_ingress, failed_egress);
            },
            None => unreachable!(),
        }
    }
    */

    /// Recovers a domain with only an ingress A and a reader B1.
    fn recover_reader(&mut self, ingress: NodeIndex, reader: NodeIndex) {
        /*
        let domain_b = self.ingredients[ingress].domain();
        assert_eq!(domain_b, self.ingredients[reader].domain());

        // prevent A from sending messages to B1 even once the connection is regenerated.
        let egress_a = self.parent(ingress);
        let domain_a = self.ingredients[egress_a].domain();
        let m = box Packet::RemoveChild { child: ingress, domain: domain_b };
        self.domains
            .get_mut(&domain_a)
            .unwrap()
            .send_to_healthy(m, &self.workers)
            .unwrap();

        // remove tags on replay paths involving domain B1 since materializations were
        // recreated in the previous migration.
        let tags = self.materializations
            .get_setup_replay_paths(domain_b)
            .iter()
            .map(|m| m.tag)
            .collect::<Vec<_>>();

        // do a migration that regenerates the nodes from domain B1 in domain B2.
        self.migrate(|mig| mig.replicate_nodes(&vec![ingress, reader]));

        for tag in tags {
            let path = self.materializations.get_path(tag).unwrap();
            let domains = path
                .iter()
                .map(|&ni| self.ingredients[ni].domain())
                .collect::<HashSet<_>>();
            for domain in &domains {
                // TODO(ygina): should get ack from RemoveTag before continuing
                let m = box Packet::RemoveTag { old_tag: tag, new_state: None };
                self.domains
                    .get_mut(&domain)
                    .unwrap()
                    .send_to_healthy(m, &self.workers)
                    .unwrap();
            }
        }

        // don't bother trying to recover reader state... just let there be a replay
        */
        unimplemented!();
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
        to: ReplicaAddr,
        new_incoming: ReplicaAddr,
        old_incoming: Option<ReplicaAddr>,
    ) {
        let old_incoming = old_incoming.unwrap_or(new_incoming);

        debug!(
            self.log,
            "notifying domain {}.{} of new incoming connection {}.{} to replace {}.{}",
            to.0.index(),
            to.1,
            old_incoming.0.index(),
            old_incoming.1,
            new_incoming.0.index(),
            new_incoming.1,
        );

        let (domain, shard) = to;
        let dh = self.domains.get_mut(&domain).unwrap();
        let m = box Packet::NewIncoming {
            old: old_incoming,
            new: new_incoming,
        };
        dh.send_to_healthy_shard(shard, m, &self.workers).unwrap();
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

    pub(crate) fn handle_ack_new_incoming(
        &mut self,
        from: ReplicaAddr,
        updates: Vec<Provenance>,
        min_provenance: Provenance,
    ) {
        assert!(self.waiting_on.len() > 0, "in recovery mode");
        self.provenance.insert(from, (min_provenance, updates));

        // We're no longer waiting on the node that acked the NewIncoming message
        self.handle_ack_resume_at(from);
    }

    /// Populate recovery fields using provenance and update information received in AckNewIncoming
    /// messages once all such messages have been received.
    ///
    /// Returns the updates to send with this message... basically an empty vec unless we just
    /// populated the fields.
    fn acked_all_new_incoming(&mut self) -> (Option<Provenance>, Vec<Provenance>) {
        if self.provenance.is_empty() {
            // This function has already been called.
            return (None, vec![]);
        }

        // Determine the limiting factor for which we request upstream replicas to resume.
        // Then apply all updates up to the minimum max_label, inclusive.
        let mut min_max_label = std::usize::MAX;
        for (min_provenance, updates) in self.provenance.values() {
            let max_label = if updates.is_empty() {
                min_provenance.label()
            } else {
                updates[updates.len() - 1].label()
            };
            if max_label < min_max_label {
                min_max_label = max_label;
            }
        }
        for (_, (min_provenance, updates)) in self.provenance.iter_mut() {
            while !updates.is_empty() {
                if updates[0].label() > min_max_label {
                    break;
                }
                min_provenance.apply_update(&updates.remove(0));
            }
        }

        // Use the values of the remaining min_provenances to determine which values to resume at.
        // Since we applied the updates from the min_provenance, the min_provenances here should
        // reflect the total provenance of the graph (even though some upstream updates are
        // dropped). Though I'm not really sure what replays do. We might need to store those in
        // the updates too?
        //
        // For each child of the failed domain, resume at 1+ the max_label using the last update.
        for (&child, (min_provenance, updates)) in self.provenance.iter() {
            let resume_at = if updates.is_empty() {
                min_provenance.label() + 1
            } else {
                updates[updates.len() - 1].label() + 1
            };
            self.resume_ats
                .entry(min_provenance.root())
                .or_insert(vec![])
                .push((child, resume_at));
        }

        // For upstream nodes, resume at 1+ the maximum label of the unions of all min_provenance.
        assert_eq!(self.resume_ats.len(), 1);
        let failed_replica = *self.resume_ats.keys().next().unwrap();
        let mut max_union = Provenance::new(failed_replica, 0);
        for (min_provenance, _) in self.provenance.values() {
            max_union.max_union(min_provenance);
        }
        let mut queue = vec![];
        for grandparent in max_union.edges().values() {
            queue.push((failed_replica, grandparent));
        }
        while !queue.is_empty() {
            let (addr, parent) = queue.remove(0);
            let parent_root = parent.root();
            if !self.waiting_on.contains_key(&parent_root) {
                continue;
            }
            self.resume_ats
                .entry(parent_root)
                .or_insert(vec![])
                .push((addr, parent.label() + 1));
            for grandparent in parent.edges().values() {
                queue.push((parent_root, grandparent));
            }
        }

        // Consolidate all remaining updates to send to the limiting replica.
        let mut updates_to_send = self.provenance
            .drain()
            .flat_map(|(_, (_, updates))| updates)
            .collect::<Vec<_>>();
        updates_to_send.sort_by_key(|p| p.label());
        updates_to_send.dedup_by_key(|update| update.label());
        (Some(max_union), updates_to_send)
    }

    pub(crate) fn handle_ack_resume_at(&mut self, from: ReplicaAddr) {
        // Update waiting on lists of nodes that were waiting on "from"
        let mut empty = vec![];
        for (waiting, on) in self.waiting_on.iter_mut() {
            let removed = on.remove(&from);
            if removed && on.len() == 0 {
                empty.push(waiting.clone());
            }
        }

        // If those nodes aren't waiting on anyone else, send ResumeAts to them. Send ResumeAt
        // information for all nodes in a single message so the sender domain can update its
        // state atomically.
        for addr in empty {
            assert!(self.waiting_on.remove(&addr).is_some());

            // Convert the indexed resume at information into ResumeAt messages.
            let (min_provenance, targets) = self.acked_all_new_incoming();
            let addr_labels = self.resume_ats.remove(&addr).unwrap();

            // Send the message!
            let m = box Packet::ResumeAt { addr_labels, min_provenance, targets };
            self.domains
                .get_mut(&addr.0)
                .unwrap()
                .send_to_healthy_shard(addr.1, m, &self.workers)
                .unwrap();
        }
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
        materializations.set_frontier_strategy(state.config.frontier_strategy);

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
            domain_graph: petgraph::Graph::new(),
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
            domain_nodes: Default::default(),
            channel_coordinator: cc,
            debug_channel: None,
            epoch: state.epoch,

            remap: HashMap::default(),

            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            pending_recovery,
            last_checked_workers: Instant::now(),

            replies: DomainReplies(drx),

            provenance: Default::default(),
            waiting_on: Default::default(),
            resume_ats: Default::default(),
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
            w.domains.push(domain.index);
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
        for (wid, endpoint) in self.workers.iter_mut() {
            for &dd in &announce {
                if endpoint
                    .sender
                    .send(CoordinationMessage {
                        epoch: self.epoch,
                        source: endpoint.sender.local_addr().unwrap(),
                        payload: CoordinationPayload::DomainBooted(dd),
                    })
                    .is_err() {
                    warn!(
                        log,
                        "failed to tell worker {} about new domain {}.{}",
                        wid,
                        dd.domain().index(),
                        dd.shard(),
                    );
                }
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
                (base.name().to_owned(), n)
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

    fn find_view_for(&self, node: NodeIndex, name: &str) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        let mut bfs = Bfs::new(&self.ingredients, node);
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child]
                .with_reader(|r| r.is_for() == node)
                .unwrap_or(false)
                && self.ingredients[child].name() == name
            {
                return Some(child);
            }
        }
        None
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

        self.find_view_for(node, name).map(|r| {
            let domain = self.ingredients[r].domain();
            let columns = self.ingredients[r].fields().to_vec();
            let schema = self.view_schema(r);
            let shards = (0..self.domains[&domain].shards())
                .map(|i| self.read_addrs[&self.domains[&domain].assignment(i)])
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
            .map(|i| schema::column_schema(&self.ingredients, view_ni, &self.recipe, i, &self.log))
            .collect();

        if schema.iter().any(Option::is_none) {
            None
        } else {
            Some(schema.into_iter().map(Option::unwrap).collect())
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
            addr: node.local_addr(),
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
        trace!(self.log, "asked to get statistics");
        let log = &self.log;
        let workers = &self.workers;
        let replies = &mut self.replies;
        // TODO: request stats from domains in parallel.
        let domains = self
            .domains
            .iter_mut()
            .flat_map(|(&di, s)| {
                trace!(log, "requesting stats from domain"; "di" => di.index());
                let mut num_healthy = 0;
                for i in 0..s.shards.len() {
                    if s.send_to_healthy_shard(i, box Packet::GetStatistics, workers).is_ok() {
                        num_healthy += 1;
                    } else {
                        // ignore unhealthy shards
                    }
                }

                replies.wait_for_statistics(num_healthy).into_iter().enumerate().map(
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

    fn get_instances(&self) -> Vec<(WorkerIdentifier, bool, Duration, Vec<DomainIndex>)> {
        self.workers
            .iter()
            .map(|(&id, ref status)| {
                (id, status.healthy, status.last_heartbeat.elapsed(), status.domains.clone())
            })
            .collect()
    }

    fn shutdown_worker(&mut self, instance_addr: String) -> Result<(), String> {
        let instance_addr: WorkerIdentifier = instance_addr
            .parse()
            .expect("unable to parse socket address");
        let w = self.workers.get_mut(&instance_addr).unwrap();
        let src = w.sender.local_addr().unwrap();
        w.sender
            .send(CoordinationMessage {
                epoch: self.epoch,
                source: src,
                payload: CoordinationPayload::Shutdown,
            })
            .map_err(|_| format!("failed to shutdown {:?}", instance_addr))
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
                    .wait_for_statistics(s.shards.len())
                    .into_iter()
                    .flat_map(move |(_, node_stats)| {
                        node_stats
                            .into_iter()
                            .filter_map(|(ni, ns)| match ns.materialized {
                                MaterializationStatus::Partial { .. } => Some((ni, ns.mem_size)),
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

    // Computes the domain graph from the current ingredients.
    //
    // By domain we actually mean a replica address, which includes both the domain index and the
    // shard. All nodes assigned to the replica address must be contiguous in the original graph.
    // Typically, shard mergers and sharders are in the same replica. These replicas have multiple
    // parents (the shards of the parent domain) and multiple children (the shards of the child
    // domain).
    pub(crate) fn compute_domain_graph(&mut self) {
        let ingredients = &self.ingredients;
        let mut g = petgraph::Graph::new();

        // add all replica addresses as nodes
        let mut sharding: HashMap<DomainIndex, usize> = HashMap::new();
        let mut nodes: HashMap<(DomainIndex, usize), _> = HashMap::new();
        for ni in ingredients.node_indices() {
            let node = &ingredients[ni];
            if !node.has_domain() || node.is_ingress() || node.is_egress() || node.is_dropped() {
                continue;
            }

            let domain = node.domain();
            let shards = node.sharded_by().shards().unwrap_or(1);
            if let Some(shards_old) = sharding.get(&domain) {
                assert_eq!(shards, *shards_old);
            } else {
                sharding.insert(domain, shards);
                for i in 0..shards {
                    let ni = g.add_node((domain, i));
                    nodes.insert((domain, i), ni);
                }
            }
        }

        // create an edge between each shard in a domain to each shard in another domain
        // if there are nodes in those domains with an edge
        for ei in ingredients.edge_indices() {
            let (source_ni, target_ni) = ingredients.edge_endpoints(ei).unwrap();
            if !ingredients[source_ni].has_domain() || !ingredients[target_ni].has_domain() {
                continue;
            }

            let source = ingredients[source_ni].domain();
            let target = ingredients[target_ni].domain();
            if source == target {
                continue;
            }

            for i in 0..*sharding.get(&source).unwrap() {
                let s = nodes.get(&(source, i)).unwrap();
                for j in 0..*sharding.get(&target).unwrap() {
                    let t = nodes.get(&(target, j)).unwrap();
                    g.add_edge(*s, *t, ());
                }
            }
        }

        self.domain_graph = g;
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
                .or_insert_with(Vec::new)
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
        for d in self.domains.values_mut() {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send_to_healthy(box Packet::Quit, &self.workers));
            }
        }
    }
}
