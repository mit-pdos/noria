//! Functions for starting up a *new* domain.
//!
//! This includes constructing local identifiers for nodes, construcing domain-local structures
//! such as `DomainNodes`, and initializing transaction handling.

use channel;
use flow::prelude::*;
use flow::domain::single;
use flow::domain;
use flow::checktable;
use souplet::Souplet;

use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::cell;
use std::thread;

use slog::Logger;

fn build_descriptors(graph: &mut Graph, nodes: Vec<(NodeIndex, bool)>) -> DomainNodes {
    nodes
        .into_iter()
        .map(|(ni, _)| single::NodeDescriptor::new(graph, ni))
        .map(|nd| (*nd.addr().as_local(), cell::RefCell::new(nd)))
        .collect()
}

pub fn can_be_remote(graph: &Graph, nodes: &Vec<(NodeIndex, bool)>) -> bool {
    for &(ni, _) in nodes {
        if graph[ni].is_internal() && graph[ni].get_base().is_some() {
            return false;
        }
    }
    true
}

pub fn boot_new(log: Logger,
                index: domain::Index,
                graph: &mut Graph,
                nodes: Vec<(NodeIndex, bool)>,
                checktable: Arc<Mutex<checktable::CheckTable>>,
                txs: &mut HashMap<domain::Index, channel::PacketSender>,
                input_rx: mpsc::Receiver<Packet>,
                ts: i64)
                -> thread::JoinHandle<()> {
    let (tx, rx) = mpsc::sync_channel(1);
    txs.insert(index, tx.into());

    let nodes = build_descriptors(graph, nodes);
    let domain = domain::Domain::new(log, index, nodes, checktable, ts);
    domain.boot(rx, input_rx)
}

pub fn boot_remote(index: domain::Index,
                   graph: &mut Graph,
                   nodes: Vec<(NodeIndex, bool)>,
                   souplet: &mut Souplet,
                   txs: &mut HashMap<domain::Index, channel::PacketSender>) -> SocketAddr {
    let peer = &souplet.get_peers().next().unwrap();
    let nodes = build_descriptors(graph, nodes);
    let tx = souplet.start_domain(peer, index, nodes);
    txs.insert(index, tx);

    *peer.clone()
}
