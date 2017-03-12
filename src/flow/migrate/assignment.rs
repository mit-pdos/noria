//! Functions for assigning new nodes to thread domains.
//!
//! This includes tracking all new nodes assigned to each domain, and assigning them domain-local
//! identifiers.

use flow::prelude::*;
use flow::domain;
use flow::Migration;

use petgraph::graph::NodeIndex;

use std::collections::HashSet;

use slog::Logger;

pub fn divvy_up_graph(_log: &Logger,
                      _graph: &Graph,
                      _source: NodeIndex,
                      _new: &HashSet<NodeIndex>)
                      -> Vec<(NodeIndex, domain::Index)> {
    // TODO(malte): look at `graph` here to derive smarter assignments
    vec![]
}

pub fn assign_domains(mig: &mut Migration, new: &HashSet<NodeIndex>) {
    // Compute domain assignments using graph
    let new_assignments = divvy_up_graph(&mig.log,
                                         &mig.mainline.ingredients,
                                         mig.mainline.source,
                                         new);
    for (ni, d) in new_assignments {
        mig.added.insert(ni, Some(d));
    }

    // Apply domain assignments
    for node in new {
        let domain = mig.added[node].unwrap_or_else(|| {
            // new node that doesn't belong to a domain
            // create a new domain just for that node
            let d = mig.add_domain();
            trace!(mig.log, "unassigned node automatically added to domain"; "node" => node.index(),
                   "domain" => d.index());
            d
        });
        mig.mainline.ingredients[*node].add_to(domain);
    }
}
