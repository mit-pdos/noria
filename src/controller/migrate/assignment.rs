//! Functions for assigning new nodes to thread domains.

use dataflow::prelude::*;
use slog::Logger;
use std::collections::HashSet;
use petgraph;

pub fn assign(
    log: Logger,
    graph: &mut Graph,
    source: NodeIndex,
    new: &HashSet<NodeIndex>,
    ndomains: &mut usize,
    fixed_domains: bool,
) {
    // we need to walk the data flow graph and assign domains to all new nodes.
    // we use a couple of heuristics to pick and assignment:
    //
    //  - the child of a Sharder is always in a different domain from the sharder
    //  - shard merge nodes are never in the same domain as their sharded ancestors
    //  - reader nodes are always in the same domain as their immediate ancestor
    //  - base nodes start a new domain
    //  - aggregations are placed in new domains
    //  - all other nodes are in the same domain as their parent
    //

    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&*graph);
    while let Some(node) = topo.next(&*graph) {
        if node == source {
            continue;
        }
        if graph[node].is_dropped() {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }

    let mut cur_domain = 0;
    let mut next_domain = || {
        if fixed_domains {
            cur_domain = (cur_domain + 1) % *ndomains;
            cur_domain
        } else {
            *ndomains += 1;
            *ndomains - 1
        }
    };

    for node in topo_list {
        let assignment = {
            let n = &graph[node];
            let ps: Vec<_> = graph
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .map(|ni| (ni, &graph[ni]))
                .collect();

            if ps.iter().any(|&(_, ref p)| p.is_sharder()) {
                // child of a sharder
                // TODO: this is stupid -- assign to some domain that already exists under the
                // sharder if possible.
                next_domain()
            } else if n.is_sharder() {
                // sharder belongs to parent domain
                ps[0].1.domain().index()
            } else if n.sharded_by().is_none()
                && ps.iter().any(|&(_, ref p)| !p.sharded_by().is_none())
            {
                // shard merger
                next_domain()
            } else if n.is_reader() {
                // reader can be in its own domain
                next_domain()
            } else if n.is_internal() {
                match **n {
                    NodeOperator::Base(..) => {
                        // base nodes start new domains
                        next_domain()
                    }
                    NodeOperator::Sum(..)
                    | NodeOperator::Extremum(..)
                    | NodeOperator::Concat(..) => {
                        // aggregation
                        next_domain()
                    }
                    _ => {
                        // "all other nodes", but only internal
                        // prefer new
                        if let Some(&(_, ref p)) =
                            ps.iter().find(|&&(ref pni, _)| new.contains(pni))
                        {
                            p.domain().index()
                        } else {
                            ps[0].1.domain().index()
                        }
                    }
                }
            } else {
                // actually all other nodes
                ps[0].1.domain().index()
            }
        };

        debug!(log, "node added to domain";
           "node" => node.index(),
           "type" => ?graph[node],
           "domain" => ?assignment);
        graph[node].add_to(assignment.into());
    }
}
