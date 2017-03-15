//! Functions for assigning new nodes to thread domains.
//!
//! This includes tracking all new nodes assigned to each domain, and assigning them domain-local
//! identifiers.

use flow::domain;
use flow::Migration;

use petgraph::graph::NodeIndex;
use petgraph::visit::{EdgeRef, Dfs};
use petgraph::Incoming;
use num_cpus;

use std::collections::{HashSet, HashMap};

pub struct DomainAssignment {
    domains: HashMap<domain::Index, HashSet<NodeIndex>>,
    // max number of domains
    num_domains: u8,
}

impl DomainAssignment {
    pub fn new() -> DomainAssignment {
        DomainAssignment {
            domains: HashMap::new(),
            num_domains: num_cpus::get() as u8,
        }
    }

    pub fn num_domains(&mut self, num_domains: u8) -> &mut DomainAssignment {
        self.num_domains = num_domains;
        self
    }

    pub fn get_num_domains(&self) -> u8 {
        self.num_domains
    }

    pub fn add_to_domain(&mut self, idx: domain::Index, nx: NodeIndex) {
        if !self.domains.contains_key(&idx) {
            self.domains.insert(idx, HashSet::new());
        }
        let unwrapped = self.domains.get_mut(&idx).unwrap();
        unwrapped.insert(nx);
    }
}

pub fn divvy_up_graph(_mig: &mut Migration, _new: &HashSet<NodeIndex>)
    -> Vec<(NodeIndex, domain::Index)> {
        let mut sorted_subgraphs: Vec<Vec<NodeIndex>> = Vec::new();
        let max_domains;
        let current_domains;
        let mut new_domains_map: HashMap<domain::Index, HashSet<NodeIndex>>;
        {
            let ref _graph = _mig.mainline.ingredients;
            // find the roots of all the new subgraphs
            let mut roots: HashSet<NodeIndex> = _new.clone();
            for ni in _new {
                let incoming = _graph.edges_directed(*ni, Incoming);
                for inc_edge in incoming {
                    let src = inc_edge.source();
                    if _new.contains(&src) {
                        roots.remove(ni);
                        break
                    }
                }
            }
            // subgraphs sorted by increasing cardinality
            for root in roots {
                let mut subtree: Vec<NodeIndex> = Vec::new();
                let mut dfs = Dfs::new(&_graph, root);
                while let Some(nx) = dfs.next(&_graph) {
                    subtree.push(nx);
                }
                sorted_subgraphs.push(subtree);
            }
            sorted_subgraphs.sort_by_key(|k| k.len());


            new_domains_map = _mig.mainline.assignment.domains.clone();
            current_domains = _mig.mainline.assignment.domains.len();
            max_domains = _mig.mainline.assignment.get_num_domains();
        }
        let mut result: Vec<(NodeIndex, domain::Index)> = Vec::new();
        let mut new_domains = 0;
        while current_domains + new_domains < max_domains as usize && sorted_subgraphs.len() > 0 {
            let subgraph: Vec<NodeIndex> = sorted_subgraphs.pop().unwrap();
            let dx = _mig.add_domain();
            new_domains_map.insert(dx, HashSet::new());
            new_domains += 1;
            for nx in subgraph {
                new_domains_map.get_mut(&dx).unwrap().insert(nx);
                result.push((nx, dx));
            }
        }

        let mut sorted_estb_subgraphs: Vec<(domain::Index, i32)> = Vec::new();
        for pair in new_domains_map.iter() {
            sorted_estb_subgraphs.push((*(pair.0), pair.1.len() as i32));
        }
        // negated for reverse order
        sorted_estb_subgraphs.sort_by_key(|k| -k.1);
        while sorted_subgraphs.len() > 0 {
            let subgraph: Vec<NodeIndex> = sorted_subgraphs.pop().unwrap();
            let last = sorted_estb_subgraphs.pop().unwrap();
            sorted_estb_subgraphs.push((last.0, last.1 + subgraph.len() as i32));
            for nx in subgraph {
                result.push((nx, last.0));
            }
            // TODO binary search insert instead of sorting every time
            sorted_estb_subgraphs.sort_by_key(|k| -k.1);
        }

        result
    }

pub fn assign_domains(mig: &mut Migration, new: &HashSet<NodeIndex>) {
    // Compute domain assignments using graph
    let new_assignments = divvy_up_graph(mig, new);
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
        mig.mainline.assignment.add_to_domain(domain, *node);
    }
}
