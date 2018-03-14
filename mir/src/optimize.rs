use MirNodeRef;
use node::{MirNode, MirNodeType};
use query::MirQuery;
use dataflow::ops::filter::FilterCondition;

use std::collections::HashMap;

pub fn optimize(q: MirQuery) -> MirQuery {
    // impose deterministic filter order to increase reuse
    canonicalize_filters(&q);

    // drop unnecessary projection operators (e.g., those which just project all of the parent
    // columns, or ones that follow operators that can themselves project, such as joins).
    //remove_extraneous_projections(&mut q);

    q
}

pub fn optimize_post_reuse(_q: &mut MirQuery) {
    // find_and_merge_filter_chains(q);
}

fn canonicalize_filters(q: &MirQuery) {
    // traverse query, collect all filters connected with the same logical operator.
    // We can swap the order of any filters applied in a direct chain, but not move filters
    // between parallel chains.
    let mut chains: Vec<Vec<_>> = Vec::new();

    // depth first search to locate AND-ed filter chains
    // TODO(malte): this only covers simple, non-nested chains for now
    let mut node_stack = Vec::new();
    node_stack.extend(q.roots.iter().cloned());
    let mut visited_nodes = HashMap::new();
    let mut active_chain = None;
    while !node_stack.is_empty() {
        let n = node_stack.pop().unwrap();
        let node_name = n.borrow().versioned_name();
        let mut end_chain = false;
        if visited_nodes.contains_key(&node_name) {
            continue;
        }

        visited_nodes.insert(node_name, true);

        match n.borrow().inner {
            MirNodeType::Filter { .. } => {
                if let None = active_chain {
                    active_chain = Some(Vec::new());
                }

                if active_chain.as_ref().unwrap().is_empty() {
                    active_chain.as_mut().unwrap().push(n.clone());
                } else if n.borrow().ancestors.len() == 1 {
                    active_chain.as_mut().unwrap().push(n.clone());
                } else {
                    chains.push(active_chain.take().unwrap());
                }

                if n.borrow().children.len() != 1 {
                    chains.push(active_chain.take().unwrap());
                }
            }
            _ => {
                // we need this because n most likely will be a children of
                // last_node. if that's the case, mutably borrowing the
                // child in end_filter_chain will cause a BorrowMutError
                // because it was already borrowed in the match.
                end_chain = true;
            }
        }

        if end_chain && active_chain.is_some() {
            chains.push(active_chain.take().unwrap());
        }

        for child in n.borrow().children.iter() {
            node_stack.push(child.clone());
        }
    }

    // now reorder the chains in canonical order (alphabetical by filter column)
    println!("chains: {:?}", chains);
    for c in chains {
        if c.len() <= 1 {
            // nothing to reorder
            continue;
        }
        let mut i = 0;
        while i + 1 < c.len() {
            use std::cmp::Ordering;

            let mut n1 = c[i].borrow_mut();
            let mut n2 = c[i + 1].borrow_mut();
            let conditions1 = match n1.inner {
                MirNodeType::Filter { ref conditions } => conditions.clone(),
                _ => unreachable!(),
            };
            let conditions2 = match n2.inner {
                MirNodeType::Filter { ref conditions } => conditions.clone(),
                _ => unreachable!(),
            };

            //
            let c1: Vec<_> = conditions1
                .iter()
                .enumerate()
                .map(|(i, cc)| (n1.columns[i].clone(), cc))
                .filter(|cc| cc.1.is_some())
                .collect();
            let c2: Vec<_> = conditions2
                .iter()
                .enumerate()
                .map(|(i, cc)| (n1.columns[i].clone(), cc))
                .filter(|cc| cc.1.is_some())
                .collect();
            assert_eq!(c1.len(), 1);
            assert_eq!(c2.len(), 1);

            match c1[0].0.name.cmp(&c2[0].0.name) {
                Ordering::Greater => {
                    // swap the nodes!
                    println!("hoist filter on {:?} above {:?}", c2[0], c1[0]);
                    // also need to update other adjacent nodes
                    let n1_children_tmp = n1.children.clone();
                    let n2_ancestors_tmp = n2.ancestors.clone();
                    for an in n1.ancestors.iter_mut() {
                        an.borrow_mut().children = n1_children_tmp.clone();
                    }
                    for cn in n2.children.iter_mut() {
                        cn.borrow_mut().ancestors = n2_ancestors_tmp.clone();
                    }

                    n1.children = n2.children.clone();
                    n2.children = n2.ancestors.clone();
                    n2.ancestors = n1.ancestors.clone();
                    n1.ancestors = n1_children_tmp.clone();
                }
                _ => (),
            }

            i += 1;
        }
    }
}

#[allow(dead_code)]
fn find_and_merge_filter_chains(q: &MirQuery) {
    let mut chained_filters = Vec::new();
    // depth first search
    let mut node_stack = Vec::new();
    node_stack.extend(q.roots.iter().cloned());

    let mut visited_nodes = HashMap::new();

    while !node_stack.is_empty() {
        let n = node_stack.pop().unwrap();
        let node_name = n.borrow().versioned_name();
        let mut end_chain = false;
        if visited_nodes.contains_key(&node_name) {
            continue;
        }

        visited_nodes.insert(node_name, true);

        match n.borrow().inner {
            MirNodeType::Filter { .. } => {
                try_add_node_to_chain(&n, &mut chained_filters);
            }
            _ => {
                // we need this because n most likely will be a children of
                // last_node. if that's the case, mutably borrowing the
                // child in end_filter_chain will cause a BorrowMutError
                // because it was already borrowed in the match.
                end_chain = true;
            }
        }

        if end_chain {
            end_filter_chain(&mut chained_filters);
        }

        for child in n.borrow().children.iter() {
            node_stack.push(child.clone());
        }
    }
}

fn try_add_node_to_chain(node: &MirNodeRef, chained_filters: &mut Vec<MirNodeRef>) {
    // any filter node can start a new chain
    if chained_filters.is_empty() {
        chained_filters.push(node.clone());
    } else if node.borrow().ancestors.len() == 1 {
        chained_filters.push(node.clone());
    } else {
        end_filter_chain(chained_filters);
        return;
    }

    if node.borrow().children.len() != 1 {
        end_filter_chain(chained_filters);
    }
}

fn end_filter_chain(chained_filters: &mut Vec<MirNodeRef>) {
    use std::cmp::max;

    if chained_filters.len() < 2 {
        chained_filters.clear();
        return;
    }

    {
        let first_node = chained_filters.first().unwrap();
        let last_node = chained_filters.last().unwrap();
        let schema_version = first_node.borrow().from_version.clone();

        let name = chained_filters.iter().fold(
            "merged_filter_".to_string(),
            |mut acc, ref node| {
                acc.push_str(node.borrow().name());
                acc
            },
        );

        let prev_node = first_node.borrow().ancestors.first().unwrap().clone();
        let fields: Vec<_> = prev_node.borrow().columns().iter().cloned().collect();
        let width = chained_filters.iter().fold(0, |mut acc, ref node| {
            let w = match node.borrow().inner {
                MirNodeType::Filter { ref conditions } => conditions.len(),
                _ => 0,
            };
            acc = max(acc, w);
            acc
        });
        let merged_conditions = to_conditions(chained_filters, width);

        let merged_filter = MirNode::new(
            name.as_str(),
            schema_version,
            fields,
            MirNodeType::Filter {
                conditions: merged_conditions.clone(),
            },
            vec![prev_node],
            vec![],
        );

        for ancestor in &first_node.borrow().ancestors {
            ancestor.borrow_mut().remove_child(first_node.clone());
        }

        first_node.borrow_mut().ancestors.clear();

        for child in &last_node.borrow().children {
            merged_filter.borrow_mut().add_child(child.clone());
            child.borrow_mut().add_ancestor(merged_filter.clone());
            child.borrow_mut().remove_ancestor(last_node.clone());
        }
    }

    chained_filters.clear();
}

fn to_conditions(
    chained_filters: &Vec<MirNodeRef>,
    num_columns: usize,
) -> Vec<Option<FilterCondition>> {
    let mut merged_conditions = vec![None; num_columns];
    for filter in chained_filters {
        match filter.borrow().inner {
            MirNodeType::Filter { ref conditions } => {
                // Note that this assumes that there is only ever one column being filtered on for
                // each filter that is being merged.
                let i = conditions.iter().position(|c| c.is_some()).unwrap();
                merged_conditions[i] = conditions[i].clone();
            }
            _ => unreachable!(),
        }
    }

    merged_conditions
}

// currently unused
#[allow(dead_code)]
fn remove_extraneous_projections(_q: &mut MirQuery) {
    unimplemented!()
}
