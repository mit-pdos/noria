extern crate serde_json;

use flow;
use ops::Update;
use query::{DataType, Query};

use std::collections::HashMap;
use std::io::Read;
use std::string::String;

#[derive(Deserialize, Debug)]
struct JdlBase {
    name: String,
    outputs: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct JdlJoin {
    name: String,
    from: Vec<String>,
    emit: Vec<(usize, usize)>,
    on: Vec<Vec<usize>>,
    outputs: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct JdlIdentity {
    name: String,
    from: String,
    emit: Option<Vec<usize>>,
    outputs: Vec<String>,
    having: Option<String>,
}

#[derive(Deserialize, Debug)]
struct JdlPermute {
    name: String,
    from: String,
    emit: Vec<usize>,
    outputs: Vec<String>,
}

#[derive(Deserialize, Debug)]
enum JdlAggregation {
    COUNT,
    SUM,
}

#[derive(Deserialize, Debug)]
struct JdlGroup {
    name: String,
    from: String,
    func: JdlAggregation,
    over: Option<usize>,
    by: Vec<usize>,
    outputs: Vec<String>,
}

#[derive(Deserialize, Debug)]
enum JdlNode {
    Base(JdlBase),
    Join(JdlJoin),
    Permute(JdlPermute),
    Identity(JdlIdentity),
    Group(JdlGroup)
}

trait NodeBase<'a> {
    fn name(&'a self) -> &'a str;
    fn outputs(&'a self) -> Vec<&'a str>;
}

macro_rules! node_base {
    ($node:ident) => {
        impl<'a> NodeBase<'a> for $node {
            fn name(&'a self) -> &'a str {
                &*self.name
            }

            fn outputs(&'a self) -> Vec<&'a str> {
                self.outputs.iter().map(|s| &**s).collect()
            }
        }
    }
}

node_base!(JdlBase);
node_base!(JdlJoin);
node_base!(JdlIdentity);
node_base!(JdlPermute);
node_base!(JdlGroup);

impl JdlNode {
    fn node(&self) -> Box<&NodeBase> {
        match self {
            &JdlNode::Base(ref b) => Box::new(b),
            &JdlNode::Join(ref j) => Box::new(j),
            &JdlNode::Permute(ref p) => Box::new(p),
            &JdlNode::Identity(ref i) => Box::new(i),
            &JdlNode::Group(ref g) => Box::new(g),
        }
    }
}

/// Convert JSON to a FlowGraph.
pub fn parse<R>(file: R) -> flow::FlowGraph<Query, Update, Vec<DataType>>
    where R: Read
{
    use super::*;

    let jdl_nodes: Vec<JdlNode> = serde_json::from_reader(file).unwrap();
    let mut graph = flow::FlowGraph::new();
    let mut nodes: HashMap<String, flow::NodeIndex> = HashMap::new();

    for jdl_node in jdl_nodes {
        let name = jdl_node.node().name().to_string();

        let index = {
            let lookup = |n: &str| {
                nodes.get(n).expect(&format!(
                    "while constructing {}, unable to find {}", name, n)).to_owned()
            };

            match jdl_node {
                JdlNode::Base(b) => {
                    graph.incorporate(new(b.name(), &b.outputs(), true, Base {}))
                }
                JdlNode::Identity(i) => {
                    let from_idx = lookup(&i.from);
                    let identity = Identity::new(from_idx);
                    graph.incorporate(new(i.name(), &i.outputs(), true, identity))
                }
                JdlNode::Permute(p) => {
                    let from_idx = lookup(&p.from);
                    let permuter = Permute::new(from_idx, &p.emit[..]);
                    graph.incorporate(new(p.name(), &p.outputs(), true, permuter))
                }
                JdlNode::Group(g) => {
                    let from_idx = lookup(&g.from);
                    let (agg, over) = match g.func {
                        // COUNTs ignore the `over` column, so we pass in a dummy index.
                        JdlAggregation::COUNT => (Aggregation::COUNT, usize::max_value()),
                        JdlAggregation::SUM => {
                            let over = g.over.expect(&format!(
                                "while constructing {}, missing over column for SUM", name));
                            (Aggregation::SUM, over)
                        }
                    };
                    graph.incorporate(new(g.name(), &g.outputs(), true, agg.over(
                        from_idx, over, &g.by)))
                }
                JdlNode::Join(j) => {
                    let from_idxs = j.from.iter().map(|f| lookup(f))
                        .collect::<Vec<_>>();
                    let emit = j.emit.iter().map(|&(ti, ci)| (from_idxs[ti], ci))
                        .collect::<Vec<_>>();
                    assert!(from_idxs.len() == 2);
                    let builder = JoinBuilder::new(emit)
                        .from(from_idxs[0], j.on[0].clone())
                        .join(from_idxs[1], j.on[1].clone());
                    graph.incorporate(new(j.name(), &j.outputs(), true, builder))
                }
            }
        };

        nodes.insert(name, index);
    }

    graph
}
