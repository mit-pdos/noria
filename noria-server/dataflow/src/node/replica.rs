use petgraph::graph::NodeIndex;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum ReplicaType {
    Top { bottom_next_nodes: Vec<NodeIndex> },
    Bottom { top_prev_nodes: Vec<NodeIndex> },
}
