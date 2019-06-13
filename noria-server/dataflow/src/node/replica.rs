use petgraph::graph::NodeIndex;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum ReplicaType {
    Top {
        bottom: NodeIndex,
    },
    Bottom {
        top: NodeIndex,
    },
}
