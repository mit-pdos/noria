use std::collections::HashMap;

use prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replica {
    src: IndexPair,
    op: Option<Box<NodeOperator>>,
}

impl Replica {
    /// Construct a new replica operator.
    pub fn new(src: NodeIndex) -> Replica {
        Replica { src: src.into(), op: None }
    }

    pub fn set_op(&mut self, op: Box<NodeOperator>) {
        self.op = Some(op);
    }

    pub fn take_op(&mut self) -> Box<NodeOperator> {
        self.op.take().expect("replica op should be set in migration")
    }
}

impl Ingredient for Replica {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: &mut Executor,
        _: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    /// Suggest indexes to reflect the materialized state of its internal operator
    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        self.op.as_ref().unwrap().suggest_indexes(this)
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self, _: bool) -> String {
        "[replica]".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}
