use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;
use randomkit::dist::Laplace;
use randomkit::{Rng, Sample};
use std::f64;
use std::fmt;

use prelude::*;

use nom_sql::OrderType;

#[derive(Clone, Serialize, Deserialize)]
struct Order(Vec<(usize, OrderType)>);
impl Order {
    fn cmp(&self, a: &[DataType], b: &[DataType]) -> Ordering {
        for &(c, ref order_type) in &self.0 {
            let result = match *order_type {
                OrderType::OrderAscending => a[c].cmp(&b[c]),
                OrderType::OrderDescending => b[c].cmp(&a[c]),
            };
            if result != Ordering::Equal {
                return result;
            }
        }
        Ordering::Equal
    }
}

impl From<Vec<(usize, OrderType)>> for Order {
    fn from(other: Vec<(usize, OrderType)>) -> Self {
        Order(other)
    }
}

// Define the Binary, Logarithmic, and Hybrid Mechanisms

// Binary Mechanism (bounded in a window of size T)
#[derive(Serialize, Deserialize)]
pub struct BinaryMechanism {
    #[serde(skip)]
    alphas: Option<HashMap<u32, u32>>,
    #[serde(skip)]
    noisy_alphas: Option<HashMap<u32, f64>>,
    T: f64,
    t: f64,
    eps: f64,
    #[serde(skip)]
    noise_distr: Option<Laplace>,
    #[serde(skip)]
    rng: Option<Rng>,
    prev_output: f64,
}

impl Clone for BinaryMechanism {
    fn clone(&self) -> Self {
        assert!(self.noise_distr.is_none());
        assert!(self.rng.is_none());
        assert!(self.alphas.is_none());
        assert!(self.noisy_alphas.is_none());
        BinaryMechanism {
            t: self.t,
            T: self.T,
            prev_output: self.prev_output,
            eps: self.eps,
            noise_distr: None,
            rng: None,
            alphas: None,
            noisy_alphas: None,
        }
    }
}

impl BinaryMechanism {
    pub fn new(T: f64, e: f64) -> BinaryMechanism {
        BinaryMechanism {
            alphas: None,
            noisy_alphas: None,
            T: T,
            t: 1.0,
            eps: e,
            noise_distr: None,
            rng: None,
            prev_output: 0.0,
        }
    }

    pub fn set_noise_distr(&mut self) {
        self.noise_distr = Some(Laplace::new(0.0, self.T.log2()/self.eps).unwrap());
        self.rng = Some(Rng::from_seed(1));
    }

    pub fn initialize_psums(&mut self) {
        self.alphas = Some(HashMap::new());
        self.noisy_alphas = Some(HashMap::new());
    }
    
    pub fn step_forward(&mut self, element: i64) -> f64 {
        if self.t > self.T {
            return self.prev_output;
        }

        // Get lowest nonzero bit
        let t_prime = self.t as i32;
        let i = ((t_prime & -t_prime) as f64).log2() as u32;
        
        // Create and store a new psum that includes this timestep
        let mut value = element as u32;
        for j in 0..i {
            value += *self.alphas.as_mut().unwrap().entry(j).or_insert(1000); // TODO: better default value to indicate error
            self.alphas.as_mut().unwrap().insert(
                i,
                value,
            );
        }

        // Delete any psums contained in the new psum     
        for j in 0..i {
            self.alphas.as_mut().unwrap().remove(&j);
            self.noisy_alphas.as_mut().unwrap().remove(&j);
        }

        // Update noisy_alphas
        let noise = self.noise_distr.unwrap().sample(self.rng.as_mut().unwrap());    
        self.noisy_alphas.as_mut().unwrap().insert(
            i,
            (value as f64) + noise,
        );

        // Calculate the output
        let t_bin = format!("{:b}", self.t as u32).chars().rev().collect::<String>();      
        let mut output = 0.0;        
        for char_index in t_bin.char_indices() {
            let (j, elt) = char_index;
            if elt == '1' {
                output += *self.noisy_alphas.as_mut().unwrap().entry(j as u32).or_insert(1000.0);
            }
        }
        // Update previous_output, increment t and t_bin, and return                           
        self.t += 1.0;
        self.prev_output = output;
        output
    }
}

// Logarithmic mechanism (unbounded)
#[derive(Serialize, Deserialize)]
pub struct LogarithmicMechanism {
    beta: f64,
    t: f64,
    prev_output: f64,
    eps: f64,
    #[serde(skip)]
    noise_distr: Option<Laplace>,
    #[serde(skip)]
    rng: Option<Rng>,
}

impl Clone for LogarithmicMechanism {
    fn clone(&self) -> Self {
        assert!(self.noise_distr.is_none());
        assert!(self.rng.is_none());
        LogarithmicMechanism {
            beta: self.beta,
            t: self.t,
            prev_output: self.prev_output,
            eps: self.eps,
            noise_distr: None,
            rng: None,
        }
    }
}

impl LogarithmicMechanism {
    pub fn new(e: f64) -> LogarithmicMechanism {
        LogarithmicMechanism {
            beta: 0.0,
            t: 1.0,
            prev_output: 0.0,
            eps: e,
            noise_distr: None,
            rng: None,
        }
    }

    pub fn set_noise_distr(&mut self) -> () {
        self.noise_distr = Some(Laplace::new(0.0, 1.0/self.eps).unwrap());
        self.rng = Some(Rng::from_seed(1));
    }

    pub fn step_forward(&mut self, element: i64) -> f64 {
        self.beta += (element as u32) as f64;
        // If t is not a power of 2, return previous output
        if self.t.log2().floor() != self.t.log2().ceil() {
            self.t += 1.0;
            return self.prev_output
        }
        // t is a power of 2; update beta and return new output
        let noise = self.noise_distr.unwrap().sample(self.rng.as_mut().unwrap());
        self.beta += noise;
        self.prev_output = self.beta;
        self.t += 1.0;
        self.beta
    }
}

// Hybrid Mechanism (unbounded): composition of Logarithmic & Binary mechanisms
#[derive(Clone, Serialize, Deserialize)]
pub struct HybridMechanism {
    l: LogarithmicMechanism,
    b: BinaryMechanism,
    e: f64,
    t: f64,
}

impl fmt::Debug for HybridMechanism {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HybridMechanism {{ epsilon: {}, t: {}, current T: {} }}", self.e, self.t, self.b.T)
    }
}

impl HybridMechanism {
    pub fn new(e: f64) -> HybridMechanism {
        HybridMechanism {
            l: LogarithmicMechanism::new(e/2.0),
            b: BinaryMechanism::new(2.0, e/2.0),
            e: e,
            t: 1.0,
        }
    }

    pub fn step_forward(&mut self, element: i64) -> f64 {
        // Always step Log Mech forward; will only do an update if power of 2.
        let l_out = self.l.step_forward(element);

        // If t is a power of 2, initialize new binary mechanism.
        if self.t > 1.0 && self.t.log2().floor() == self.t.log2().ceil() {
            self.b = BinaryMechanism::new(self.t, self.e/2.0);
            self.t += 1.0;
            return l_out
        }

        // t is not a power of 2; update binary mechanism.
        if self.t > 1.0 {
            let b_out = self.b.step_forward(element);
            self.t += 1.0;
            return l_out + b_out
        }
        // t <= 1.0
        self.t += 1.0;
        l_out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAggregator {
    src: IndexPair,

    // some cache state
    // TODO: where is this updated
    us: Option<IndexPair>,
    cols: usize,

    // precomputed datastructures
    group_by: Vec<usize>,
    // TODO: need out_key & colfix? (from GroupedOperator)
    out_key: Vec<usize>,
    colfix: Vec<usize>,
    
    // aggregator state (for over, have to see where 'over' is
    // called in GroupedOperator)
    over: usize, // aggregated column
    counter: HybridMechanism,
}

impl DpAggregator {
    pub fn new(
        src: NodeIndex,
        over: usize,
        group_by: &[usize],
        eps: f64,
    ) -> Self {
        assert!(
            !group_by.iter().any(|&i| i == over),
            "cannot group by aggregation column"
        );

        DpAggregator {
            src: src.into(),
            us: None,
            cols: 0,
            group_by: group_by.into(), // TODO: sorted or not? or empty vector?
            out_key: Vec::new(),
            colfix: Vec::new(),
            over: over,
            counter: HybridMechanism::new(eps),
        }
    }
}

impl Ingredient for DpAggregator {
    fn take(&mut self) -> NodeOperator {
        // Necessary because cmp_rows can't be cloned.
        Self {
            src: self.src,

            us: self.us,
            cols: self.cols,

            group_by: self.group_by.clone(),

            over: self.over,
            counter: self.counter.clone(),
        }
        .into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn to_diff(&self, _r: &[DataType], pos: bool) -> Self::Diff {
        match self.op {
            DpAggregation::COUNT if pos => 1,
            DpAggregation::COUNT => -1,
        }
    }

    fn apply(
        &mut self,
        _current: Option<&DataType>,
        diffs: &mut Iterator<Item = Self::Diff>,
    ) -> DataType {
        // "current" is superfluous, already tracked in counter state.
        // LATER: for increment and decrement counters
        // TODO: should both pos and neg take the 0's as well? How is clocking affected by the split?
        // Should -1's be treated as zeros in pos counter and vice versa (if so, below code won't work)?
        // pos = diffs.into_iter().filter(|d| d > 0).map(|d| self.pos_counter.step_forward(d)).last().into()
        // neg = diffs.into_iter().filter(|d| d < 0).map(|d| self.neg_counter.step_forward(-1*d)).last().into()
        // pos - neg
        diffs.into_iter().map(|d| self.counter.step_forward(d as i64)).last().unwrap().into()
    }

    // Called at the beginning of on_connect()
    fn setup(&mut self, parent: &Node) {
        assert!(
            self.over < parent.fields().len(),
            "cannot aggregate over non-existing column"
        );
        // Initialize Option<...> fields in counter.
        // TODO: wrong place to do this, needs to be done on deserialization.
        self.counter.l.set_noise_distr();
        self.counter.b.set_noise_distr();
        self.counter.b.initialize_psums();
    }
    
    // IMPL is copied from grouped/mod.rs
    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];

        // give our inner operation a chance to initialize
        self.setup(srcn);

        // group by all columns
        self.cols = srcn.fields().len();
        self.group_by.extend(self.inner.group_by().iter().cloned());
        self.group_by.sort();
        // cache the range of our output keys 
        // TODO: does dp counter need this?
        self.out_key = (0..self.group_by.len()).collect();

        // build a translation mechanism for going from output columns to input columns
        // TODO: does dp counter need this?
        let colfix: Vec<_> = (0..self.cols)
            .into_iter()
            .filter_map(|col| {
                if self.group_by.iter().any(|c| c == &col) {
                    // since the generated value goes at the end,
                    // this is the n'th output value
                    Some(col)
                } else {
                    // this column does not appear in output
                    None
                }
            })
            .collect();
        self.colfix.extend(colfix.into_iter());
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        // who's our parent really?
        self.src.remap(remap);

        // who are we?
        self.us = Some(remap[&us]);
    }

    // IMPL is copied from grouped/mod.rs
    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_cols: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        let group_by = &self.group_by;
        let group_cmp = |a: &Record, b: &Record| {
            group_by
                .iter()
                .map(|&col| &a[col])
                .cmp(group_by.iter().map(|&col| &b[col]))
        };

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        // TODO: reinstate sort? after figuring out if batching is the right thing to do
        let mut rs: Vec<_> = rs.into();
//        rs.sort_by(&cmp);

        // find the current value for this group                  
        let us = self.us.unwrap();
        let db = state
            .get(*us)
            .expect("grouped operators must have their own state materialized");
        let mut misses = Vec::new();
        let mut out = Vec::new();
        {
            let out_key = &self.out_key;
            let mut handle_group =
                |inner: &mut T, // TODO: rename to op
                 group_rs: ::std::vec::Drain<Record>,
                 mut diffs: ::std::vec::Drain<_>| {
                    let mut group_rs = group_rs.peekable();

                    let mut group = Vec::with_capacity(group_by.len() + 1);
                    {
                        let group_r = group_rs.peek().unwrap();
                        let mut group_by_i = 0;
                        for (col, v) in group_r.iter().enumerate() {
                            if col == group_by[group_by_i] {
                                group.push(v.clone());
                                group_by_i += 1;
                                if group_by_i == group_by.len() {
                                    break;
                                }
                            }
                        }
                    }

                    let rs = {
                        match db.lookup(&out_key[..], &KeyType::from(&group[..])) {
                            LookupResult::Some(rs) => {
                                debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                                rs
                            }
                            LookupResult::Missing => {
                                misses.extend(group_rs.map(|r| Miss {
                                    on: *us,
                                    lookup_idx: out_key.clone(),
                                    lookup_cols: group_by.clone(),
                                    replay_cols: replay_key_cols.map(Vec::from),
                                    record: r.extract().0,
                                    }));
                                return;
                            }
                        }
                    };

                    let old = rs.into_iter().next();
                    // current value is in the last output column      
                    // or "" if there is no current group                                    
                    let current = old.as_ref().map(|rows| match rows {
                        Cow::Borrowed(rs) => Cow::Borrowed(&rs[rs.len() - 1]),
                        Cow::Owned(rs) => Cow::Owned(rs[rs.len() - 1].clone()),
                    });

                    // new is the result of applying all diffs for the group to the current value
                    let new = self.apply(current.as_ref().map(|v| &**v), &mut diffs as &mut _);
                    match current {
                        Some(ref current) if new == **current => {
                            // no change
                            }
                        _ => {
                            if let Some(old) = old {
                                // revoke old value    
                                debug_assert!(current.is_some());
                                out.push(Record::Negative(old.into_owned()));
                            }

                            // emit positive, which is group + new.
                            let mut rec = group;
                            rec.push(new);
                            out.push(Record::Positive(rec));
                        }
                    }
                };
            let mut diffs = Vec::new();
            let mut group_rs = Vec::new();
            for r in rs {
                if !group_rs.is_empty() && cmp(&group_rs[0], &r) != Ordering::Equal {
                    handle_group(&mut self, group_rs.drain(..), diffs.drain(..));
                }
                diffs.push(self.to_diff(&r[..], r.is_positive()));
                group_rs.push(r);
            }
            assert!(!diffs.is_empty());
            handle_group(&mut self, group_rs.drain(..), diffs.drain(..));
        }

        ProcessingResult {
            results: out.into(),
            misses: misses,
        }
    }

    fn on_eviction(
        &mut self,
        _: LocalNodeIndex,
        key_columns: &[usize],
        _: &mut Vec<Vec<DataType>>,
    ) {
        assert_eq!(key_columns, &self.group_by[..]);
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        vec![(this, (self.group_by.clone(), true))]
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("DP_COUNT");
        }
        let op_string : String = match self.op {
            DpAggregation::COUNT => "|*|".into(),
        };
        let group_cols = self
            .group
            .iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} γ[{}]", op_string, group_cols)
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(col))]
    }

    // Temporary: for now, disable backwards queries
    fn requires_full_materialization(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(reversed: bool) -> (ops::test::MockGraph, IndexPair) {
        let cmp_rows = if reversed {
            vec![(2, OrderType::OrderDescending)]
        } else {
            vec![(2, OrderType::OrderAscending)]
        };

        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "topk",
            &["x", "y", "z"],
            TopK::new(s.as_global(), cmp_rows, vec![1], 3),
            true,
        );
        (g, s)
    }

    #[test]
    fn it_forwards() {
        let (mut g, _) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    #[ignore]
    fn it_must_query() {
        let (mut g, s) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];
        let r10b: Vec<DataType> = vec![6.into(), "z".into(), 10.into()];
        let r10c: Vec<DataType> = vec![7.into(), "z".into(), 10.into()];

        // fill topk
        g.narrow_one_row(r12.clone(), true);
        g.narrow_one_row(r10.clone(), true);
        g.narrow_one_row(r11.clone(), true);
        g.narrow_one_row(r5.clone(), true);
        g.narrow_one_row(r15.clone(), true);

        // put stuff to query for in the bases
        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());

        // check that removing 15 brings back 10
        let a = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r15.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r10.clone(), true).into()));
        g.unseed(s);

        let a = g.narrow_one_row(r10b.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r10c.clone(), true);
        assert_eq!(a.len(), 0);

        g.seed(s, r12.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());
        g.seed(s, r10b.clone());
        g.seed(s, r10c.clone());
        let a = g.narrow_one_row((r10.clone(), false), true);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], (r10.clone(), false).into());
        assert!(a[1] == (r10b.clone(), true).into() || a[1] == (r10c.clone(), true).into());
    }

    #[test]
    fn it_forwards_reversed() {
        let (mut g, _) = setup(true);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), (-12.123).into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), (0.0431).into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), (-0.082).into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), (5.601).into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), (-15.9).into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    fn it_suggests_indices() {
        let (g, _) = setup(false);
        let me = 2.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 1);
        assert_eq!(*idx.iter().next().unwrap().1, (vec![1], true));
    }

    #[test]
    fn it_resolves() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_parent_columns() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_handles_updates() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 10.into()];
        let r2: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r3: Vec<DataType> = vec![3.into(), "z".into(), 10.into()];
        let r4: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r4a: Vec<DataType> = vec![4.into(), "z".into(), 10.into()];
        let r4b: Vec<DataType> = vec![4.into(), "z".into(), 11.into()];

        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);

        // a positive for a row not in the Top-K should not change the Top-K and shouldn't emit
        // anything
        let emit = g.narrow_one_row(r4.clone(), true);
        assert_eq!(g.states[ni].rows(), 3);
        assert_eq!(emit, Vec::<Record>::new().into());

        // should now have 3 rows in Top-K
        // [1, z, 10]
        // [2, z, 10]
        // [3, z, 10]

        let emit = g.narrow_one(
            Records::from(vec![Record::Negative(r4.clone()), Record::Positive(r4a.clone())].into()),
            true,
        );
        // nothing should have been emitted, as [4, z, 10] doesn't enter Top-K
        assert_eq!(emit, Vec::<Record>::new().into());

        let emit = g.narrow_one(
            Records::from(
                vec![Record::Negative(r4a.clone()), Record::Positive(r4b.clone())].into(),
            ),
            true,
        );

        // now [4, z, 11] is in, BUT we still only keep 3 elements
        // and have to remove one of the existing ones
        assert_eq!(g.states[ni].rows(), 3);
        assert_eq!(emit.len(), 2); // 1 pos, 1 neg
        assert!(emit.iter().any(|r| !r.is_positive() && r[2] == 10.into()));
        assert!(emit.iter().any(|r| r.is_positive() && r[2] == 11.into()));
    }
}
