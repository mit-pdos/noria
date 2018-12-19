use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;
use randomkit::dist::Laplace;
use randomkit::{Rng, Sample};
use std::cmp::Ordering;

use prelude::*;

/// Supported aggregation operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DpAggregation {
    /// Count the number of records for each group. The value for the `over` column is ignored.
    COUNT,
}

impl DpAggregation {
    /// Construct a new `Aggregator` that performs this operation.
    ///
    /// The aggregation will aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier. The `over` column should not be in the `group_by` array.
    pub fn over(
        self,
        src: NodeIndex,
        over: usize,
        group_by: &[usize],
        eps: f64,
    ) -> GroupedOperator<DpAggregator> {
        assert!(
            !group_by.iter().any(|&i| i == over),
            "cannot group by aggregation column"
        );
        GroupedOperator::new(
            src,
            DpAggregator {
                op: self,
                over: over,
                group: group_by.into(),
                noise: 0.0,
                eps: eps,
                seed: 1,
            },
        )
    }
}

/// Aggregator implements a Soup node that performs common aggregation operations such as counts
/// and sums.
///
/// `Aggregator` nodes are constructed through `Aggregation` variants using `Aggregation::new`.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column
/// (`self.over`) of the incoming record is then added to the current aggregation value according
/// to the operator in use (`COUNT` always adds/subtracts 1, `SUM` adds/subtracts the value of the
/// value in the incoming record. The output record is constructed by concatenating the columns
/// identifying the group, and appending the aggregated value. For example, for a sum with
/// `self.over == 1`, a previous sum of `3`, and an incoming record with `[a, 1, x]`, the output
/// would be `[a, x, 4]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAggregator {
    op: DpAggregation,
    over: usize,
    group: Vec<usize>,
    noise: f64,
    eps: f64,
    seed: u32,
}

impl GroupedOperation for DpAggregator {
    type Diff = i64;

    fn setup(&mut self, parent: &Node) {
        assert!(
            self.over < parent.fields().len(),
            "cannot aggregate over non-existing column"
        );
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DataType], pos: bool) -> Self::Diff {
        match self.op {
            DpAggregation::COUNT if pos => 1,
            DpAggregation::COUNT => -1,
        }
    }

    fn apply(
        &mut self,
        current: Option<&DataType>,
        diffs: &mut Iterator<Item = Self::Diff>,
    ) -> DataType {
        let noisy_n = match current {
            Some(x) => x.into() : f64,
//            Some(&DataType::BigInt(n)) => n as f64,
            None => 0 as f64,
            _ => unreachable!(),
        };
        let n: f64 = noisy_n - self.noise; // Is this even necessary if noise should be cumulative?
        let mut rng = Rng::from_seed(self.seed);
        let noise_distr = Laplace::new(0.0, 1.0/self.eps).unwrap();
        self.noise = noise_distr.sample(&mut rng);
        self.seed += 1;
        (diffs.into_iter().fold(n, |n, d| n + (d as f64)) + self.noise).into()
    }

    fn description(&self) -> String {
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

    // Temporary: for now, disable backwards queries
    fn requires_full_materialization(&self) -> bool {
        true
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            DpAggregation::COUNT.over(s.as_global(), 1, &[0], 0.1), // epsilon = 0.1
            mat,
        );
        g
    }

    fn setup_multicolumn(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "identity",
            &["x", "z", "ys"],
            DpAggregation::COUNT.over(s.as_global(), 1, &[0, 2], 0.1), // epsilon = 0.1
            mat,
        );
        g
    }

    #[test]
    fn it_describes() {
        let s = 0.into();

        let c = DpAggregation::COUNT.over(s, 1, &[0, 2], 0.1); // epsilon = 0.1
        assert_eq!(c.description(), "|*| γ[0, 2]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(true);

        let u: Record = vec![1.into(), 1.into()].into();

        // first row for a group should emit +1 for that group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                // Should be within 50 of true count w/ Pr >= 99.3%
                println!("r[1]: {}", r[1]);
                assert!(r[1] <= DataType::from(51.0));
                assert!(r[1] >= DataType::from(-49.0));
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into()].into();

        // first row for a second group should emit +1 for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                // Should be within 50 of true count w/ Pr >= 99.3%
                assert!(r[1] <= DataType::from(51.0));
                assert!(r[1] >= DataType::from(-49.0));
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2); // Why is rs.len = 2 for this record?
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = (vec![1.into(), 1.into()], false); // false indicates a negative record

        // negative row for a group should emit -2 and +1
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u = vec![
            (vec![1.into(), 1.into()], false),
            (vec![1.into(), 1.into()], true),
            (vec![1.into(), 2.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![2.into(), 2.into()], true),
            (vec![2.into(), 3.into()], true),
            (vec![2.into(), 1.into()], true),
            (vec![3.into(), 3.into()], true),
        ];

        // multiple positives and negatives should update aggregation value by appropriate amount
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5); // one - and one + for each group, except 3 which is new
                                 // group 1 lost 1 and gained 2
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == 1.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == 2.into()
        } else {
            false
        }));
        // group 2 lost 1 and gained 3
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == 1.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 2.into() && r[1] == 3.into()
        } else {
            false
        }));
        // group 3 lost 0 and gained 1
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into() && r[1] == 1.into()
        } else {
            false
        }));
    }
}
