use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use prelude::*;

/// Supported aggregation operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Aggregation {
    /// Count the number of records for each group. The value for the `over` column is ignored.
    COUNT,
    /// Sum the value of the `over` column for all records of each group.
    SUM,
}

impl Aggregation {
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
    ) -> GroupedOperator<Aggregator> {
        assert!(
            !group_by.iter().any(|&i| i == over),
            "cannot group by aggregation column"
        );
        GroupedOperator::new(
            src,
            Aggregator {
                op: self,
                over,
                group: group_by.into(),
            },
        )
    }
}

/// Aggregator implementas a Soup node that performans common aggregation operations such as counts
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
pub struct Aggregator {
    op: Aggregation,
    over: usize,
    group: Vec<usize>,
}

impl GroupedOperation for Aggregator {
    type Diff = i128;

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
            Aggregation::COUNT if pos => 1,
            Aggregation::COUNT => -1,
            Aggregation::SUM => {
                let v = match r[self.over] {
                    DataType::Int(n) => i128::from(n),
                    DataType::UnsignedInt(n) => i128::from(n),
                    DataType::BigInt(n) => i128::from(n),
                    DataType::UnsignedBigInt(n) => i128::from(n),
                    DataType::None => 0,
                    ref x => unreachable!("tried to aggregate over {:?} on {:?}", x, r),
                };
                if pos {
                    v
                } else {
                    0i128 - v
                }
            }
        }
    }

    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
    ) -> DataType {
        let n = match current {
            Some(&DataType::Int(n)) => i128::from(n),
            Some(&DataType::UnsignedInt(n)) => i128::from(n),
            Some(&DataType::BigInt(n)) => i128::from(n),
            Some(&DataType::UnsignedBigInt(n)) => i128::from(n),
            None => 0,
            _ => unreachable!(),
        };
        diffs.fold(n, |n, d| n + d).into()
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from(match self.op {
                Aggregation::COUNT => "+",
                Aggregation::SUM => "𝛴",
            });
        }

        let op_string = match self.op {
            Aggregation::COUNT => "|*|".into(),
            Aggregation::SUM => format!("𝛴({})", self.over),
        };
        let group_cols = self
            .group
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} γ[{}]", op_string, group_cols)
    }

    fn over_columns(&self) -> Vec<usize> {
        vec![self.over]
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
            Aggregation::COUNT.over(s.as_global(), 1, &[0]),
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
            Aggregation::COUNT.over(s.as_global(), 1, &[0, 2]),
            mat,
        );
        g
    }

    #[test]
    fn it_describes() {
        let s = 0.into();

        let c = Aggregation::COUNT.over(s, 1, &[0, 2]);
        assert_eq!(c.description(true), "|*| γ[0, 2]");

        let s = Aggregation::SUM.over(s, 1, &[2, 0]);
        assert_eq!(s.description(true), "𝛴(1) γ[2, 0]");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
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
                assert_eq!(r[1], 1.into());
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
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
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

        let u = (vec![1.into(), 1.into()], false);

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

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn it_groups_by_multiple_columns() {
        let mut c = setup_multicolumn(true);

        let u: Record = vec![1.into(), 1.into(), 2.into()].into();

        // first row for a group should emit +1 for the group (1, 1)
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 1.into(), 2.into()].into();

        // first row for a second group should emit +1 for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 1.into(), 2.into()].into();

        // second row for a group should emit -1 and +2
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }

        let u = (vec![1.into(), 1.into(), 2.into()], false);

        // negative row for a group should emit -2 and +1
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
    }

    // TODO: also test SUM

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let c = setup(false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], vec![0]);
    }

    #[test]
    fn it_resolves() {
        let c = setup(false);
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(c.node().resolve(1), None);
    }
}
