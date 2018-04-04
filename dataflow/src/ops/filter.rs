use std::borrow::Cow;
use std::collections::HashMap;
use std::sync;

pub use nom_sql::Operator;
use prelude::*;

/// Filters incoming records according to some filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    src: IndexPair,
    filter: sync::Arc<Vec<Option<FilterCondition>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterCondition {
    Equality(Operator, DataType),
    In(Vec<DataType>),
}

impl Filter {
    /// Construct a new filter operator. The `filter` vector must have as many elements as the
    /// `src` node has columns. Each column that is set to `None` matches any value, while columns
    /// in the filter that have values set will check for equality on that column.
    pub fn new(src: NodeIndex, filter: &[Option<FilterCondition>]) -> Filter {
        Filter {
            src: src.into(),
            filter: sync::Arc::new(Vec::from(filter)),
        }
    }
}

impl Ingredient for Filter {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];
        // N.B.: <= because the adjacent node might be a base with a suffix of removed columns.
        // It's okay to just ignore those.
        assert!(self.filter.len() <= srcn.fields().len());
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: LocalNodeIndex,
        mut rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        rs.retain(|r| {
            self.filter.iter().enumerate().all(|(i, fi)| {
                // check if this filter matches
                let d = &r[i];
                if let Some(ref cond) = *fi {
                    match *cond {
                        FilterCondition::Equality(ref op, ref f) => match *op {
                            Operator::Equal => d == f,
                            Operator::NotEqual => d != f,
                            Operator::Greater => d > f,
                            Operator::GreaterOrEqual => d >= f,
                            Operator::Less => d < f,
                            Operator::LessOrEqual => d <= f,
                            Operator::In => unreachable!(),
                            _ => unimplemented!(),
                        },
                        FilterCondition::In(ref fs) => fs.contains(d),
                    }
                } else {
                    // everything matches no condition
                    true
                }
            })
        });

        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self) -> String {
        use regex::Regex;

        let escape = |s: &str| {
            Regex::new("([<>])")
                .unwrap()
                .replace_all(s, "\\$1")
                .to_string()
        };
        format!(
            "σ[{}]",
            self.filter
                .iter()
                .enumerate()
                .filter_map(|(i, ref e)| match e.as_ref() {
                    Some(cond) => match *cond {
                        FilterCondition::Equality(ref op, ref x) => {
                            Some(format!("f{} {} {}", i, escape(&format!("{}", op)), x))
                        }
                        FilterCondition::In(ref xs) => Some(format!(
                            "f{} IN ({})",
                            i,
                            xs.iter()
                                .map(|d| format!("{}", d))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )),
                    },
                    None => None,
                })
                .collect::<Vec<_>>()
                .as_slice()
                .join(", ")
        ).into()
    }

    fn can_query_through(&self) -> bool {
        true
    }

    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType,
        states: &'a StateMap,
    ) -> Option<Option<Box<Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        states.get(&*self.src).and_then(|state| {
            let f = self.filter.clone();
            let filter = move |r: &&Row| {
                r.iter().enumerate().all(|(i, d)| {
                    // check if this filter matches
                    if let Some(ref cond) = f[i] {
                        match *cond {
                            FilterCondition::Equality(ref op, ref f) => match *op {
                                Operator::Equal => d == f,
                                Operator::NotEqual => d != f,
                                Operator::Greater => d > f,
                                Operator::GreaterOrEqual => d >= f,
                                Operator::Less => d < f,
                                Operator::LessOrEqual => d <= f,
                                _ => unimplemented!(),
                            },
                            FilterCondition::In(ref fs) => fs.contains(d),
                        }
                    } else {
                        // everything matches no condition
                        true
                    }
                })
            };

            match state.lookup(columns, key) {
                LookupResult::Some(Cow::Borrowed(rs)) => {
                    let r = Box::new(rs.iter().filter(filter).map(|r| Cow::from(&r[..]))) as Box<_>;
                    Some(Some(r))
                }
                LookupResult::Some(Cow::Owned(rs)) => {
                    let r = Box::new(
                        rs.into_iter()
                            .filter(move |ref r| filter(r))
                            .map(|r| Cow::from(r.unpack())),
                    ) as Box<_>;
                    Some(Some(r))
                }
                LookupResult::Missing => Some(None),
            }
        })
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(
        materialized: bool,
        filters: Option<&[Option<FilterCondition>]>,
    ) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "filter",
            &["x", "y"],
            Filter::new(
                s.as_global(),
                filters.unwrap_or(&[
                    None,
                    Some(FilterCondition::Equality(Operator::Equal, "a".into())),
                ]),
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_nofilter() {
        let mut g = setup(false, Some(&[None, None]));

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false, None);

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_mfilter() {
        let mut g = setup(
            false,
            Some(&[
                Some(FilterCondition::Equality(Operator::Equal, 1.into())),
                Some(FilterCondition::Equality(Operator::Equal, "a".into())),
            ]),
        );

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false, None);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false, None);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
    }

    #[test]
    fn it_works_with_many() {
        let mut g = setup(false, None);

        let mut many = Vec::new();

        for i in 0..10 {
            many.push(vec![i.into(), "a".into()]);
        }

        assert_eq!(g.narrow_one(many.clone(), false), many.into());
    }

    #[test]
    fn it_works_with_inequalities() {
        let mut g = setup(
            false,
            Some(&[
                Some(FilterCondition::Equality(Operator::LessOrEqual, 2.into())),
                Some(FilterCondition::Equality(Operator::NotEqual, "a".into())),
            ]),
        );

        let mut left: Vec<DataType>;

        // both conditions match (2 <= 2, "b" != "a")
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" != "a")
        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 <= 2)
        left = vec![3.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (1 <= 2, "b" != "a")
        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_works_with_in_list() {
        let mut g = setup(
            false,
            Some(&[
                Some(FilterCondition::In(vec![2.into(), 42.into()])),
                Some(FilterCondition::In(vec!["b".into()])),
            ]),
        );

        let mut left: Vec<DataType>;

        // both conditions match (2 IN (2, 42), "b" IN ("b"))
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" NOT IN ("b"))
        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 NOT IN (2, 42))
        left = vec![3.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (42 IN (2, 42), "b" IN ("b"))
        left = vec![42.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }
}
