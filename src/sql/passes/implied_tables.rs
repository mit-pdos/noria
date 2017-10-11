use nom_sql::{ArithmeticBase, Column, ConditionExpression, ConditionTree, FieldExpression,
              JoinRightSide, SqlQuery, Table};

use std::collections::HashMap;

pub trait ImpliedTableExpansion {
    fn expand_implied_tables(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

fn rewrite_conditional<F>(
    expand_columns: &F,
    ce: ConditionExpression,
    avail_tables: &Vec<Table>,
) -> ConditionExpression
where
    F: Fn(Column, &Vec<Table>) -> Column,
{
    use nom_sql::ConditionExpression::*;
    use nom_sql::ConditionBase::*;

    let translate_ct_arm = |bce: Box<ConditionExpression>| -> Box<ConditionExpression> {
        let new_ce = match *bce {
            Base(Field(f)) => Base(Field(expand_columns(f, avail_tables))),
            Base(b) => Base(b),
            x => rewrite_conditional(expand_columns, x, avail_tables),
        };
        Box::new(new_ce)
    };

    match ce {
        ComparisonOp(ct) => {
            let l = translate_ct_arm(ct.left);
            let r = translate_ct_arm(ct.right);
            let rewritten_ct = ConditionTree {
                operator: ct.operator,
                left: l,
                right: r,
            };
            ComparisonOp(rewritten_ct)
        }
        LogicalOp(ConditionTree {
            operator,
            box left,
            box right,
        }) => LogicalOp(ConditionTree {
            operator: operator,
            left: Box::new(rewrite_conditional(expand_columns, left, avail_tables)),
            right: Box::new(rewrite_conditional(expand_columns, right, avail_tables)),
        }),
        x => x,
    }
}

impl ImpliedTableExpansion for SqlQuery {
    fn expand_implied_tables(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        use nom_sql::FunctionExpression::*;
        use nom_sql::{GroupByClause, OrderClause};
        use nom_sql::TableKey::*;

        // Tries to find a table with a matching column in the `tables_in_query` (information
        // passed as `write_schemas`; this is not something the parser or the expansion pass can
        // know on their own). Panics if no match is found or the match is ambiguous.
        let find_table = |f: &Column, tables_in_query: &Vec<Table>| -> Option<String> {
            let mut matches = write_schemas
                .iter()
                .filter(|&(t, _)| if tables_in_query.len() > 0 {
                    for qt in tables_in_query {
                        if qt.name == *t {
                            return true;
                        }
                    }
                    false
                } else {
                    // preserve all tables if there are no tables in the query
                    true
                })
                .filter_map(|(t, ws)| {
                    let num_matching = ws.iter().filter(|c| **c == f.name).count();
                    assert!(num_matching <= 1);
                    if num_matching == 1 {
                        Some((*t).clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<String>>();
            if matches.len() > 1 {
                println!(
                    "Ambiguous column {} exists in tables: {} -- picking a random one",
                    f.name,
                    matches.as_slice().join(", ")
                );
                Some(matches.pop().unwrap())
            } else if matches.is_empty() {
                // This might be an alias for a computed column, which has no
                // implied table. So, we allow it to pass and our code should
                // crash in the future if this is not the case.
                None
            } else {
                // exactly one match
                Some(matches.pop().unwrap())
            }
        };

        let err = "Must apply StarExpansion pass before ImpliedTableExpansion"; // for wrapping

        // Traverses a query and calls `find_table` on any column that has no explicit table set,
        // including computed columns. Should not be used for CREATE TABLE and INSERT queries,
        // which can use the simpler `set_table`.
        let expand_columns = |mut f: Column, tables_in_query: &Vec<Table>| -> Column {
            f.table = match f.table {
                None => {
                    match f.function {
                        Some(ref mut f) => {
                            // There is no implied table (other than "self") for anonymous function
                            // columns, but we have to peek inside the function to expand implied
                            // tables in its specification
                            match (*f).as_mut() {
                                &mut Avg(ref mut fe, _) |
                                &mut Count(ref mut fe, _) |
                                &mut Sum(ref mut fe, _) |
                                &mut Min(ref mut fe) |
                                &mut Max(ref mut fe) |
                                &mut GroupConcat(ref mut fe, _) => {
                                    fe.table = find_table(fe, tables_in_query);
                                    None
                                }
                                &mut CountStar => None,
                            }
                        }
                        None => find_table(&f, tables_in_query),
                    }
                }
                Some(x) => Some(x),
            };
            f
        };

        // Sets the table for the `Column` in `f`to `table`. This is mostly useful for CREATE TABLE
        // and INSERT queries and deliberately leaves function specifications unaffected, since
        // they can refer to remote tables and `set_table` should not be used for queries that have
        // computed columns.
        let set_table = |mut f: Column, table: &Table| -> Column {
            f.table = match f.table {
                None => match f.function {
                    Some(ref mut f) => panic!(
                        "set_table({}) invoked on computed column {:?}",
                        table.name,
                        f
                    ),
                    None => Some(table.name.clone()),
                },
                Some(x) => Some(x),
            };
            f
        };

        match self {
            SqlQuery::Select(mut sq) => {
                let mut tables: Vec<Table> = sq.tables.clone();
                // tables mentioned in JOINs are also available for expansion
                for jc in sq.join.iter() {
                    match jc.right {
                        JoinRightSide::Table(ref join_table) => tables.push(join_table.clone()),
                        JoinRightSide::Tables(ref join_tables) => {
                            tables.extend(join_tables.clone())
                        }
                        _ => unimplemented!(),
                    }
                }
                // Expand within field list
                for field in sq.fields.iter_mut() {
                    match field {
                        &mut FieldExpression::All => panic!(err),
                        &mut FieldExpression::AllInTable(_) => panic!(err),
                        &mut FieldExpression::Literal(_) => (),
                        &mut FieldExpression::Arithmetic(ref mut e) => {
                            if let ArithmeticBase::Column(ref mut c) = e.left {
                                *c = expand_columns(c.clone(), &tables);
                            }

                            if let ArithmeticBase::Column(ref mut c) = e.right {
                                *c = expand_columns(c.clone(), &tables);
                            }
                        }
                        &mut FieldExpression::Col(ref mut f) => {
                            *f = expand_columns(f.clone(), &tables);
                        }
                    }
                }
                // Expand within WHERE clause
                sq.where_clause = match sq.where_clause {
                    None => None,
                    Some(wc) => Some(rewrite_conditional(&expand_columns, wc, &tables)),
                };
                // Expand within GROUP BY clause
                sq.group_by = match sq.group_by {
                    None => None,
                    Some(gbc) => Some(GroupByClause {
                        columns: gbc.columns
                            .into_iter()
                            .map(|f| expand_columns(f, &tables))
                            .collect(),
                        having: match gbc.having {
                            None => None,
                            Some(hc) => Some(rewrite_conditional(&expand_columns, hc, &tables)),
                        },
                    }),
                };
                // Expand within ORDER BY clause
                sq.order = match sq.order {
                    None => None,
                    Some(oc) => Some(OrderClause {
                        columns: oc.columns
                            .into_iter()
                            .map(|(f, o)| (expand_columns(f, &tables), o))
                            .collect(),
                    }),
                };

                SqlQuery::Select(sq)
            }
            SqlQuery::CreateTable(mut ctq) => {
                let table = ctq.table.clone();
                let transform_key = |key_cols: Vec<Column>| {
                    key_cols.into_iter().map(|k| set_table(k, &table)).collect()
                };
                // Expand within field list
                ctq.fields = ctq.fields
                    .into_iter()
                    .map(|mut tfs| {
                        tfs.column = set_table(tfs.column, &table);
                        tfs
                    })
                    .collect();
                // Expand tables for key specification
                if ctq.keys.is_some() {
                    ctq.keys = Some(
                        ctq.keys
                            .unwrap()
                            .into_iter()
                            .map(|k| match k {
                                PrimaryKey(key_cols) => PrimaryKey(transform_key(key_cols)),
                                UniqueKey(name, key_cols) => {
                                    UniqueKey(name, transform_key(key_cols))
                                }
                                FulltextKey(name, key_cols) => {
                                    FulltextKey(name, transform_key(key_cols))
                                }
                                Key(name, key_cols) => Key(name, transform_key(key_cols)),
                            })
                            .collect(),
                    );
                }
                SqlQuery::CreateTable(ctq)
            }
            SqlQuery::Insert(mut iq) => {
                let table = iq.table.clone();
                // Expand within field list
                iq.fields = iq.fields
                    .into_iter()
                    .map(|(c, n)| (set_table(c, &table), n))
                    .collect();
                SqlQuery::Insert(iq)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column, ColumnSpecification, FieldExpression, SqlQuery, SqlType, Table};
    use std::collections::HashMap;
    use super::ImpliedTableExpansion;

    #[test]
    fn it_expands_implied_tables_for_create_table() {
        use nom_sql::CreateTableStatement;

        // CREATE TABLE address (addr_id, addr_street1);
        // -->
        // CREATE TABLE address (address.addr_id, address.addr_street1);
        let q = CreateTableStatement {
            table: Table::from("address"),
            fields: vec![
                ColumnSpecification::new(Column::from("addr_id"), SqlType::Text),
                ColumnSpecification::new(Column::from("addr_street1"), SqlType::Text),
            ],
            ..Default::default()
        };

        // empty write schema for CREATE
        let schema = HashMap::new();
        let res = SqlQuery::CreateTable(q).expand_implied_tables(&schema);
        match res {
            SqlQuery::CreateTable(tq) => {
                let cs1 = ColumnSpecification::new(Column::from("address.addr_id"), SqlType::Text);
                let cs2 =
                    ColumnSpecification::new(Column::from("address.addr_street1"), SqlType::Text);
                assert_eq!(tq.fields, vec![cs1, cs2]);
                assert_eq!(tq.table, Table::from("address"));
            }
            // if we get anything other than a table creation query back,
            // something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_implied_tables_for_select() {
        use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Operator, SelectStatement};

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));

        // SELECT name, title FROM users, articles WHERE users.id = author;
        // -->
        // SELECT users.name, articles.title FROM users, articles WHERE users.id = articles.author;
        let q = SelectStatement {
            tables: vec![Table::from("users"), Table::from("articles")],
            fields: vec![
                FieldExpression::Col(Column::from("name")),
                FieldExpression::Col(Column::from("title")),
            ],
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(ConditionBase::Field(Column::from("users.id"))),
                right: wrap(ConditionBase::Field(Column::from("author"))),
            })),
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );
        schema.insert(
            "articles".into(),
            vec!["id".into(), "title".into(), "text".into(), "author".into()],
        );

        let res = SqlQuery::Select(q).expand_implied_tables(&schema);
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldExpression::Col(Column::from("users.name")),
                        FieldExpression::Col(Column::from("articles.title")),
                    ]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(ConditionExpression::ComparisonOp(ConditionTree {
                        operator: Operator::Equal,
                        left: wrap(ConditionBase::Field(Column::from("users.id"))),
                        right: wrap(ConditionBase::Field(Column::from("articles.author"))),
                    }))
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
