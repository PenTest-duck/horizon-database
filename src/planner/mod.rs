//! # Query Planner
//!
//! Transforms parsed SQL AST nodes into logical and physical execution plans.
//! Currently implements a simple rule-based planner; cost-based optimization
//! will be added in a later phase.

use crate::catalog::Catalog;
use crate::error::{HorizonError, Result};
use crate::sql::ast::*;

/// A logical plan node describing what to compute.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan all rows from a table.
    SeqScan {
        table: String,
        alias: Option<String>,
    },
    /// Filter rows by a predicate.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Project (select) specific columns/expressions.
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<SelectColumn>,
    },
    /// Sort rows.
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByItem>,
    },
    /// Limit the number of rows.
    Limit {
        input: Box<LogicalPlan>,
        limit: Expr,
        offset: Option<Expr>,
    },
    /// Group by and aggregate.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    },
    /// Join two inputs.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        on: Option<Expr>,
    },
    /// Distinct (remove duplicate rows).
    Distinct {
        input: Box<LogicalPlan>,
    },
    /// Insert rows into a table.
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        or_replace: bool,
    },
    /// Update rows in a table.
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        predicate: Option<Expr>,
    },
    /// Delete rows from a table.
    Delete {
        table: String,
        predicate: Option<Expr>,
    },
    /// Create a new table.
    CreateTable(CreateTableStatement),
    /// Drop a table.
    DropTable(DropTableStatement),
    /// Create an index.
    CreateIndex(CreateIndexStatement),
    /// Drop an index.
    DropIndex(DropIndexStatement),
    /// Begin transaction.
    Begin,
    /// Commit transaction.
    Commit,
    /// Rollback transaction.
    Rollback,
    /// Empty result (no-op).
    Empty,
}

/// Plan a parsed SQL statement into a logical plan.
pub fn plan_statement(stmt: &Statement, catalog: &Catalog) -> Result<LogicalPlan> {
    match stmt {
        Statement::Select(select) => plan_select(select, catalog),
        Statement::Insert(insert) => plan_insert(insert, catalog),
        Statement::Update(update) => plan_update(update, catalog),
        Statement::Delete(delete) => plan_delete(delete, catalog),
        Statement::CreateTable(ct) => Ok(LogicalPlan::CreateTable(ct.clone())),
        Statement::DropTable(dt) => Ok(LogicalPlan::DropTable(dt.clone())),
        Statement::CreateIndex(ci) => Ok(LogicalPlan::CreateIndex(ci.clone())),
        Statement::DropIndex(di) => Ok(LogicalPlan::DropIndex(di.clone())),
        Statement::AlterTable(_) => Err(HorizonError::NotImplemented("ALTER TABLE".into())),
        Statement::Explain(inner) => plan_statement(inner, catalog),
        Statement::ExplainQueryPlan(inner) => plan_statement(inner, catalog),
        Statement::Pragma(_) => Ok(LogicalPlan::Empty),
        Statement::Begin => Ok(LogicalPlan::Begin),
        Statement::Commit => Ok(LogicalPlan::Commit),
        Statement::Rollback => Ok(LogicalPlan::Rollback),
        Statement::CreateView(_) => Err(HorizonError::NotImplemented("CREATE VIEW".into())),
        Statement::DropView(_) => Err(HorizonError::NotImplemented("DROP VIEW".into())),
        Statement::CreateTrigger(_) => Err(HorizonError::NotImplemented("CREATE TRIGGER".into())),
        Statement::DropTrigger(_) => Err(HorizonError::NotImplemented("DROP TRIGGER".into())),
        Statement::AttachDatabase(_) => Err(HorizonError::NotImplemented("ATTACH DATABASE".into())),
        Statement::DetachDatabase(_) => Err(HorizonError::NotImplemented("DETACH DATABASE".into())),
        Statement::Vacuum => Err(HorizonError::NotImplemented("VACUUM".into())),
        Statement::CreateVirtualTable(_) => Err(HorizonError::NotImplemented("CREATE VIRTUAL TABLE".into())),
    }
}

fn plan_select(select: &SelectStatement, _catalog: &Catalog) -> Result<LogicalPlan> {
    // Build the plan bottom-up: scan -> filter -> aggregate -> having -> project -> sort -> distinct -> limit

    // 1. FROM clause -> base scan or join
    let mut plan = if let Some(ref from) = select.from {
        plan_from(from)?
    } else {
        // SELECT without FROM (e.g., SELECT 1+1)
        LogicalPlan::Empty
    };

    // 2. WHERE -> filter
    if let Some(ref where_clause) = select.where_clause {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: where_clause.clone(),
        };
    }

    // 3. GROUP BY -> aggregate
    if !select.group_by.is_empty() || has_aggregate_functions(&select.columns) {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            having: select.having.clone(),
        };
    }

    // 4. Project
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        columns: select.columns.clone(),
    };

    // 5. DISTINCT
    if select.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    // 6. ORDER BY
    if !select.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: select.order_by.clone(),
        };
    }

    // 7. LIMIT / OFFSET
    if let Some(ref limit) = select.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: limit.clone(),
            offset: select.offset.clone(),
        };
    }

    Ok(plan)
}

fn plan_from(from: &FromClause) -> Result<LogicalPlan> {
    match from {
        FromClause::Table { name, alias } => Ok(LogicalPlan::SeqScan {
            table: name.clone(),
            alias: alias.clone(),
        }),
        FromClause::Join {
            left,
            join_type,
            right,
            on,
        } => {
            let left_plan = plan_from(left)?;
            let right_plan = plan_from(right)?;
            Ok(LogicalPlan::Join {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                join_type: join_type.clone(),
                on: on.clone(),
            })
        }
        FromClause::Subquery { query: _, alias: _ } => {
            // TODO: Handle subqueries in FROM
            Err(HorizonError::NotImplemented("subquery in FROM clause".into()))
        }
        FromClause::TableFunction { .. } => {
            Err(HorizonError::NotImplemented("table function in FROM clause".into()))
        }
    }
}

fn plan_insert(insert: &InsertStatement, _catalog: &Catalog) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Insert {
        table: insert.table.clone(),
        columns: insert.columns.clone(),
        values: insert.values.clone(),
        or_replace: insert.or_replace,
    })
}

fn plan_update(update: &UpdateStatement, _catalog: &Catalog) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Update {
        table: update.table.clone(),
        assignments: update.assignments.clone(),
        predicate: update.where_clause.clone(),
    })
}

fn plan_delete(delete: &DeleteStatement, _catalog: &Catalog) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Delete {
        table: delete.table.clone(),
        predicate: delete.where_clause.clone(),
    })
}

/// Check if any select columns contain aggregate function calls.
fn has_aggregate_functions(columns: &[SelectColumn]) -> bool {
    for col in columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if expr_has_aggregate(expr) {
                return true;
            }
        }
    }
    false
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } => {
            let upper = name.to_uppercase();
            matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
            )
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::Parser;

    fn plan(sql: &str) -> LogicalPlan {
        let stmts = Parser::parse(sql).unwrap();
        let catalog = Catalog::new();
        plan_statement(&stmts[0], &catalog).unwrap()
    }

    #[test]
    fn test_plan_simple_select() {
        let p = plan("SELECT * FROM users");
        match p {
            LogicalPlan::Project { input, .. } => {
                match *input {
                    LogicalPlan::SeqScan { table, .. } => assert_eq!(table, "users"),
                    other => panic!("expected SeqScan, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[test]
    fn test_plan_select_with_where() {
        let p = plan("SELECT id FROM users WHERE id > 5");
        match p {
            LogicalPlan::Project { input, .. } => {
                match *input {
                    LogicalPlan::Filter { .. } => {}
                    other => panic!("expected Filter, got {:?}", other),
                }
            }
            other => panic!("expected Project, got {:?}", other),
        }
    }

    #[test]
    fn test_plan_create_table() {
        let p = plan("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(matches!(p, LogicalPlan::CreateTable(_)));
    }

    #[test]
    fn test_plan_insert() {
        let p = plan("INSERT INTO users VALUES (1, 'Alice')");
        assert!(matches!(p, LogicalPlan::Insert { .. }));
    }
}
