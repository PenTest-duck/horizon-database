//! # Execution Engine
//!
//! Executes SQL statements against the storage layer. This module bridges
//! the SQL parser/planner with the B+Tree storage, catalog, and MVCC layers.

use std::sync::Arc;
use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, ColumnInfo, TableInfo};
use crate::error::{HorizonError, Result};
use crate::mvcc::{TransactionManager, UndoEntry};
use crate::planner::{LogicalPlan, plan_statement};
use crate::sql::ast::*;
use crate::types::{DataType, Value, determine_affinity};
use crate::{QueryResult, Row};

/// Execute a non-query statement (DDL, INSERT, UPDATE, DELETE).
/// Returns the number of affected rows.
pub fn execute_statement(
    stmt: &Statement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    match stmt {
        Statement::CreateTable(ct) => execute_create_table(ct, pool, catalog),
        Statement::DropTable(dt) => execute_drop_table(dt, pool, catalog),
        Statement::Insert(ins) => execute_insert(ins, pool, catalog, txn_mgr),
        Statement::Update(upd) => execute_update(upd, pool, catalog, txn_mgr),
        Statement::Delete(del) => execute_delete(del, pool, catalog, txn_mgr),
        Statement::CreateIndex(ci) => execute_create_index(ci, pool, catalog),
        Statement::DropIndex(di) => execute_drop_index(di, pool, catalog),
        Statement::Begin => {
            txn_mgr.begin_user_txn()?;
            Ok(0)
        }
        Statement::Commit => {
            txn_mgr.commit_user_txn()?;
            Ok(0)
        }
        Statement::Rollback => {
            execute_rollback(pool, catalog, txn_mgr)
        }
        Statement::AlterTable(alter) => execute_alter_table(alter, pool, catalog),
        Statement::Explain(_) => {
            // EXPLAIN returns rows; handled in Database::query()
            Err(HorizonError::Internal("use query() for EXPLAIN statements".into()))
        }
        Statement::Pragma(_) => {
            // PRAGMA returns rows; handled in Database::query()
            Err(HorizonError::Internal("use query() for PRAGMA statements".into()))
        }
        Statement::CreateView(_) => {
            Err(HorizonError::NotImplemented("CREATE VIEW".into()))
        }
        Statement::DropView(_) => {
            Err(HorizonError::NotImplemented("DROP VIEW".into()))
        }
        Statement::CreateTrigger(_) => {
            Err(HorizonError::NotImplemented("CREATE TRIGGER".into()))
        }
        Statement::DropTrigger(_) => {
            Err(HorizonError::NotImplemented("DROP TRIGGER".into()))
        }
        Statement::Select(_) => {
            Err(HorizonError::Internal("use execute_query for SELECT statements".into()))
        }
    }
}

/// Execute a SELECT query and return results.
pub fn execute_query(
    stmt: &Statement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    _txn_mgr: &mut TransactionManager,
) -> Result<QueryResult> {
    match stmt {
        Statement::Select(select) => execute_select(select, pool, catalog),
        Statement::Pragma(pragma) => execute_pragma(pragma, pool, catalog),
        Statement::Explain(inner) => execute_explain(inner, catalog),
        _ => Err(HorizonError::Internal("execute_query requires a SELECT, PRAGMA, or EXPLAIN statement".into())),
    }
}

// ---- CREATE TABLE ----

fn execute_create_table(
    ct: &CreateTableStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    if ct.if_not_exists && catalog.table_exists(&ct.name) {
        return Ok(0);
    }

    // Create a B+Tree for the table data
    let tree = BTree::create(pool)?;
    let root_page = tree.root_page();

    // Build column info from the statement
    let mut columns = Vec::new();
    let mut pk_column = None;

    for (i, col_def) in ct.columns.iter().enumerate() {
        let type_name = col_def.type_name.clone().unwrap_or_default();
        let affinity = determine_affinity(&type_name);

        if col_def.primary_key {
            pk_column = Some(i);
        }

        columns.push(ColumnInfo {
            name: col_def.name.clone(),
            type_name,
            affinity,
            primary_key: col_def.primary_key,
            autoincrement: col_def.autoincrement,
            not_null: col_def.not_null || col_def.primary_key,
            unique: col_def.unique || col_def.primary_key,
            default_value: col_def.default.as_ref().map(|e| eval_const_expr(e)),
            position: i,
        });
    }

    let table = TableInfo {
        name: ct.name.clone(),
        columns,
        root_page,
        next_rowid: 1,
        pk_column,
    };

    catalog.create_table(pool, table)?;
    Ok(0)
}

// ---- DROP TABLE ----

fn execute_drop_table(
    dt: &DropTableStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    if dt.if_exists && !catalog.table_exists(&dt.name) {
        return Ok(0);
    }
    catalog.drop_table(pool, &dt.name)?;
    Ok(0)
}

// ---- INSERT ----

fn execute_insert(
    ins: &InsertStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    let table = catalog.get_table(&ins.table)?.clone();
    let mut tree = BTree::open(table.root_page);
    let mut next_rowid = table.next_rowid;
    let _txn_id = txn_mgr.auto_commit();
    let mut inserted = 0;

    for value_row in &ins.values {
        // Determine column ordering
        let col_order: Vec<usize> = if let Some(ref col_names) = ins.columns {
            col_names
                .iter()
                .map(|name| {
                    table.find_column_index(name).ok_or_else(|| {
                        HorizonError::ColumnNotFound(format!(
                            "{}.{}", ins.table, name
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            (0..table.columns.len()).collect()
        };

        if value_row.len() != col_order.len() {
            return Err(HorizonError::InvalidSql(format!(
                "expected {} values but got {}",
                col_order.len(),
                value_row.len()
            )));
        }

        // Build the row values
        let mut row_values = vec![Value::Null; table.columns.len()];

        for (val_idx, &col_idx) in col_order.iter().enumerate() {
            let val = eval_expr(&value_row[val_idx], &[], &[], &table)?;
            // Apply type affinity
            let affinity = table.columns[col_idx].affinity;
            row_values[col_idx] = val.apply_affinity(affinity);
        }

        // Set defaults for columns not in the insert list
        for (i, col) in table.columns.iter().enumerate() {
            if !col_order.contains(&i) {
                if let Some(ref default_val) = col.default_value {
                    row_values[i] = default_val.clone();
                }
            }
        }

        // Determine the rowid
        let rowid = if let Some(pk_idx) = table.pk_column {
            if table.columns[pk_idx].affinity == DataType::Integer {
                match &row_values[pk_idx] {
                    Value::Null if table.columns[pk_idx].autoincrement => {
                        let id = next_rowid;
                        row_values[pk_idx] = Value::Integer(id);
                        next_rowid = id + 1;
                        id
                    }
                    Value::Null => {
                        let id = next_rowid;
                        row_values[pk_idx] = Value::Integer(id);
                        next_rowid = id + 1;
                        id
                    }
                    Value::Integer(id) => {
                        if *id >= next_rowid {
                            next_rowid = *id + 1;
                        }
                        *id
                    }
                    _ => {
                        let id = next_rowid;
                        next_rowid = id + 1;
                        id
                    }
                }
            } else {
                let id = next_rowid;
                next_rowid = id + 1;
                id
            }
        } else {
            let id = next_rowid;
            next_rowid = id + 1;
            id
        };

        // Check NOT NULL constraints
        for (i, col) in table.columns.iter().enumerate() {
            if col.not_null && row_values[i].is_null() {
                return Err(HorizonError::ConstraintViolation(format!(
                    "NOT NULL constraint failed: {}.{}",
                    ins.table, col.name
                )));
            }
        }

        // Check for duplicate primary key / handle OR REPLACE
        let key = rowid.to_be_bytes();
        let existing = tree.search(pool, &key)?;
        if let Some(ref old_value) = existing {
            if ins.or_replace {
                // Record undo for the row we are about to overwrite
                txn_mgr.record_undo(UndoEntry::Update {
                    table: ins.table.clone(),
                    root_page: tree.root_page(),
                    key: key.to_vec(),
                    old_value: old_value.clone(),
                });
            } else {
                return Err(HorizonError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {}.rowid",
                    ins.table
                )));
            }
        } else {
            // Record undo for a fresh insert
            txn_mgr.record_undo(UndoEntry::Insert {
                table: ins.table.clone(),
                root_page: tree.root_page(),
                key: key.to_vec(),
            });
        }

        // Serialize row values
        let row_data = serialize_row(&row_values);

        // Insert (or overwrite) into B+Tree: key = rowid (big-endian i64), value = serialized row
        tree.insert(pool, &key, &row_data)?;

        // Maintain indexes: insert index entries for this row
        let indexes = catalog.get_indexes_for_table(&ins.table)
            .iter().map(|idx| (*idx).clone()).collect::<Vec<_>>();
        for idx_info in &indexes {
            let col_indices: Vec<usize> = idx_info.columns.iter()
                .filter_map(|col_name| table.find_column_index(col_name))
                .collect();
            if col_indices.len() == idx_info.columns.len() {
                let index_key = build_index_key(&row_values, &col_indices, &key);
                let mut index_tree = BTree::open(idx_info.root_page);
                index_tree.insert(pool, &index_key, &key)?;
                // Update index root page if it changed due to splits
                if index_tree.root_page() != idx_info.root_page {
                    let mut updated_idx = idx_info.clone();
                    updated_idx.root_page = index_tree.root_page();
                    // We need to update the index metadata in catalog
                    let _ = catalog.drop_index(pool, &idx_info.name);
                    let _ = catalog.create_index(pool, updated_idx);
                }
            }
        }

        // Update root page if it changed due to splits
        if tree.root_page() != table.root_page {
            let mut updated_table = table.clone();
            updated_table.root_page = tree.root_page();
            updated_table.next_rowid = next_rowid;
            catalog.update_table_meta(pool, &ins.table, &updated_table)?;
        }

        inserted += 1;
    }

    // Update next_rowid in catalog
    let mut updated_table = catalog.get_table(&ins.table)?.clone();
    updated_table.next_rowid = next_rowid;
    if tree.root_page() != updated_table.root_page {
        updated_table.root_page = tree.root_page();
    }
    catalog.update_table_meta(pool, &ins.table, &updated_table)?;

    Ok(inserted)
}

// ---- SELECT ----

fn execute_select(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<QueryResult> {
    // Check if we need plan-based execution (JOINs or aggregates)
    let needs_plan = matches!(&select.from, Some(FromClause::Join { .. }))
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select_has_aggregate(&select.columns);

    if needs_plan {
        let stmt = Statement::Select(select.clone());
        let plan = plan_statement(&stmt, catalog)?;
        return execute_plan_select(&plan, pool, catalog);
    }

    // Get the table to scan
    let (table_name, _alias) = match &select.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        Some(FromClause::Join { .. }) => {
            unreachable!("JOINs handled by plan-based path above");
        }
        Some(FromClause::Subquery { .. }) => {
            return Err(HorizonError::NotImplemented("subqueries in FROM".into()));
        }
        None => {
            // SELECT without FROM -- evaluate expressions directly
            return execute_select_no_from(select);
        }
    };

    let table = catalog.get_table(&table_name)?.clone();
    let data_tree = BTree::open(table.root_page);

    // Try to use an index scan if the WHERE clause has a simple predicate
    // on an indexed column.
    let entries = if let Some(ref where_clause) = select.where_clause {
        if let Some(index_entries) = try_index_scan(where_clause, &table_name, &table, pool, catalog)? {
            index_entries
        } else {
            data_tree.scan_all(pool)?
        }
    } else {
        data_tree.scan_all(pool)?
    };

    // Determine output column names
    let column_names = resolve_column_names(&select.columns, &table)?;
    let columns = Arc::new(column_names);

    let mut rows = Vec::new();

    for entry in &entries {
        // Deserialize row values
        let row_values = deserialize_row(&entry.value, table.columns.len())?;

        // Evaluate WHERE clause (still needed even with index scan for correctness,
        // e.g. when index scan returns a superset for range queries).
        // Use eval_expr_with_ctx to support subqueries (EXISTS, scalar subquery) in WHERE.
        if let Some(ref where_clause) = select.where_clause {
            let result = eval_expr_with_ctx(where_clause, &row_values, &table.columns, &table, pool, catalog)?;
            if !result.to_bool() {
                continue;
            }
        }

        // Project columns using subquery-aware evaluation
        let projected = project_row_with_ctx(&select.columns, &row_values, &table, pool, catalog)?;

        rows.push(Row {
            columns: columns.clone(),
            values: projected,
        });
    }

    // Handle ORDER BY
    if !select.order_by.is_empty() {
        sort_rows(&mut rows, &select.order_by, &table)?;
    }

    // Handle DISTINCT
    if select.distinct {
        rows.dedup_by(|a, b| a.values == b.values);
    }

    // Handle LIMIT / OFFSET
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_expr(offset_expr)
            .as_integer()
            .unwrap_or(0) as usize;
        if offset < rows.len() {
            rows = rows.into_iter().skip(offset).collect();
        } else {
            rows.clear();
        }
    }

    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_expr(limit_expr)
            .as_integer()
            .unwrap_or(i64::MAX) as usize;
        rows.truncate(limit);
    }

    Ok(QueryResult { columns, rows })
}

/// Check if any select columns contain aggregate function calls.
fn select_has_aggregate(columns: &[SelectColumn]) -> bool {
    for col in columns {
        if let SelectColumn::Expr { expr, .. } = col {
            if expr_has_aggregate_fn(expr) {
                return true;
            }
        }
    }
    false
}

/// Recursively check if an expression contains an aggregate function.
fn expr_has_aggregate_fn(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            let upper = name.to_uppercase();
            // MIN and MAX with 2+ args are scalar functions, not aggregates
            if (upper == "MIN" || upper == "MAX") && args.len() >= 2 {
                // Check if any arguments contain aggregate functions
                return args.iter().any(|a| expr_has_aggregate_fn(a));
            }
            matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
            )
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate_fn(left) || expr_has_aggregate_fn(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_aggregate_fn(expr),
        Expr::Cast { expr, .. } => expr_has_aggregate_fn(expr),
        _ => false,
    }
}

// ---- Plan-Based Execution (JOINs and Aggregates) ----

/// Execute a SELECT via the logical plan (used for JOINs and aggregates).
fn execute_plan_select(
    plan: &LogicalPlan,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<QueryResult> {
    let (col_names, rows) = execute_plan_rows(plan, pool, catalog)?;
    let columns = Arc::new(col_names);
    let rows = rows
        .into_iter()
        .map(|values| Row {
            columns: columns.clone(),
            values,
        })
        .collect();
    Ok(QueryResult { columns, rows })
}

/// Recursively execute a logical plan, returning (column_names, rows).
/// Each row is a Vec<Value>.
fn execute_plan_rows(
    plan: &LogicalPlan,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    match plan {
        LogicalPlan::SeqScan { table, alias } => {
            let table_info = catalog.get_table(table)?.clone();
            let data_tree = BTree::open(table_info.root_page);
            let entries = data_tree.scan_all(pool)?;

            let prefix = alias.as_deref().unwrap_or(table);
            let col_names: Vec<String> = table_info
                .columns
                .iter()
                .map(|c| format!("{}.{}", prefix, c.name))
                .collect();

            let mut rows = Vec::new();
            for entry in &entries {
                let row_values = deserialize_row(&entry.value, table_info.columns.len())?;
                rows.push(row_values);
            }
            Ok((col_names, rows))
        }

        LogicalPlan::Filter { input, predicate } => {
            let (col_names, rows) = execute_plan_rows(input, pool, catalog)?;
            let mut filtered = Vec::new();
            for row in rows {
                let val = eval_expr_dynamic(predicate, &row, &col_names)?;
                if val.to_bool() {
                    filtered.push(row);
                }
            }
            Ok((col_names, filtered))
        }

        LogicalPlan::Project { input, columns } => {
            // Check if input is an Aggregate -- if so, do combined aggregate+project
            if let LogicalPlan::Aggregate {
                input: agg_input,
                group_by,
                having,
            } = input.as_ref()
            {
                return execute_aggregate_project(
                    agg_input, group_by, having, columns, pool, catalog,
                );
            }

            let (input_col_names, input_rows) = execute_plan_rows(input, pool, catalog)?;

            // Resolve output column names
            let out_col_names =
                resolve_column_names_dynamic(columns, &input_col_names)?;

            let mut out_rows = Vec::new();
            for row in &input_rows {
                let projected =
                    project_row_dynamic(columns, row, &input_col_names)?;
                out_rows.push(projected);
            }
            Ok((out_col_names, out_rows))
        }

        LogicalPlan::Sort { input, order_by } => {
            let (col_names, mut rows) = execute_plan_rows(input, pool, catalog)?;
            sort_rows_dynamic(&mut rows, order_by, &col_names)?;
            Ok((col_names, rows))
        }

        LogicalPlan::Limit { input, limit, offset } => {
            let (col_names, mut rows) = execute_plan_rows(input, pool, catalog)?;

            if let Some(ref offset_expr) = offset {
                let off = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
                if off < rows.len() {
                    rows = rows.into_iter().skip(off).collect();
                } else {
                    rows.clear();
                }
            }

            let lim = eval_const_expr(limit).as_integer().unwrap_or(i64::MAX) as usize;
            rows.truncate(lim);

            Ok((col_names, rows))
        }

        LogicalPlan::Distinct { input } => {
            let (col_names, rows) = execute_plan_rows(input, pool, catalog)?;
            let mut seen = Vec::new();
            let mut unique = Vec::new();
            for row in rows {
                if !seen.contains(&row) {
                    seen.push(row.clone());
                    unique.push(row);
                }
            }
            Ok((col_names, unique))
        }

        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
        } => execute_join(left, right, join_type, on, pool, catalog),

        LogicalPlan::Aggregate {
            input,
            group_by,
            having,
        } => {
            // Standalone aggregate without Project on top.
            let (col_names, rows) = execute_plan_rows(input, pool, catalog)?;
            let groups = group_rows(&rows, group_by, &col_names)?;

            let mut result = Vec::new();
            for (_key, group) in &groups {
                if let Some(ref having_expr) = having {
                    let first = &group[0];
                    let val = eval_aggregate_expr(having_expr, first, &col_names, group)?;
                    if !val.to_bool() {
                        continue;
                    }
                }
                result.push(group[0].clone());
            }
            Ok((col_names, result))
        }

        LogicalPlan::Empty => {
            Ok((vec![], vec![vec![]]))
        }

        _ => Err(HorizonError::NotImplemented(
            "plan node in SELECT context".into(),
        )),
    }
}

// ---- JOIN Execution (Nested Loop Join) ----

fn execute_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: &JoinType,
    on: &Option<Expr>,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let (left_cols, left_rows) = execute_plan_rows(left, pool, catalog)?;
    let (right_cols, right_rows) = execute_plan_rows(right, pool, catalog)?;

    let num_right = right_cols.len();
    let num_left = left_cols.len();

    // Merged column names: left columns followed by right columns
    let mut merged_cols = left_cols.clone();
    merged_cols.extend(right_cols.clone());

    let null_right: Vec<Value> = vec![Value::Null; num_right];
    let null_left: Vec<Value> = vec![Value::Null; num_left];

    let mut result = Vec::new();

    match join_type {
        JoinType::Inner => {
            for l_row in &left_rows {
                for r_row in &right_rows {
                    let mut merged = l_row.clone();
                    merged.extend(r_row.iter().cloned());
                    if let Some(ref on_expr) = on {
                        let val = eval_expr_dynamic(on_expr, &merged, &merged_cols)?;
                        if !val.to_bool() {
                            continue;
                        }
                    }
                    result.push(merged);
                }
            }
        }

        JoinType::Left => {
            for l_row in &left_rows {
                let mut matched = false;
                for r_row in &right_rows {
                    let mut merged = l_row.clone();
                    merged.extend(r_row.iter().cloned());
                    if let Some(ref on_expr) = on {
                        let val = eval_expr_dynamic(on_expr, &merged, &merged_cols)?;
                        if !val.to_bool() {
                            continue;
                        }
                    }
                    matched = true;
                    result.push(merged);
                }
                if !matched {
                    let mut merged = l_row.clone();
                    merged.extend(null_right.iter().cloned());
                    result.push(merged);
                }
            }
        }

        JoinType::Right => {
            for r_row in &right_rows {
                let mut matched = false;
                for l_row in &left_rows {
                    let mut merged = l_row.clone();
                    merged.extend(r_row.iter().cloned());
                    if let Some(ref on_expr) = on {
                        let val = eval_expr_dynamic(on_expr, &merged, &merged_cols)?;
                        if !val.to_bool() {
                            continue;
                        }
                    }
                    matched = true;
                    result.push(merged);
                }
                if !matched {
                    let mut merged = null_left.clone();
                    merged.extend(r_row.iter().cloned());
                    result.push(merged);
                }
            }
        }

        JoinType::Cross => {
            for l_row in &left_rows {
                for r_row in &right_rows {
                    let mut merged = l_row.clone();
                    merged.extend(r_row.iter().cloned());
                    result.push(merged);
                }
            }
        }
    }

    Ok((merged_cols, result))
}

// ---- Aggregate + Project Execution ----

/// Execute an Aggregate node followed by a Project node.
/// Groups rows, applies HAVING, then projects with aggregate-aware evaluation.
fn execute_aggregate_project(
    agg_input: &LogicalPlan,
    group_by: &[Expr],
    having: &Option<Expr>,
    select_columns: &[SelectColumn],
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let (input_col_names, input_rows) = execute_plan_rows(agg_input, pool, catalog)?;

    let groups = group_rows(&input_rows, group_by, &input_col_names)?;

    // Resolve output column names
    let out_col_names = resolve_column_names_dynamic(select_columns, &input_col_names)?;

    let mut result = Vec::new();

    for (_key, group) in &groups {
        // Skip empty groups (shouldn't normally happen, but guard against it)
        // However, for aggregates without GROUP BY on an empty table, we have
        // one group with zero rows -- we still need to produce a result row.
        let representative: Vec<Value> = if group.is_empty() {
            vec![Value::Null; input_col_names.len()]
        } else {
            group[0].clone()
        };

        // Apply HAVING filter
        if let Some(ref having_expr) = having {
            let val = eval_aggregate_expr(having_expr, &representative, &input_col_names, group)?;
            if !val.to_bool() {
                continue;
            }
        }

        // Project each group into an output row
        let mut out_row = Vec::new();

        for col in select_columns {
            match col {
                SelectColumn::AllColumns => {
                    out_row.extend(representative.iter().cloned());
                }
                SelectColumn::TableAllColumns(_) => {
                    out_row.extend(representative.iter().cloned());
                }
                SelectColumn::Expr { expr, .. } => {
                    let val = eval_aggregate_expr(expr, &representative, &input_col_names, group)?;
                    out_row.push(val);
                }
            }
        }

        result.push(out_row);
    }

    Ok((out_col_names, result))
}

/// Group rows by GROUP BY expressions. Returns an ordered list of (key, group_rows).
/// If group_by is empty, all rows go into a single group.
fn group_rows(
    rows: &[Vec<Value>],
    group_by: &[Expr],
    col_names: &[String],
) -> Result<Vec<(Vec<Value>, Vec<Vec<Value>>)>> {
    if group_by.is_empty() {
        // Entire input is one group
        return Ok(vec![(vec![], rows.to_vec())]);
    }

    let mut map: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();

    for row in rows {
        let mut key = Vec::new();
        for expr in group_by {
            key.push(eval_expr_dynamic(expr, row, col_names)?);
        }

        // Find or create the group
        let found = map.iter_mut().find(|(k, _)| *k == key);
        if let Some((_, group)) = found {
            group.push(row.clone());
        } else {
            map.push((key, vec![row.clone()]));
        }
    }

    Ok(map)
}

// ---- Dynamic Expression Evaluation (column-name based, for JOINs & aggregates) ----

/// Evaluate an expression using column names (not TableInfo).
/// Column lookup: tries exact match, then suffix match (e.g., "id" matches "users.id").
fn eval_expr_dynamic(
    expr: &Expr,
    row: &[Value],
    col_names: &[String],
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),

        Expr::Column { table: tbl, name } => {
            let qualified = if let Some(t) = tbl {
                format!("{}.{}", t, name)
            } else {
                name.clone()
            };
            find_column_dynamic(&qualified, row, col_names)
        }

        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr_dynamic(left, row, col_names)?;
            let r = eval_expr_dynamic(right, row, col_names)?;
            Ok(eval_binary_op(&l, op, &r))
        }

        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            Ok(eval_unary_op(op, &val))
        }

        Expr::IsNull { expr: inner, negated } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            let is_null = val.is_null();
            Ok(Value::Integer(if is_null != *negated { 1 } else { 0 }))
        }

        Expr::Between { expr: inner, low, high, negated } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            let lo = eval_expr_dynamic(low, row, col_names)?;
            let hi = eval_expr_dynamic(high, row, col_names)?;
            let in_range = val >= lo && val <= hi;
            Ok(Value::Integer(if in_range != *negated { 1 } else { 0 }))
        }

        Expr::InList { expr: inner, list, negated } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            let mut found = false;
            for item in list {
                if val == eval_expr_dynamic(item, row, col_names)? {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if found != *negated { 1 } else { 0 }))
        }

        Expr::Like { expr: inner, pattern, negated } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            let pat = eval_expr_dynamic(pattern, row, col_names)?;
            let matches = match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => sql_like_match(s, p),
                _ => false,
            };
            Ok(Value::Integer(if matches != *negated { 1 } else { 0 }))
        }

        Expr::Function { name, args, distinct: _ } => {
            eval_function_dynamic(name, args, row, col_names)
        }

        Expr::Cast { expr: inner, type_name } => {
            let val = eval_expr_dynamic(inner, row, col_names)?;
            let affinity = determine_affinity(type_name);
            Ok(val.apply_affinity(affinity))
        }

        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_expr_dynamic(operand_expr, row, col_names)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_dynamic(when_expr, row, col_names)?;
                    if op_val == when_val {
                        return eval_expr_dynamic(then_expr, row, col_names);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_dynamic(when_expr, row, col_names)?;
                    if when_val.to_bool() {
                        return eval_expr_dynamic(then_expr, row, col_names);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_expr_dynamic(else_expr, row, col_names)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::Placeholder(_) => Ok(Value::Null),
        Expr::Subquery(_) | Expr::Exists(_) => {
            Err(HorizonError::NotImplemented("subqueries in expressions".into()))
        }
    }
}

/// Look up a column value by name in a dynamic row.
/// Tries: (1) exact case-insensitive match, (2) suffix match.
fn find_column_dynamic(
    col_ref: &str,
    row: &[Value],
    col_names: &[String],
) -> Result<Value> {
    // Exact match (case-insensitive)
    for (i, name) in col_names.iter().enumerate() {
        if name.eq_ignore_ascii_case(col_ref) {
            return Ok(row.get(i).cloned().unwrap_or(Value::Null));
        }
    }

    // Suffix match: if col_ref has no dot, try matching the part after the dot in col_names
    if !col_ref.contains('.') {
        let suffix = format!(".{}", col_ref);
        let mut matches: Vec<usize> = Vec::new();
        for (i, name) in col_names.iter().enumerate() {
            if name.to_lowercase().ends_with(&suffix.to_lowercase()) {
                matches.push(i);
            }
        }
        if matches.len() == 1 {
            return Ok(row.get(matches[0]).cloned().unwrap_or(Value::Null));
        }
        if matches.len() > 1 {
            return Err(HorizonError::InvalidSql(format!(
                "ambiguous column reference: {}",
                col_ref
            )));
        }
    }

    // Check rowid
    if col_ref.eq_ignore_ascii_case("rowid") {
        return Ok(Value::Null);
    }

    Err(HorizonError::ColumnNotFound(col_ref.to_string()))
}

/// Evaluate a scalar function in dynamic (column-name) context.
fn eval_function_dynamic(
    name: &str,
    args: &[Expr],
    row: &[Value],
    col_names: &[String],
) -> Result<Value> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "ABS" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::Real(r) => Ok(Value::Real(r.abs())),
                _ => Ok(Value::Null),
            }
        }
        "LENGTH" | "LEN" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Integer(format!("{}", val).len() as i64)),
            }
        }
        "UPPER" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                _ => Ok(val),
            }
        }
        "LOWER" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                _ => Ok(val),
            }
        }
        "TYPEOF" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            let type_str = match val {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
            };
            Ok(Value::Text(type_str.to_string()))
        }
        "COALESCE" => {
            for arg in args {
                let val = eval_expr_dynamic(arg, row, col_names)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() >= 2 {
                let val = eval_expr_dynamic(&args[0], row, col_names)?;
                if val.is_null() {
                    eval_expr_dynamic(&args[1], row, col_names)
                } else {
                    Ok(val)
                }
            } else {
                Ok(Value::Null)
            }
        }
        "NULLIF" => {
            if args.len() >= 2 {
                let a = eval_expr_dynamic(&args[0], row, col_names)?;
                let b = eval_expr_dynamic(&args[1], row, col_names)?;
                if a == b { Ok(Value::Null) } else { Ok(a) }
            } else {
                Ok(Value::Null)
            }
        }
        "MAX" if args.len() >= 2 => {
            // Scalar MAX: returns the maximum of its arguments
            let mut max_val = eval_expr_dynamic(&args[0], row, col_names)?;
            for arg in &args[1..] {
                let v = eval_expr_dynamic(arg, row, col_names)?;
                if v.is_null() { continue; }
                if max_val.is_null() || v > max_val {
                    max_val = v;
                }
            }
            Ok(max_val)
        }
        "MIN" if args.len() >= 2 => {
            // Scalar MIN: returns the minimum of its arguments
            let mut min_val = eval_expr_dynamic(&args[0], row, col_names)?;
            for arg in &args[1..] {
                let v = eval_expr_dynamic(arg, row, col_names)?;
                if v.is_null() { continue; }
                if min_val.is_null() || v < min_val {
                    min_val = v;
                }
            }
            Ok(min_val)
        }
        // Aggregate functions in per-row context just evaluate the argument
        "MAX" | "MIN" | "COUNT" | "SUM" | "AVG" | "TOTAL" | "GROUP_CONCAT" => {
            let is_star = args.len() == 1 && matches!(&args[0], Expr::Column { table: None, name } if name == "*");
            if is_star || args.is_empty() {
                // COUNT(*) in per-row context: return 1 for each row
                Ok(Value::Integer(1))
            } else {
                eval_expr_dynamic(&args[0], row, col_names)
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            let start = eval_expr_dynamic(&args[1], row, col_names)?;
            let len = if args.len() > 2 {
                eval_expr_dynamic(&args[2], row, col_names)?.as_integer()
            } else {
                None
            };
            match (val, start.as_integer()) {
                (Value::Text(s), Some(start_pos)) => {
                    let start_idx = if start_pos > 0 { (start_pos - 1) as usize } else { 0 };
                    let substr_result = if let Some(l) = len {
                        s.chars().skip(start_idx).take(l as usize).collect::<String>()
                    } else {
                        s.chars().skip(start_idx).collect::<String>()
                    };
                    Ok(Value::Text(substr_result))
                }
                _ => Ok(Value::Null),
            }
        }
        "TRIM" => {
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                _ => Ok(val),
            }
        }
        "REPLACE" => {
            if args.len() < 3 {
                return Ok(Value::Null);
            }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            let from = eval_expr_dynamic(&args[1], row, col_names)?;
            let to = eval_expr_dynamic(&args[2], row, col_names)?;
            match (val, from, to) {
                (Value::Text(s), Value::Text(f), Value::Text(t)) => {
                    Ok(Value::Text(s.replace(&f, &t)))
                }
                _ => Ok(Value::Null),
            }
        }
        "INSTR" => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            let search = eval_expr_dynamic(&args[1], row, col_names)?;
            match (val, search) {
                (Value::Text(s), Value::Text(needle)) => {
                    Ok(Value::Integer(s.find(&needle).map(|i| i as i64 + 1).unwrap_or(0)))
                }
                _ => Ok(Value::Integer(0)),
            }
        }
        "LTRIM" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim_start().to_string())),
                _ => Ok(val),
            }
        }
        "RTRIM" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim_end().to_string())),
                _ => Ok(val),
            }
        }
        "HEX" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Text(s) => {
                    let hex: String = s.as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Integer(i) => {
                    let hex: String = i.to_string().as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Null => Ok(Value::Null),
                Value::Real(r) => {
                    let hex: String = r.to_string().as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
            }
        }
        "ROUND" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            let decimals = if args.len() > 1 {
                eval_expr_dynamic(&args[1], row, col_names)?.as_integer().unwrap_or(0)
            } else {
                0
            };
            match val {
                Value::Real(r) => {
                    let factor = 10f64.powi(decimals as i32);
                    Ok(Value::Real((r * factor).round() / factor))
                }
                Value::Integer(i) => {
                    if decimals >= 0 {
                        Ok(Value::Real(i as f64))
                    } else {
                        let factor = 10f64.powi((-decimals) as i32);
                        Ok(Value::Real(((i as f64 / factor).round()) * factor))
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "RANDOM" => {
            // Simple pseudo-random integer using system time
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            // Mix bits for a reasonable pseudo-random value
            let val = (now.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)) as i64;
            Ok(Value::Integer(val))
        }
        "ZEROBLOB" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Integer(n) => {
                    let size = n.max(0) as usize;
                    Ok(Value::Blob(vec![0u8; size]))
                }
                _ => Ok(Value::Null),
            }
        }
        "IIF" => {
            if args.len() < 3 { return Ok(Value::Null); }
            let cond = eval_expr_dynamic(&args[0], row, col_names)?;
            if cond.to_bool() {
                eval_expr_dynamic(&args[1], row, col_names)
            } else {
                eval_expr_dynamic(&args[2], row, col_names)
            }
        }
        _ => Err(HorizonError::NotImplemented(format!("function: {}", name))),
    }
}

/// Evaluate an expression that may contain aggregate functions, given a group of rows.
/// For aggregate functions (COUNT, SUM, etc.), compute over the entire group.
/// For non-aggregate expressions, evaluate against the representative row.
fn eval_aggregate_expr(
    expr: &Expr,
    representative: &[Value],
    col_names: &[String],
    group: &[Vec<Value>],
) -> Result<Value> {
    match expr {
        Expr::Function { name, args, distinct: _ } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "COUNT" => {
                    let is_star = args.len() == 1 && matches!(&args[0], Expr::Column { table: None, name } if name == "*");
                    if args.is_empty() || is_star {
                        // COUNT(*) -- count all rows in the group
                        Ok(Value::Integer(group.len() as i64))
                    } else {
                        // COUNT(expr) -- count non-NULL values
                        let mut count = 0i64;
                        for row in group {
                            let val = eval_expr_dynamic(&args[0], row, col_names)?;
                            if !val.is_null() {
                                count += 1;
                            }
                        }
                        Ok(Value::Integer(count))
                    }
                }
                "SUM" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut int_sum: i64 = 0;
                    let mut real_sum: f64 = 0.0;
                    let mut has_real = false;
                    let mut all_null = true;
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        match val {
                            Value::Integer(i) => {
                                int_sum += i;
                                all_null = false;
                            }
                            Value::Real(r) => {
                                real_sum += r;
                                has_real = true;
                                all_null = false;
                            }
                            Value::Null => {}
                            _ => { all_null = false; }
                        }
                    }
                    if all_null {
                        Ok(Value::Null)
                    } else if has_real {
                        Ok(Value::Real(real_sum + int_sum as f64))
                    } else {
                        Ok(Value::Integer(int_sum))
                    }
                }
                "AVG" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut sum: f64 = 0.0;
                    let mut count: i64 = 0;
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        match val {
                            Value::Integer(i) => {
                                sum += i as f64;
                                count += 1;
                            }
                            Value::Real(r) => {
                                sum += r;
                                count += 1;
                            }
                            Value::Null => {}
                            _ => { count += 1; }
                        }
                    }
                    if count == 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real(sum / count as f64))
                    }
                }
                "MIN" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut min_val: Option<Value> = None;
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        if val.is_null() {
                            continue;
                        }
                        min_val = Some(match min_val {
                            None => val,
                            Some(cur) => if val < cur { val } else { cur },
                        });
                    }
                    Ok(min_val.unwrap_or(Value::Null))
                }
                "MAX" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut max_val: Option<Value> = None;
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        if val.is_null() {
                            continue;
                        }
                        max_val = Some(match max_val {
                            None => val,
                            Some(cur) => if val > cur { val } else { cur },
                        });
                    }
                    Ok(max_val.unwrap_or(Value::Null))
                }
                // Non-aggregate functions: evaluate on representative row
                _ => eval_function_dynamic(name, args, representative, col_names),
            }
        }

        // For non-function expressions, recurse to find nested aggregate calls
        Expr::BinaryOp { left, op, right } => {
            let l = eval_aggregate_expr(left, representative, col_names, group)?;
            let r = eval_aggregate_expr(right, representative, col_names, group)?;
            Ok(eval_binary_op(&l, op, &r))
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_aggregate_expr(inner, representative, col_names, group)?;
            Ok(eval_unary_op(op, &val))
        }
        Expr::Cast { expr: inner, type_name } => {
            let val = eval_aggregate_expr(inner, representative, col_names, group)?;
            let affinity = determine_affinity(type_name);
            Ok(val.apply_affinity(affinity))
        }

        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_aggregate_expr(operand_expr, representative, col_names, group)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_aggregate_expr(when_expr, representative, col_names, group)?;
                    if op_val == when_val {
                        return eval_aggregate_expr(then_expr, representative, col_names, group);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_aggregate_expr(when_expr, representative, col_names, group)?;
                    if when_val.to_bool() {
                        return eval_aggregate_expr(then_expr, representative, col_names, group);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_aggregate_expr(else_expr, representative, col_names, group)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::IsNull { expr: inner, negated } => {
            let val = eval_aggregate_expr(inner, representative, col_names, group)?;
            let is_null = val.is_null();
            Ok(Value::Integer(if is_null != *negated { 1 } else { 0 }))
        }

        // Non-aggregate leaf expressions: evaluate on representative row
        _ => eval_expr_dynamic(expr, representative, col_names),
    }
}

// ---- Dynamic helpers for plan-based execution ----

/// Resolve output column names for a Project node.
fn resolve_column_names_dynamic(
    select_cols: &[SelectColumn],
    input_col_names: &[String],
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                // Strip the "table." prefix for cleaner output
                for c in input_col_names {
                    let short = if let Some(pos) = c.find('.') {
                        c[pos + 1..].to_string()
                    } else {
                        c.clone()
                    };
                    names.push(short);
                }
            }
            SelectColumn::TableAllColumns(table_prefix) => {
                let prefix_dot = format!("{}.", table_prefix);
                for c in input_col_names {
                    if c.to_lowercase().starts_with(&prefix_dot.to_lowercase()) {
                        names.push(c[prefix_dot.len()..].to_string());
                    }
                }
            }
            SelectColumn::Expr { expr, alias } => {
                if let Some(a) = alias {
                    names.push(a.clone());
                } else if let Expr::Column { name, .. } = expr {
                    names.push(name.clone());
                } else if let Expr::Function { name, .. } = expr {
                    names.push(name.clone());
                } else {
                    names.push(format!("{:?}", expr));
                }
            }
        }
    }
    Ok(names)
}

/// Project a row in dynamic (column-name) context.
fn project_row_dynamic(
    select_cols: &[SelectColumn],
    row: &[Value],
    col_names: &[String],
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                values.extend(row.iter().cloned());
            }
            SelectColumn::TableAllColumns(table_prefix) => {
                let prefix_dot = format!("{}.", table_prefix);
                for (i, c) in col_names.iter().enumerate() {
                    if c.to_lowercase().starts_with(&prefix_dot.to_lowercase()) {
                        values.push(row.get(i).cloned().unwrap_or(Value::Null));
                    }
                }
            }
            SelectColumn::Expr { expr, .. } => {
                let val = eval_expr_dynamic(expr, row, col_names)?;
                values.push(val);
            }
        }
    }
    Ok(values)
}

/// Sort rows using column-name-based evaluation.
fn sort_rows_dynamic(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    col_names: &[String],
) -> Result<()> {
    rows.sort_by(|a, b| {
        for item in order_by {
            let val_a = eval_expr_dynamic(&item.expr, a, col_names).unwrap_or(Value::Null);
            let val_b = eval_expr_dynamic(&item.expr, b, col_names).unwrap_or(Value::Null);
            let cmp = val_a.cmp(&val_b);
            let cmp = if item.desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn execute_select_no_from(select: &SelectStatement) -> Result<QueryResult> {
    let mut column_names = Vec::new();
    let mut values = Vec::new();

    for col in &select.columns {
        match col {
            SelectColumn::Expr { expr, alias } => {
                let val = eval_const_expr(expr);
                let name = alias.clone().unwrap_or_else(|| format!("{:?}", expr));
                column_names.push(name);
                values.push(val);
            }
            _ => return Err(HorizonError::InvalidSql("* without FROM".into())),
        }
    }

    let columns = Arc::new(column_names);
    let rows = vec![Row {
        columns: columns.clone(),
        values,
    }];

    Ok(QueryResult { columns, rows })
}

// ---- INDEX SCAN ----

/// Attempt to use an index to satisfy a WHERE clause predicate.
/// Returns Some(entries) if an index scan was performed, where entries are
/// the matching rows from the data table (key=rowid, value=serialized row).
/// Returns None if no suitable index exists or the predicate is too complex.
fn try_index_scan(
    where_clause: &Expr,
    table_name: &str,
    table: &TableInfo,
    pool: &mut BufferPool,
    catalog: &Catalog,
) -> Result<Option<Vec<crate::btree::BTreeEntry>>> {
    // Extract a simple predicate: column op literal
    let pred = match extract_index_predicate(where_clause) {
        Some(p) => p,
        None => return Ok(None),
    };

    let (col_name, op, search_val) = pred;

    // Find an index on this table that has this column as its first (or only) column
    let indexes = catalog.get_indexes_for_table(table_name);
    let matching_index = indexes.iter().find(|idx| {
        !idx.columns.is_empty() && idx.columns[0].eq_ignore_ascii_case(&col_name)
    });

    let index_info = match matching_index {
        Some(idx) => (*idx).clone(),
        None => return Ok(None),
    };

    // Only use index for single-column indexes to keep it simple
    if index_info.columns.len() != 1 {
        return Ok(None);
    }

    // Verify the column exists in the table
    let _col_idx = match table.find_column_index(&col_name) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    let index_tree = BTree::open(index_info.root_page);
    let data_tree = BTree::open(table.root_page);

    // Build the search prefix from the predicate value (without rowid)
    let search_prefix = build_index_prefix(&[search_val.clone()]);

    // For prefix-based scanning, we need a key that is strictly greater than
    // any key starting with the prefix. We compute this by incrementing the
    // last byte of the prefix (with carry).
    let end_prefix = compute_successor_prefix(&search_prefix);

    // Get rowids from the index based on the operator.
    // Index keys are [column_value_serialized][rowid_bytes].
    // We compare only the prefix (column value) portion for semantic matching,
    // but since we still apply the WHERE clause for correctness, using a
    // scan_from/scan_range that returns a superset is fine.
    let index_entries: Vec<Vec<u8>> = match op {
        BinaryOp::Eq => {
            // Scan all index entries whose key starts with search_prefix.
            // These are in range [search_prefix, end_prefix).
            let entries = if let Some(ref end) = end_prefix {
                index_tree.scan_range(pool, &search_prefix, end)?
            } else {
                index_tree.scan_from(pool, &search_prefix)?
            };
            entries.into_iter().map(|e| e.value).collect()
        }
        BinaryOp::Gt | BinaryOp::GtEq => {
            // For >= val: scan from search_prefix onward
            // For > val: scan from end_prefix onward (skip all entries with this exact value)
            if matches!(op, BinaryOp::GtEq) {
                let entries = index_tree.scan_from(pool, &search_prefix)?;
                entries.into_iter().map(|e| e.value).collect()
            } else {
                // > val: skip everything with this prefix
                if let Some(ref end) = end_prefix {
                    let entries = index_tree.scan_from(pool, end)?;
                    entries.into_iter().map(|e| e.value).collect()
                } else {
                    // No successor means this was the maximum possible prefix
                    vec![]
                }
            }
        }
        BinaryOp::Lt | BinaryOp::LtEq => {
            // For < val: scan from beginning up to search_prefix (exclusive)
            // For <= val: scan from beginning up to end_prefix (exclusive)
            if matches!(op, BinaryOp::Lt) {
                let entries = index_tree.scan_all(pool)?;
                entries.into_iter()
                    .take_while(|e| e.key.as_slice() < search_prefix.as_slice())
                    .map(|e| e.value)
                    .collect()
            } else {
                // <= val: include everything with this prefix
                if let Some(ref end) = end_prefix {
                    let entries = index_tree.scan_all(pool)?;
                    entries.into_iter()
                        .take_while(|e| e.key.as_slice() < end.as_slice())
                        .map(|e| e.value)
                        .collect()
                } else {
                    // No successor means include everything
                    let entries = index_tree.scan_all(pool)?;
                    entries.into_iter().map(|e| e.value).collect()
                }
            }
        }
        _ => return Ok(None),
    };

    // Look up rows by rowid from the data table
    let mut result = Vec::with_capacity(index_entries.len());
    for rowid_bytes in &index_entries {
        if let Some(row_data) = data_tree.search(pool, rowid_bytes)? {
            result.push(crate::btree::BTreeEntry {
                key: rowid_bytes.clone(),
                value: row_data,
            });
        }
    }

    Ok(Some(result))
}

/// Compute the successor of a byte-string prefix for range scanning.
/// Returns a byte sequence that is lexicographically just past any key
/// starting with `prefix`. Returns None if the prefix is all 0xFF bytes
/// (no successor exists in the byte-string space).
fn compute_successor_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut result = prefix.to_vec();
    // Increment from the last byte, carrying over.
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            return Some(result);
        }
        result.pop(); // drop the 0xFF byte (carry)
    }
    None // all bytes were 0xFF
}

// ---- UPDATE ----

fn execute_update(
    upd: &UpdateStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    let table = catalog.get_table(&upd.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let entries = tree.scan_all(pool)?;
    let mut updated = 0;

    for entry in &entries {
        let mut row_values = deserialize_row(&entry.value, table.columns.len())?;

        // Check WHERE
        if let Some(ref where_clause) = upd.where_clause {
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if !result.to_bool() {
                continue;
            }
        }

        // Record undo before mutating
        txn_mgr.record_undo(UndoEntry::Update {
            table: upd.table.clone(),
            root_page: tree.root_page(),
            key: entry.key.clone(),
            old_value: entry.value.clone(),
        });

        // Apply assignments
        for (col_name, expr) in &upd.assignments {
            let col_idx = table.find_column_index(col_name).ok_or_else(|| {
                HorizonError::ColumnNotFound(format!("{}.{}", upd.table, col_name))
            })?;
            let new_val = eval_expr(expr, &row_values, &table.columns, &table)?;
            let affinity = table.columns[col_idx].affinity;
            row_values[col_idx] = new_val.apply_affinity(affinity);
        }

        // Write back
        let row_data = serialize_row(&row_values);
        tree.insert(pool, &entry.key, &row_data)?;
        updated += 1;
    }

    // Update root page if changed
    if tree.root_page() != table.root_page {
        let mut updated_table = table.clone();
        updated_table.root_page = tree.root_page();
        catalog.update_table_meta(pool, &upd.table, &updated_table)?;
    }

    Ok(updated)
}

// ---- DELETE ----

fn execute_delete(
    del: &DeleteStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    let table = catalog.get_table(&del.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let entries = tree.scan_all(pool)?;
    let mut to_delete: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for entry in &entries {
        if let Some(ref where_clause) = del.where_clause {
            let row_values = deserialize_row(&entry.value, table.columns.len())?;
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if result.to_bool() {
                to_delete.push((entry.key.clone(), entry.value.clone()));
            }
        } else {
            to_delete.push((entry.key.clone(), entry.value.clone()));
        }
    }

    let deleted = to_delete.len();
    for (key, old_value) in &to_delete {
        // Record undo before deleting
        txn_mgr.record_undo(UndoEntry::Delete {
            table: del.table.clone(),
            root_page: tree.root_page(),
            key: key.clone(),
            old_value: old_value.clone(),
        });
        tree.delete(pool, key)?;
    }

    // Update root page if changed
    if tree.root_page() != table.root_page {
        let mut updated_table = table.clone();
        updated_table.root_page = tree.root_page();
        catalog.update_table_meta(pool, &del.table, &updated_table)?;
    }

    Ok(deleted)
}

// ---- CREATE INDEX ----

fn execute_create_index(
    ci: &CreateIndexStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    if ci.if_not_exists {
        if catalog.get_index(&ci.name).is_ok() {
            return Ok(0);
        }
    }

    // Verify the table exists and get its metadata
    let table = catalog.get_table(&ci.table)?.clone();

    // Resolve the column names from the index definition
    let index_columns: Vec<String> = ci.columns.iter().map(|c| {
        if let Expr::Column { name, .. } = &c.expr {
            name.clone()
        } else {
            format!("{:?}", c.expr)
        }
    }).collect();

    // Verify that all index columns exist in the table
    let col_indices: Vec<usize> = index_columns.iter().map(|col_name| {
        table.find_column_index(col_name).ok_or_else(|| {
            HorizonError::ColumnNotFound(format!("{}.{}", ci.table, col_name))
        })
    }).collect::<Result<Vec<_>>>()?;

    // Create a B+Tree for the index
    let mut index_tree = BTree::create(pool)?;

    // Scan existing table data and populate the index
    let data_tree = BTree::open(table.root_page);
    let entries = data_tree.scan_all(pool)?;

    for entry in &entries {
        let row_values = deserialize_row(&entry.value, table.columns.len())?;

        // Build the index key from the indexed column values + rowid
        let index_key = build_index_key(&row_values, &col_indices, &entry.key);

        // The index value is the rowid (the table B+Tree key)
        index_tree.insert(pool, &index_key, &entry.key)?;
    }

    let index_info = crate::catalog::IndexInfo {
        name: ci.name.clone(),
        table_name: ci.table.clone(),
        columns: index_columns,
        unique: ci.unique,
        root_page: index_tree.root_page(),
    };

    catalog.create_index(pool, index_info)?;
    Ok(0)
}

// ---- DROP INDEX ----

fn execute_drop_index(
    di: &DropIndexStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    if di.if_exists {
        if catalog.get_index(&di.name).is_err() {
            return Ok(0);
        }
    }
    catalog.drop_index(pool, &di.name)?;
    Ok(0)
}

// ---- ALTER TABLE ----

fn execute_alter_table(alter: &AlterTableStatement, pool: &mut BufferPool, catalog: &mut Catalog) -> Result<usize> {
    match &alter.action {
        AlterTableAction::AddColumn(col_def) => {
            let type_name = col_def.type_name.clone().unwrap_or_default();
            let affinity = determine_affinity(&type_name);
            let table = catalog.get_table(&alter.table)?;
            let position = table.columns.len();
            let col_info = ColumnInfo {
                name: col_def.name.clone(), type_name, affinity,
                primary_key: false, autoincrement: false,
                not_null: col_def.not_null, unique: col_def.unique,
                default_value: col_def.default.as_ref().map(|e| eval_const_expr(e)),
                position,
            };
            catalog.add_column(pool, &alter.table, col_info)?;
            Ok(0)
        }
        AlterTableAction::RenameTable(new_name) => { catalog.rename_table(pool, &alter.table, new_name)?; Ok(0) }
        AlterTableAction::RenameColumn { old_name, new_name } => { catalog.rename_column(pool, &alter.table, old_name, new_name)?; Ok(0) }
        AlterTableAction::DropColumn(col_name) => { catalog.drop_column(pool, &alter.table, col_name)?; Ok(0) }
    }
}

// ---- PRAGMA ----

fn execute_pragma(pragma: &PragmaStatement, pool: &mut BufferPool, catalog: &mut Catalog) -> Result<QueryResult> {
    let pragma_name = pragma.name.to_lowercase();
    match pragma_name.as_str() {
        "table_info" => {
            let table_name = pragma_extract_table_name(pragma)?;
            let table = catalog.get_table(&table_name)?;
            let columns = Arc::new(vec!["cid".into(), "name".into(), "type".into(), "notnull".into(), "dflt_value".into(), "pk".into()]);
            let mut rows = Vec::new();
            for (i, col) in table.columns.iter().enumerate() {
                let dflt = col.default_value.clone().unwrap_or(Value::Null);
                rows.push(Row { columns: columns.clone(), values: vec![
                    Value::Integer(i as i64), Value::Text(col.name.clone()), Value::Text(col.type_name.clone()),
                    Value::Integer(if col.not_null { 1 } else { 0 }), dflt, Value::Integer(if col.primary_key { 1 } else { 0 }),
                ]});
            }
            Ok(QueryResult { columns, rows })
        }
        "index_list" => {
            let table_name = pragma_extract_table_name(pragma)?;
            let _ = catalog.get_table(&table_name)?;
            let indexes = catalog.get_indexes_for_table(&table_name);
            let columns = Arc::new(vec!["seq".into(), "name".into(), "unique".into(), "origin".into(), "partial".into()]);
            let mut rows = Vec::new();
            for (i, idx) in indexes.iter().enumerate() {
                rows.push(Row { columns: columns.clone(), values: vec![
                    Value::Integer(i as i64), Value::Text(idx.name.clone()),
                    Value::Integer(if idx.unique { 1 } else { 0 }), Value::Text("c".into()), Value::Integer(0),
                ]});
            }
            Ok(QueryResult { columns, rows })
        }
        "database_list" => {
            let columns = Arc::new(vec!["seq".into(), "name".into(), "file".into()]);
            let rows = vec![Row { columns: columns.clone(), values: vec![Value::Integer(0), Value::Text("main".into()), Value::Text(String::new())] }];
            Ok(QueryResult { columns, rows })
        }
        "page_size" => {
            let columns = Arc::new(vec!["page_size".into()]);
            Ok(QueryResult { columns: columns.clone(), rows: vec![Row { columns, values: vec![Value::Integer(4096)] }] })
        }
        "page_count" => {
            let count = pool.pager().page_count();
            let columns = Arc::new(vec!["page_count".into()]);
            Ok(QueryResult { columns: columns.clone(), rows: vec![Row { columns, values: vec![Value::Integer(count as i64)] }] })
        }
        _ => {
            let columns = Arc::new(vec![pragma.name.clone()]);
            Ok(QueryResult { columns, rows: vec![] })
        }
    }
}

fn pragma_extract_table_name(pragma: &PragmaStatement) -> Result<String> {
    match &pragma.value {
        Some(Expr::Column { name, .. }) => Ok(name.clone()),
        Some(Expr::Literal(LiteralValue::String(s))) => Ok(s.clone()),
        _ => Err(HorizonError::InvalidSql("PRAGMA requires a table name argument".into())),
    }
}

// ---- EXPLAIN ----

fn execute_explain(inner_stmt: &Statement, catalog: &Catalog) -> Result<QueryResult> {
    let plan = plan_statement(inner_stmt, catalog)?;
    let detail = format_plan(&plan, 0);
    let columns = Arc::new(vec!["detail".to_string()]);
    let rows = detail.lines().map(|line| Row { columns: columns.clone(), values: vec![Value::Text(line.to_string())] }).collect();
    Ok(QueryResult { columns, rows })
}

fn format_plan(plan: &LogicalPlan, indent: usize) -> String {
    let pfx = "  ".repeat(indent);
    match plan {
        LogicalPlan::SeqScan { table, alias } => {
            let a = alias.as_deref().map(|a| format!(" AS {}", a)).unwrap_or_default();
            format!("{}SCAN TABLE {}{}", pfx, table, a)
        }
        LogicalPlan::Filter { input, predicate } => format!("{}FILTER {:?}\n{}", pfx, predicate, format_plan(input, indent + 1)),
        LogicalPlan::Project { input, columns } => {
            let cs: Vec<String> = columns.iter().map(|c| match c {
                SelectColumn::AllColumns => "*".into(),
                SelectColumn::TableAllColumns(t) => format!("{}.*", t),
                SelectColumn::Expr { expr, alias } => { let b = format!("{:?}", expr); alias.as_ref().map(|a| format!("{} AS {}", b, a)).unwrap_or(b) }
            }).collect();
            format!("{}PROJECT {}\n{}", pfx, cs.join(", "), format_plan(input, indent + 1))
        }
        LogicalPlan::Sort { input, order_by } => {
            let ps: Vec<String> = order_by.iter().map(|o| format!("{:?} {}", o.expr, if o.desc { "DESC" } else { "ASC" })).collect();
            format!("{}SORT {}\n{}", pfx, ps.join(", "), format_plan(input, indent + 1))
        }
        LogicalPlan::Limit { input, limit, offset } => {
            let o = offset.as_ref().map(|e| format!(" OFFSET {:?}", e)).unwrap_or_default();
            format!("{}LIMIT {:?}{}\n{}", pfx, limit, o, format_plan(input, indent + 1))
        }
        LogicalPlan::Aggregate { input, group_by, having } => {
            let g = if group_by.is_empty() { String::new() } else { format!(" GROUP BY {:?}", group_by) };
            let h = having.as_ref().map(|e| format!(" HAVING {:?}", e)).unwrap_or_default();
            format!("{}AGGREGATE{}{}\n{}", pfx, g, h, format_plan(input, indent + 1))
        }
        LogicalPlan::Join { left, right, join_type, on } => {
            let jt = match join_type { JoinType::Inner => "INNER JOIN", JoinType::Left => "LEFT JOIN", JoinType::Right => "RIGHT JOIN", JoinType::Cross => "CROSS JOIN" };
            let o = on.as_ref().map(|e| format!(" ON {:?}", e)).unwrap_or_default();
            format!("{}{}{}\n{}\n{}", pfx, jt, o, format_plan(left, indent + 1), format_plan(right, indent + 1))
        }
        LogicalPlan::Distinct { input } => format!("{}DISTINCT\n{}", pfx, format_plan(input, indent + 1)),
        LogicalPlan::Insert { table, .. } => format!("{}INSERT INTO {}", pfx, table),
        LogicalPlan::Update { table, .. } => format!("{}UPDATE {}", pfx, table),
        LogicalPlan::Delete { table, .. } => format!("{}DELETE FROM {}", pfx, table),
        LogicalPlan::CreateTable(ct) => format!("{}CREATE TABLE {}", pfx, ct.name),
        LogicalPlan::DropTable(dt) => format!("{}DROP TABLE {}", pfx, dt.name),
        LogicalPlan::CreateIndex(ci) => format!("{}CREATE INDEX {} ON {}", pfx, ci.name, ci.table),
        LogicalPlan::DropIndex(di) => format!("{}DROP INDEX {}", pfx, di.name),
        LogicalPlan::Begin => format!("{}BEGIN", pfx),
        LogicalPlan::Commit => format!("{}COMMIT", pfx),
        LogicalPlan::Rollback => format!("{}ROLLBACK", pfx),
        LogicalPlan::Empty => format!("{}EMPTY", pfx),
    }
}

// ---- ROLLBACK ----

/// Execute a ROLLBACK by replaying the undo log in reverse order.
fn execute_rollback(
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    let undo_entries = txn_mgr.rollback_user_txn()?;

    for entry in undo_entries {
        match entry {
            UndoEntry::Insert { table, root_page: _, key } => {
                // Undo an insert by deleting the key
                let current_table = catalog.get_table(&table)?.clone();
                let mut tree = BTree::open(current_table.root_page);
                tree.delete(pool, &key)?;
                // Update root page if changed
                if tree.root_page() != current_table.root_page {
                    let mut updated = current_table.clone();
                    updated.root_page = tree.root_page();
                    catalog.update_table_meta(pool, &table, &updated)?;
                }
            }
            UndoEntry::Delete { table, root_page: _, key, old_value } => {
                // Undo a delete by re-inserting the old value
                let current_table = catalog.get_table(&table)?.clone();
                let mut tree = BTree::open(current_table.root_page);
                tree.insert(pool, &key, &old_value)?;
                // Update root page if changed
                if tree.root_page() != current_table.root_page {
                    let mut updated = current_table.clone();
                    updated.root_page = tree.root_page();
                    catalog.update_table_meta(pool, &table, &updated)?;
                }
            }
            UndoEntry::Update { table, root_page: _, key, old_value } => {
                // Undo an update by restoring the old value
                let current_table = catalog.get_table(&table)?.clone();
                let mut tree = BTree::open(current_table.root_page);
                tree.insert(pool, &key, &old_value)?;
                // Update root page if changed
                if tree.root_page() != current_table.root_page {
                    let mut updated = current_table.clone();
                    updated.root_page = tree.root_page();
                    catalog.update_table_meta(pool, &table, &updated)?;
                }
            }
        }
    }

    Ok(0)
}

// ---- Helper Functions ----

/// Serialize a row of values into bytes.
pub fn serialize_row(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    // Write number of columns
    buf.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for val in values {
        let serialized = val.serialize();
        buf.extend_from_slice(&serialized);
    }
    buf
}

/// Deserialize a row of values from bytes.
///
/// Handles schema evolution from ALTER TABLE:
/// - If the row has fewer columns than expected (ADD COLUMN), missing values are NULL-padded.
/// - If the row has more columns than expected (DROP COLUMN), extra values are truncated.
pub fn deserialize_row(data: &[u8], expected_cols: usize) -> Result<Vec<Value>> {
    if data.len() < 2 {
        return Err(HorizonError::CorruptDatabase("row data too short".into()));
    }
    let col_count = u16::from_be_bytes(data[0..2].try_into().unwrap()) as usize;

    let mut offset = 2;
    let mut values = Vec::with_capacity(expected_cols);
    for _ in 0..col_count {
        let (val, consumed) = Value::deserialize(&data[offset..])?;
        values.push(val);
        offset += consumed;
    }

    // Truncate extra columns from rows written before DROP COLUMN
    values.truncate(expected_cols);

    // NULL-pad any columns added after this row was written (lazy ALTER TABLE ADD COLUMN)
    while values.len() < expected_cols {
        values.push(Value::Null);
    }

    Ok(values)
}

/// Build a composite index key by serializing the indexed column values.
/// The key is: [serialized_column_values][rowid_bytes]
/// The rowid suffix ensures uniqueness in the B+Tree even when column values
/// are duplicated across rows, since the B+Tree uses upsert semantics.
fn build_index_key(row_values: &[Value], col_indices: &[usize], rowid: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    for &idx in col_indices {
        let val = &row_values[idx];
        key.extend(val.serialize());
    }
    key.extend_from_slice(rowid);
    key
}

/// Build an index key prefix from column values only (without rowid).
/// Used for searching: we scan from this prefix to find all matching entries.
fn build_index_prefix(values: &[Value]) -> Vec<u8> {
    let mut key = Vec::new();
    for val in values {
        key.extend(val.serialize());
    }
    key
}

/// Attempt to extract an index-scannable predicate from a WHERE clause.
/// Returns Some((column_name, op, literal_value)) for simple predicates like:
///   column = literal, column > literal, column < literal, etc.
/// Returns None for complex predicates that cannot use an index.
fn extract_index_predicate(expr: &Expr) -> Option<(String, &BinaryOp, Value)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq |
                BinaryOp::Gt | BinaryOp::GtEq => {}
                _ => return None,
            }

            // Pattern: column op literal
            if let Expr::Column { name, .. } = left.as_ref() {
                if is_const_expr(right) {
                    let val = eval_const_expr(right);
                    return Some((name.clone(), op, val));
                }
            }

            // Pattern: literal op column (reverse the comparison)
            if let Expr::Column { name, .. } = right.as_ref() {
                if is_const_expr(left) {
                    let val = eval_const_expr(left);
                    // Reverse the operator: literal < column => column > literal
                    let reversed_op = match op {
                        BinaryOp::Lt => &BinaryOp::Gt,
                        BinaryOp::LtEq => &BinaryOp::GtEq,
                        BinaryOp::Gt => &BinaryOp::Lt,
                        BinaryOp::GtEq => &BinaryOp::LtEq,
                        BinaryOp::Eq => &BinaryOp::Eq,
                        _ => return None,
                    };
                    return Some((name.clone(), reversed_op, val));
                }
            }

            None
        }
        _ => None,
    }
}

/// Check if an expression is a constant (literal or simple arithmetic on literals).
fn is_const_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::UnaryOp { expr, .. } => is_const_expr(expr),
        Expr::BinaryOp { left, right, .. } => is_const_expr(left) && is_const_expr(right),
        _ => false,
    }
}

/// Evaluate a constant expression (no row context needed).
fn eval_const_expr(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        Expr::UnaryOp { op: UnaryOp::Neg, expr } => {
            match eval_const_expr(expr) {
                Value::Integer(i) => Value::Integer(-i),
                Value::Real(r) => Value::Real(-r),
                other => other,
            }
        }
        Expr::UnaryOp { op: UnaryOp::Not, expr } => {
            let val = eval_const_expr(expr);
            if val.is_null() { Value::Null } else {
                Value::Integer(if val.to_bool() { 0 } else { 1 })
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_const_expr(left);
            let r = eval_const_expr(right);
            eval_binary_op(&l, op, &r)
        }
        Expr::Cast { expr: inner, type_name } => {
            let val = eval_const_expr(inner);
            let affinity = determine_affinity(type_name);
            val.apply_affinity(affinity)
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_const_expr(operand_expr);
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_const_expr(when_expr);
                    if op_val == when_val {
                        return eval_const_expr(then_expr);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_const_expr(when_expr);
                    if when_val.to_bool() {
                        return eval_const_expr(then_expr);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_const_expr(else_expr)
            } else {
                Value::Null
            }
        }
        Expr::IsNull { expr: inner, negated } => {
            let val = eval_const_expr(inner);
            let is_null = val.is_null();
            Value::Integer(if is_null != *negated { 1 } else { 0 })
        }
        Expr::Function { name, args, .. } => {
            // Evaluate functions in constant context using the dynamic evaluator with empty row
            let empty_row: Vec<Value> = vec![];
            let empty_cols: Vec<String> = vec![];
            eval_function_dynamic(name, args, &empty_row, &empty_cols).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Execute a scalar subquery, returning the single value from the first row/column.
/// If the subquery returns no rows, returns NULL.
fn execute_scalar_subquery(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Value> {
    let result = execute_select(select, pool, catalog)?;
    if result.rows.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(result.rows[0].values.first().cloned().unwrap_or(Value::Null))
    }
}

/// Execute an EXISTS subquery, returning 1 (true) if the subquery returns any rows, 0 otherwise.
fn execute_exists_subquery(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Value> {
    let result = execute_select(select, pool, catalog)?;
    Ok(Value::Integer(if result.rows.is_empty() { 0 } else { 1 }))
}

/// Evaluate an expression in the context of a row.
fn eval_expr(
    expr: &Expr,
    row: &[Value],
    columns: &[ColumnInfo],
    table: &TableInfo,
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Column { table: _, name } => {
            // Look up column by name
            if let Some(idx) = table.find_column_index(name) {
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                // Check if it's "rowid"
                if name.eq_ignore_ascii_case("rowid") {
                    // The rowid is implicit; for now return NULL
                    Ok(Value::Null)
                } else {
                    Err(HorizonError::ColumnNotFound(name.clone()))
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row, columns, table)?;
            let r = eval_expr(right, row, columns, table)?;
            Ok(eval_binary_op(&l, op, &r))
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr(inner, row, columns, table)?;
            Ok(eval_unary_op(op, &val))
        }
        Expr::IsNull { expr: inner, negated } => {
            let val = eval_expr(inner, row, columns, table)?;
            let is_null = val.is_null();
            Ok(Value::Integer(if is_null != *negated { 1 } else { 0 }))
        }
        Expr::Between { expr: inner, low, high, negated } => {
            let val = eval_expr(inner, row, columns, table)?;
            let lo = eval_expr(low, row, columns, table)?;
            let hi = eval_expr(high, row, columns, table)?;
            let in_range = val >= lo && val <= hi;
            Ok(Value::Integer(if in_range != *negated { 1 } else { 0 }))
        }
        Expr::InList { expr: inner, list, negated } => {
            let val = eval_expr(inner, row, columns, table)?;
            let mut found = false;
            for item in list {
                let item_val = eval_expr(item, row, columns, table)?;
                if val == item_val {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if found != *negated { 1 } else { 0 }))
        }
        Expr::Like { expr: inner, pattern, negated } => {
            let val = eval_expr(inner, row, columns, table)?;
            let pat = eval_expr(pattern, row, columns, table)?;
            let matches = match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => sql_like_match(s, p),
                _ => false,
            };
            Ok(Value::Integer(if matches != *negated { 1 } else { 0 }))
        }
        Expr::Function { name, args, distinct: _ } => {
            eval_function(name, args, row, columns, table)
        }
        Expr::Cast { expr: inner, type_name } => {
            let val = eval_expr(inner, row, columns, table)?;
            let affinity = determine_affinity(type_name);
            Ok(val.apply_affinity(affinity))
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_expr(operand_expr, row, columns, table)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr(when_expr, row, columns, table)?;
                    if op_val == when_val {
                        return eval_expr(then_expr, row, columns, table);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr(when_expr, row, columns, table)?;
                    if when_val.to_bool() {
                        return eval_expr(then_expr, row, columns, table);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_expr(else_expr, row, columns, table)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Placeholder(_) => Ok(Value::Null),
        Expr::Subquery(_) | Expr::Exists(_) => {
            // Subqueries require pool/catalog context; use eval_expr_with_ctx instead
            Err(HorizonError::NotImplemented("subqueries in expressions (use eval_expr_with_ctx)".into()))
        }
    }
}

/// Evaluate an expression with full subquery support (has pool/catalog context).
fn eval_expr_with_ctx(
    expr: &Expr,
    row: &[Value],
    columns: &[ColumnInfo],
    table: &TableInfo,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Value> {
    match expr {
        Expr::Subquery(select) => {
            execute_scalar_subquery(select, pool, catalog)
        }
        Expr::Exists(select) => {
            execute_exists_subquery(select, pool, catalog)
        }
        // For all non-subquery expressions, delegate to eval_expr.
        // But we need to recursively handle subqueries in nested expressions too.
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr_with_ctx(left, row, columns, table, pool, catalog)?;
            let r = eval_expr_with_ctx(right, row, columns, table, pool, catalog)?;
            Ok(eval_binary_op(&l, op, &r))
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr_with_ctx(inner, row, columns, table, pool, catalog)?;
            Ok(eval_unary_op(op, &val))
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_expr_with_ctx(operand_expr, row, columns, table, pool, catalog)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_with_ctx(when_expr, row, columns, table, pool, catalog)?;
                    if op_val == when_val {
                        return eval_expr_with_ctx(then_expr, row, columns, table, pool, catalog);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_with_ctx(when_expr, row, columns, table, pool, catalog)?;
                    if when_val.to_bool() {
                        return eval_expr_with_ctx(then_expr, row, columns, table, pool, catalog);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_expr_with_ctx(else_expr, row, columns, table, pool, catalog)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::IsNull { expr: inner, negated } => {
            let val = eval_expr_with_ctx(inner, row, columns, table, pool, catalog)?;
            let is_null = val.is_null();
            Ok(Value::Integer(if is_null != *negated { 1 } else { 0 }))
        }
        // For expressions that don't contain subqueries (literals, columns, etc.),
        // fall through to the regular eval_expr
        _ => eval_expr(expr, row, columns, table),
    }
}

/// Evaluate a dynamic expression with full subquery support (has pool/catalog context).
#[allow(dead_code)]
fn eval_expr_dynamic_with_ctx(
    expr: &Expr,
    row: &[Value],
    col_names: &[String],
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Value> {
    match expr {
        Expr::Subquery(select) => {
            execute_scalar_subquery(select, pool, catalog)
        }
        Expr::Exists(select) => {
            execute_exists_subquery(select, pool, catalog)
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr_dynamic_with_ctx(left, row, col_names, pool, catalog)?;
            let r = eval_expr_dynamic_with_ctx(right, row, col_names, pool, catalog)?;
            Ok(eval_binary_op(&l, op, &r))
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr_dynamic_with_ctx(inner, row, col_names, pool, catalog)?;
            Ok(eval_unary_op(op, &val))
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(ref operand_expr) = operand {
                let op_val = eval_expr_dynamic_with_ctx(operand_expr, row, col_names, pool, catalog)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_dynamic_with_ctx(when_expr, row, col_names, pool, catalog)?;
                    if op_val == when_val {
                        return eval_expr_dynamic_with_ctx(then_expr, row, col_names, pool, catalog);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr_dynamic_with_ctx(when_expr, row, col_names, pool, catalog)?;
                    if when_val.to_bool() {
                        return eval_expr_dynamic_with_ctx(then_expr, row, col_names, pool, catalog);
                    }
                }
            }
            if let Some(ref else_expr) = else_clause {
                eval_expr_dynamic_with_ctx(else_expr, row, col_names, pool, catalog)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::IsNull { expr: inner, negated } => {
            let val = eval_expr_dynamic_with_ctx(inner, row, col_names, pool, catalog)?;
            let is_null = val.is_null();
            Ok(Value::Integer(if is_null != *negated { 1 } else { 0 }))
        }
        _ => eval_expr_dynamic(expr, row, col_names),
    }
}

fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Integer(i) => Value::Integer(*i),
        LiteralValue::Real(r) => Value::Real(*r),
        LiteralValue::String(s) => Value::Text(s.clone()),
        LiteralValue::Blob(b) => Value::Blob(b.clone()),
        LiteralValue::True => Value::Integer(1),
        LiteralValue::False => Value::Integer(0),
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Value {
    // Handle NULL propagation
    if left.is_null() || right.is_null() {
        match op {
            BinaryOp::And => {
                // FALSE AND NULL = FALSE
                if let Value::Integer(0) = left { return Value::Integer(0); }
                if let Value::Integer(0) = right { return Value::Integer(0); }
                return Value::Null;
            }
            BinaryOp::Or => {
                // TRUE OR NULL = TRUE
                if left.to_bool() { return Value::Integer(1); }
                if right.to_bool() { return Value::Integer(1); }
                return Value::Null;
            }
            BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::Gt |
            BinaryOp::LtEq | BinaryOp::GtEq => return Value::Null,
            _ => return Value::Null,
        }
    }

    match op {
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            match (left, right) {
                (Value::Integer(_), Value::Integer(0)) |
                (Value::Real(_), Value::Integer(0)) => Value::Null,
                _ => numeric_op(left, right, |a, b| if b != 0 { a / b } else { 0 }, |a, b| a / b),
            }
        }
        BinaryOp::Mod => {
            numeric_op(left, right, |a, b| if b != 0 { a % b } else { 0 }, |a, b| a % b)
        }
        BinaryOp::Eq => Value::Integer(if left == right { 1 } else { 0 }),
        BinaryOp::NotEq => Value::Integer(if left != right { 1 } else { 0 }),
        BinaryOp::Lt => Value::Integer(if left < right { 1 } else { 0 }),
        BinaryOp::Gt => Value::Integer(if left > right { 1 } else { 0 }),
        BinaryOp::LtEq => Value::Integer(if left <= right { 1 } else { 0 }),
        BinaryOp::GtEq => Value::Integer(if left >= right { 1 } else { 0 }),
        BinaryOp::And => {
            Value::Integer(if left.to_bool() && right.to_bool() { 1 } else { 0 })
        }
        BinaryOp::Or => {
            Value::Integer(if left.to_bool() || right.to_bool() { 1 } else { 0 })
        }
        BinaryOp::Concat => {
            let l = match left {
                Value::Text(s) => s.clone(),
                Value::Integer(i) => i.to_string(),
                Value::Real(r) => r.to_string(),
                _ => return Value::Null,
            };
            let r = match right {
                Value::Text(s) => s.clone(),
                Value::Integer(i) => i.to_string(),
                Value::Real(r) => r.to_string(),
                _ => return Value::Null,
            };
            Value::Text(format!("{}{}", l, r))
        }
        BinaryOp::BitAnd => {
            match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Value::Integer(a & b),
                _ => Value::Null,
            }
        }
        BinaryOp::BitOr => {
            match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Value::Integer(a | b),
                _ => Value::Null,
            }
        }
        BinaryOp::ShiftLeft => {
            match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Value::Integer(a << b),
                _ => Value::Null,
            }
        }
        BinaryOp::ShiftRight => {
            match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Value::Integer(a >> b),
                _ => Value::Null,
            }
        }
    }
}

fn eval_unary_op(op: &UnaryOp, val: &Value) -> Value {
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(i) => Value::Integer(-i),
            Value::Real(r) => Value::Real(-r),
            _ => Value::Null,
        },
        UnaryOp::Not => {
            if val.is_null() {
                Value::Null
            } else {
                Value::Integer(if val.to_bool() { 0 } else { 1 })
            }
        }
        UnaryOp::BitNot => match val {
            Value::Integer(i) => Value::Integer(!i),
            _ => Value::Null,
        },
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Value {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Value::Integer(int_op(*a, *b)),
        (Value::Real(a), Value::Real(b)) => Value::Real(float_op(*a, *b)),
        (Value::Integer(a), Value::Real(b)) => Value::Real(float_op(*a as f64, *b)),
        (Value::Real(a), Value::Integer(b)) => Value::Real(float_op(*a, *b as f64)),
        _ => Value::Null,
    }
}

fn eval_function(
    name: &str,
    args: &[Expr],
    row: &[Value],
    columns: &[ColumnInfo],
    table: &TableInfo,
) -> Result<Value> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "ABS" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::Real(r) => Ok(Value::Real(r.abs())),
                _ => Ok(Value::Null),
            }
        }
        "LENGTH" | "LEN" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Integer(format!("{}", val).len() as i64)),
            }
        }
        "UPPER" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                _ => Ok(val),
            }
        }
        "LOWER" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                _ => Ok(val),
            }
        }
        "TYPEOF" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            let type_str = match val {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
            };
            Ok(Value::Text(type_str.to_string()))
        }
        "COALESCE" => {
            for arg in args {
                let val = eval_expr(arg, row, columns, table)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() >= 2 {
                let val = eval_expr(&args[0], row, columns, table)?;
                if val.is_null() {
                    eval_expr(&args[1], row, columns, table)
                } else {
                    Ok(val)
                }
            } else {
                Ok(Value::Null)
            }
        }
        "NULLIF" => {
            if args.len() >= 2 {
                let a = eval_expr(&args[0], row, columns, table)?;
                let b = eval_expr(&args[1], row, columns, table)?;
                if a == b { Ok(Value::Null) } else { Ok(a) }
            } else {
                Ok(Value::Null)
            }
        }
        "MAX" if args.len() >= 2 => {
            // Scalar MAX: returns the maximum of its arguments
            let mut max_val = eval_expr(&args[0], row, columns, table)?;
            for arg in &args[1..] {
                let v = eval_expr(arg, row, columns, table)?;
                if v.is_null() { continue; }
                if max_val.is_null() || v > max_val {
                    max_val = v;
                }
            }
            Ok(max_val)
        }
        "MIN" if args.len() >= 2 => {
            // Scalar MIN: returns the minimum of its arguments
            let mut min_val = eval_expr(&args[0], row, columns, table)?;
            for arg in &args[1..] {
                let v = eval_expr(arg, row, columns, table)?;
                if v.is_null() { continue; }
                if min_val.is_null() || v < min_val {
                    min_val = v;
                }
            }
            Ok(min_val)
        }
        "MAX" | "MIN" | "COUNT" | "SUM" | "AVG" | "TOTAL" | "GROUP_CONCAT" => {
            // These are aggregate functions -- evaluated per-row they just return the value
            // Full aggregate support handled at a higher level
            if !args.is_empty() {
                eval_expr(&args[0], row, columns, table)
            } else {
                Ok(Value::Null)
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }
            let val = eval_expr(&args[0], row, columns, table)?;
            let start = eval_expr(&args[1], row, columns, table)?;
            let len = if args.len() > 2 {
                eval_expr(&args[2], row, columns, table)?.as_integer()
            } else {
                None
            };

            match (val, start.as_integer()) {
                (Value::Text(s), Some(start_pos)) => {
                    let start_idx = if start_pos > 0 {
                        (start_pos - 1) as usize
                    } else {
                        0
                    };
                    let substr_result = if let Some(l) = len {
                        s.chars().skip(start_idx).take(l as usize).collect::<String>()
                    } else {
                        s.chars().skip(start_idx).collect::<String>()
                    };
                    Ok(Value::Text(substr_result))
                }
                _ => Ok(Value::Null),
            }
        }
        "TRIM" => {
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                _ => Ok(val),
            }
        }
        "REPLACE" => {
            if args.len() < 3 {
                return Ok(Value::Null);
            }
            let val = eval_expr(&args[0], row, columns, table)?;
            let from = eval_expr(&args[1], row, columns, table)?;
            let to = eval_expr(&args[2], row, columns, table)?;
            match (val, from, to) {
                (Value::Text(s), Value::Text(f), Value::Text(t)) => {
                    Ok(Value::Text(s.replace(&f, &t)))
                }
                _ => Ok(Value::Null),
            }
        }
        "INSTR" => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }
            let val = eval_expr(&args[0], row, columns, table)?;
            let search = eval_expr(&args[1], row, columns, table)?;
            match (val, search) {
                (Value::Text(s), Value::Text(needle)) => {
                    Ok(Value::Integer(s.find(&needle).map(|i| i as i64 + 1).unwrap_or(0)))
                }
                _ => Ok(Value::Integer(0)),
            }
        }
        "LTRIM" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim_start().to_string())),
                _ => Ok(val),
            }
        }
        "RTRIM" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => Ok(Value::Text(s.trim_end().to_string())),
                _ => Ok(val),
            }
        }
        "HEX" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Text(s) => {
                    let hex: String = s.as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Integer(i) => {
                    let hex: String = i.to_string().as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                Value::Null => Ok(Value::Null),
                Value::Real(r) => {
                    let hex: String = r.to_string().as_bytes().iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
            }
        }
        "ROUND" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            let decimals = if args.len() > 1 {
                eval_expr(&args[1], row, columns, table)?.as_integer().unwrap_or(0)
            } else {
                0
            };
            match val {
                Value::Real(r) => {
                    let factor = 10f64.powi(decimals as i32);
                    Ok(Value::Real((r * factor).round() / factor))
                }
                Value::Integer(i) => {
                    if decimals >= 0 {
                        Ok(Value::Real(i as f64))
                    } else {
                        let factor = 10f64.powi((-decimals) as i32);
                        Ok(Value::Real(((i as f64 / factor).round()) * factor))
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "RANDOM" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let val = (now.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)) as i64;
            Ok(Value::Integer(val))
        }
        "ZEROBLOB" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Integer(n) => {
                    let size = n.max(0) as usize;
                    Ok(Value::Blob(vec![0u8; size]))
                }
                _ => Ok(Value::Null),
            }
        }
        "IIF" => {
            if args.len() < 3 { return Ok(Value::Null); }
            let cond = eval_expr(&args[0], row, columns, table)?;
            if cond.to_bool() {
                eval_expr(&args[1], row, columns, table)
            } else {
                eval_expr(&args[2], row, columns, table)
            }
        }
        _ => Err(HorizonError::NotImplemented(format!("function: {}", name))),
    }
}

/// SQL LIKE pattern matching.
fn sql_like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&text_chars, 0, &pattern_chars, 0)
}

fn like_match_inner(text: &[char], ti: usize, pattern: &[char], pi: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }

    match pattern[pi] {
        '%' => {
            // Match zero or more characters
            for i in ti..=text.len() {
                if like_match_inner(text, i, pattern, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // Match exactly one character
            if ti < text.len() {
                like_match_inner(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
        c => {
            if ti < text.len() && text[ti].to_lowercase().eq(c.to_lowercase()) {
                like_match_inner(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
    }
}

fn resolve_column_names(
    select_cols: &[SelectColumn],
    table: &TableInfo,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                for c in &table.columns {
                    names.push(c.name.clone());
                }
            }
            SelectColumn::TableAllColumns(_) => {
                for c in &table.columns {
                    names.push(c.name.clone());
                }
            }
            SelectColumn::Expr { expr, alias } => {
                if let Some(a) = alias {
                    names.push(a.clone());
                } else if let Expr::Column { name, .. } = expr {
                    names.push(name.clone());
                } else {
                    names.push(format!("{:?}", expr));
                }
            }
        }
    }
    Ok(names)
}

#[allow(dead_code)]
fn project_row(
    select_cols: &[SelectColumn],
    row: &[Value],
    table: &TableInfo,
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                values.extend(row.iter().cloned());
            }
            SelectColumn::TableAllColumns(_) => {
                values.extend(row.iter().cloned());
            }
            SelectColumn::Expr { expr, .. } => {
                let val = eval_expr(expr, row, &table.columns, table)?;
                values.push(val);
            }
        }
    }
    Ok(values)
}

/// Project a row with subquery support (has pool/catalog context).
fn project_row_with_ctx(
    select_cols: &[SelectColumn],
    row: &[Value],
    table: &TableInfo,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                values.extend(row.iter().cloned());
            }
            SelectColumn::TableAllColumns(_) => {
                values.extend(row.iter().cloned());
            }
            SelectColumn::Expr { expr, .. } => {
                let val = eval_expr_with_ctx(expr, row, &table.columns, table, pool, catalog)?;
                values.push(val);
            }
        }
    }
    Ok(values)
}

fn sort_rows(
    rows: &mut [Row],
    order_by: &[OrderByItem],
    table: &TableInfo,
) -> Result<()> {
    rows.sort_by(|a, b| {
        for item in order_by {
            let val_a = eval_expr(&item.expr, &a.values, &table.columns, table)
                .unwrap_or(Value::Null);
            let val_b = eval_expr(&item.expr, &b.values, &table.columns, table)
                .unwrap_or(Value::Null);
            let cmp = val_a.cmp(&val_b);
            let cmp = if item.desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_row() {
        let values = vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Null,
            Value::Real(3.14),
        ];
        let data = serialize_row(&values);
        let result = deserialize_row(&data, 4).unwrap();
        assert_eq!(result, values);
    }

    #[test]
    fn test_eval_const_expr() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Integer(2))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
        };
        assert_eq!(eval_const_expr(&expr), Value::Integer(5));
    }

    #[test]
    fn test_sql_like_match() {
        assert!(sql_like_match("hello", "hello"));
        assert!(sql_like_match("hello", "h%"));
        assert!(sql_like_match("hello", "%lo"));
        assert!(sql_like_match("hello", "%ell%"));
        assert!(sql_like_match("hello", "h_llo"));
        assert!(sql_like_match("hello", "%"));
        assert!(!sql_like_match("hello", "world"));
        assert!(!sql_like_match("hello", "h_lo"));
    }
}
