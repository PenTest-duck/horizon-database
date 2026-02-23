//! # Execution Engine
//!
//! Executes SQL statements against the storage layer. This module bridges
//! the SQL parser/planner with the B+Tree storage, catalog, and MVCC layers.

pub mod json;
pub mod rtree;
pub mod fts5;
mod views_triggers;

use std::collections::HashMap;
use std::sync::Arc;
use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, ColumnInfo, TableInfo, ViewInfo, TriggerInfo, TriggerTimingKind, TriggerEventKind};
use crate::error::{HorizonError, Result};
use crate::mvcc::{TransactionManager, UndoEntry};
use crate::planner::{LogicalPlan, plan_statement};
use crate::sql::ast::*;
use crate::sql::parser::Parser;
use crate::types::{DataType, Value, determine_affinity};
use crate::{QueryResult, Row};

/// Stored CTE data: column names and row values.
type CteStore = HashMap<String, (Vec<String>, Vec<Vec<Value>>)>;

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
        Statement::Insert(ins) => {
            if catalog.rtree_exists(&ins.table) {
                return rtree::execute_rtree_insert(ins, pool, catalog);
            }
            execute_insert(ins, pool, catalog, txn_mgr)
        }
        Statement::Update(upd) => execute_update(upd, pool, catalog, txn_mgr),
        Statement::Delete(del) => {
            if catalog.rtree_exists(&del.table) {
                return rtree::execute_rtree_delete(del, pool, catalog);
            }
            execute_delete(del, pool, catalog, txn_mgr)
        }
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
        Statement::CreateView(cv) => views_triggers::execute_create_view(cv, catalog),
        Statement::DropView(dv) => views_triggers::execute_drop_view(dv, catalog),
        Statement::CreateTrigger(ct) => views_triggers::execute_create_trigger(ct, catalog),
        Statement::DropTrigger(dt) => views_triggers::execute_drop_trigger(dt, catalog),
        Statement::Select(_) => {
            Err(HorizonError::Internal("use execute_query for SELECT statements".into()))
        }
        Statement::AttachDatabase(attach) => {
            catalog.attach_database(attach.path.clone(), attach.schema_name.clone())?;
            Ok(0)
        }
        Statement::DetachDatabase(detach) => {
            catalog.detach_database(&detach.schema_name)?;
            Ok(0)
        }
        Statement::Vacuum => execute_vacuum(pool, catalog),
        Statement::ExplainQueryPlan(_) => {
            // EXPLAIN QUERY PLAN returns rows; handled in Database::query()
            Err(HorizonError::Internal("use query() for EXPLAIN QUERY PLAN statements".into()))
        }
        Statement::CreateVirtualTable(cvt) => {
            match cvt.module_name.to_lowercase().as_str() {
                "fts5" => execute_create_fts5_table(cvt),
                "rtree" => rtree::execute_create_virtual_table_rtree(cvt, pool, catalog),
                _ => Err(HorizonError::InvalidSql(format!(
                    "unknown virtual table module: {}",
                    cvt.module_name
                ))),
            }
        }
    }
}

/// Execute a SELECT query and return results.
pub fn execute_query(
    stmt: &Statement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<QueryResult> {
    match stmt {
        Statement::Select(select) => execute_select(select, pool, catalog),
        Statement::Pragma(pragma) => execute_pragma(pragma, pool, catalog),
        Statement::Explain(inner) => execute_explain(inner, catalog),
        Statement::ExplainQueryPlan(inner) => execute_explain_query_plan(inner, catalog),
        Statement::Insert(ins) if ins.returning.is_some() => {
            execute_insert_returning(ins, pool, catalog, txn_mgr)
        }
        Statement::Update(upd) if upd.returning.is_some() => {
            execute_update_returning(upd, pool, catalog, txn_mgr)
        }
        Statement::Delete(del) if del.returning.is_some() => {
            execute_delete_returning(del, pool, catalog, txn_mgr)
        }
        _ => Err(HorizonError::Internal("execute_query requires a SELECT, PRAGMA, EXPLAIN, or RETURNING statement".into())),
    }
}

/// Check whether a statement has a RETURNING clause.
pub fn has_returning(stmt: &Statement) -> bool {
    match stmt {
        Statement::Insert(ins) => ins.returning.is_some(),
        Statement::Update(upd) => upd.returning.is_some(),
        Statement::Delete(del) => del.returning.is_some(),
        _ => false,
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

        let (gen_expr, gen_stored) = if let Some(ref gen) = col_def.generated {
            (Some(gen.expr.clone()), gen.stored)
        } else {
            (None, false)
        };

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
            generated_expr: gen_expr,
            is_stored: gen_stored,
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

// ---- CREATE VIRTUAL TABLE (FTS5) ----

fn execute_create_fts5_table(
    cvt: &CreateVirtualTableStatement,
) -> Result<usize> {
    if cvt.if_not_exists && fts5::fts5_table_exists(&cvt.name) {
        return Ok(0);
    }
    fts5::create_fts5_table(&cvt.name, cvt.module_args.clone())?;
    Ok(0)
}

// ---- DROP TABLE ----

fn execute_drop_table(
    dt: &DropTableStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    // Check if it's an FTS5 virtual table
    if fts5::fts5_table_exists(&dt.name) {
        fts5::fts5_drop_table(&dt.name)?;
        return Ok(0);
    }
    if dt.if_exists && !catalog.table_exists(&dt.name) {
        return Ok(0);
    }
    catalog.drop_table(pool, &dt.name)?;
    Ok(0)
}

// ---- Generated Column Helper ----

/// Fill in virtual generated column values in a row by evaluating their
/// expressions. This must be called after deserializing a row from disk
/// and before the row is used in expressions or projections.
fn fill_virtual_columns(
    row: &mut Vec<Value>,
    table: &TableInfo,
) -> Result<()> {
    for (i, col) in table.columns.iter().enumerate() {
        if let Some(ref gen_expr) = col.generated_expr {
            if !col.is_stored {
                // VIRTUAL column -- evaluate expression on the fly
                let val = eval_expr(gen_expr, row, &table.columns, table)?;
                if i < row.len() {
                    row[i] = val.apply_affinity(col.affinity);
                }
            }
        }
    }
    Ok(())
}

/// Returns true if a table has any virtual generated columns.
fn table_has_virtual_columns(table: &TableInfo) -> bool {
    table.columns.iter().any(|c| c.generated_expr.is_some() && !c.is_stored)
}

// ---- VACUUM ----

/// Execute a VACUUM command.
///
/// This implementation flushes all dirty pages to disk, effectively
/// compacting the in-memory representation. A full page-level
/// compaction would require rewriting the entire database file, which
/// is deferred to a future release.
fn execute_vacuum(
    pool: &mut BufferPool,
    _catalog: &mut Catalog,
) -> Result<usize> {
    pool.flush_all()?;
    Ok(0)
}



// ---- FTS5 SELECT ----

fn execute_fts5_select(
    select: &SelectStatement,
    table_name: &str,
    table_fn_args: Option<&Vec<Expr>>,
    _pool: &mut BufferPool,
    _catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let fts_columns = fts5::fts5_get_columns(table_name)?;

    // Determine the search query
    let query_text: Option<String> = if let Some(args) = table_fn_args {
        // Table function syntax: FROM table('query')
        if let Some(first_arg) = args.first() {
            if let Expr::Literal(LiteralValue::String(s)) = first_arg {
                Some(s.clone())
            } else {
                None
            }
        } else {
            None
        }
    } else if let Some(ref where_clause) = select.where_clause {
        // MATCH syntax: WHERE table MATCH 'query'
        extract_fts5_match_query(where_clause, table_name)
    } else {
        None
    };

    // Build the result set
    let _has_rank = select.columns.iter().any(|c| {
        if let SelectColumn::Expr { expr: Expr::Column { name, .. }, .. } = c {
            name.eq_ignore_ascii_case("rank")
        } else {
            false
        }
    }) || matches!(&select.columns[..], [SelectColumn::AllColumns]);

    // Check for FTS5 auxiliary functions in the columns
    let _has_fts_functions = select.columns.iter().any(|c| {
        if let SelectColumn::Expr { expr: Expr::Function { name, .. }, .. } = c {
            let upper = name.to_uppercase();
            matches!(upper.as_str(), "HIGHLIGHT" | "SNIPPET" | "BM25")
        } else {
            false
        }
    });

    let query_for_fns = query_text.clone().unwrap_or_default();

    let rows_data: Vec<(i64, Vec<Value>, f64)> = if let Some(ref q) = query_text {
        fts5::fts5_query(table_name, q)?
    } else {
        // Full scan
        let all = fts5::fts5_scan_all(table_name)?;
        all.into_iter().map(|(rid, vals)| (rid, vals, 0.0)).collect()
    };

    // Build column names from the select columns
    let mut col_names: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();

    // Determine output column names
    let is_star = select.columns.len() == 1 && matches!(select.columns[0], SelectColumn::AllColumns);
    if is_star {
        col_names = fts_columns.clone();
        col_names.push("rank".to_string());
    } else {
        for sc in &select.columns {
            match sc {
                SelectColumn::AllColumns => {
                    col_names.extend(fts_columns.clone());
                    col_names.push("rank".to_string());
                }
                SelectColumn::TableAllColumns(_) => {
                    col_names.extend(fts_columns.clone());
                }
                SelectColumn::Expr { expr, alias } => {
                    if let Some(a) = alias {
                        col_names.push(a.clone());
                    } else if let Expr::Column { name, .. } = expr {
                        col_names.push(name.clone());
                    } else if let Expr::Function { name, .. } = expr {
                        col_names.push(name.to_lowercase());
                    } else {
                        col_names.push(format!("{:?}", expr));
                    }
                }
            }
        }
    }

    // Build row data
    for (rowid, doc_values, bm25_score) in &rows_data {
        if is_star {
            let mut row = doc_values.clone();
            row.push(Value::Real(*bm25_score));
            rows.push(row);
        } else {
            let mut row = Vec::new();
            for sc in &select.columns {
                match sc {
                    SelectColumn::AllColumns => {
                        row.extend(doc_values.clone());
                        row.push(Value::Real(*bm25_score));
                    }
                    SelectColumn::TableAllColumns(_) => {
                        row.extend(doc_values.clone());
                    }
                    SelectColumn::Expr { expr, .. } => {
                        let val = eval_fts5_expr(
                            expr,
                            table_name,
                            *rowid,
                            doc_values,
                            &fts_columns,
                            *bm25_score,
                            &query_for_fns,
                        )?;
                        row.push(val);
                    }
                }
            }
            rows.push(row);
        }
    }

    // Apply ORDER BY
    if !select.order_by.is_empty() {
        sort_rows_dynamic(&mut rows, &select.order_by, &col_names)?;
    }

    // Apply DISTINCT
    if select.distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new();
        let mut unique = Vec::new();
        for row in rows {
            if !seen.contains(&row) {
                seen.push(row.clone());
                unique.push(row);
            }
        }
        rows = unique;
    }

    // Apply OFFSET
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
        if offset < rows.len() {
            rows = rows.into_iter().skip(offset).collect();
        } else {
            rows.clear();
        }
    }

    // Apply LIMIT
    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_expr(limit_expr).as_integer().unwrap_or(i64::MAX) as usize;
        rows.truncate(limit);
    }

    Ok((col_names, rows))
}

/// Evaluate an expression in the context of an FTS5 row.
fn eval_fts5_expr(
    expr: &Expr,
    table_name: &str,
    rowid: i64,
    doc_values: &[Value],
    fts_columns: &[String],
    bm25_score: f64,
    query: &str,
) -> Result<Value> {
    match expr {
        Expr::Column { name, .. } => {
            if name.eq_ignore_ascii_case("rank") {
                return Ok(Value::Real(bm25_score));
            }
            if name.eq_ignore_ascii_case("rowid") {
                return Ok(Value::Integer(rowid));
            }
            // Look up by column name
            if let Some(idx) = fts_columns.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                Ok(doc_values.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Function { name, args, .. } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "HIGHLIGHT" => {
                    // highlight(table, col_idx, before_tag, after_tag)
                    if args.len() < 4 {
                        return Err(HorizonError::InvalidSql(
                            "highlight() requires 4 arguments".into(),
                        ));
                    }
                    let col_idx = match &args[1] {
                        Expr::Literal(LiteralValue::Integer(n)) => *n as usize,
                        _ => 0,
                    };
                    let before = match &args[2] {
                        Expr::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => "<b>".to_string(),
                    };
                    let after = match &args[3] {
                        Expr::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => "</b>".to_string(),
                    };
                    let result = fts5::fts5_highlight(table_name, rowid, col_idx, &before, &after, query)?;
                    Ok(Value::Text(result))
                }
                "SNIPPET" => {
                    // snippet(table, col_idx, before_tag, after_tag, ellipsis, max_tokens)
                    if args.len() < 6 {
                        return Err(HorizonError::InvalidSql(
                            "snippet() requires 6 arguments".into(),
                        ));
                    }
                    let col_idx = match &args[1] {
                        Expr::Literal(LiteralValue::Integer(n)) => *n as usize,
                        _ => 0,
                    };
                    let before = match &args[2] {
                        Expr::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => "<b>".to_string(),
                    };
                    let after = match &args[3] {
                        Expr::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => "</b>".to_string(),
                    };
                    let ellipsis = match &args[4] {
                        Expr::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => "...".to_string(),
                    };
                    let max_tokens = match &args[5] {
                        Expr::Literal(LiteralValue::Integer(n)) => *n as usize,
                        _ => 10,
                    };
                    let result = fts5::fts5_snippet(
                        table_name, rowid, col_idx, &before, &after, &ellipsis, max_tokens, query,
                    )?;
                    Ok(Value::Text(result))
                }
                "BM25" => {
                    let score = fts5::fts5_bm25(table_name, rowid, query)?;
                    Ok(Value::Real(score))
                }
                _ => {
                    // For other functions, try to evaluate with dynamic context
                    let col_names: Vec<String> = fts_columns.to_vec();
                    eval_function_dynamic(name, args, doc_values, &col_names)
                }
            }
        }
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::BinaryOp { left, op, right } => {
            let l = eval_fts5_expr(left, table_name, rowid, doc_values, fts_columns, bm25_score, query)?;
            let r = eval_fts5_expr(right, table_name, rowid, doc_values, fts_columns, bm25_score, query)?;
            Ok(eval_binary_op(&l, op, &r))
        }
        _ => Ok(Value::Null),
    }
}

// ---- FTS5 INSERT ----

fn execute_fts5_insert(ins: &InsertStatement) -> Result<usize> {
    let columns = fts5::fts5_get_columns(&ins.table)?;
    let mut inserted = 0;

    for value_row in &ins.values {
        // Evaluate expressions to get values
        let dummy_table = TableInfo {
            name: ins.table.clone(),
            columns: vec![],
            root_page: 0,
            next_rowid: 0,
            pk_column: None,
        };

        let mut col_values: Vec<String> = Vec::new();

        if let Some(ref col_names) = ins.columns {
            // Named columns: map values to column positions
            let mut vals = vec![String::new(); columns.len()];
            for (i, col_name) in col_names.iter().enumerate() {
                if let Some(pos) = columns.iter().position(|c| c.eq_ignore_ascii_case(col_name)) {
                    let val = eval_expr(&value_row[i], &[], &[], &dummy_table)?;
                    vals[pos] = match val {
                        Value::Text(s) => s,
                        Value::Null => String::new(),
                        other => format!("{}", other),
                    };
                } else {
                    return Err(HorizonError::ColumnNotFound(format!(
                        "{}.{}",
                        ins.table, col_name
                    )));
                }
            }
            col_values = vals;
        } else {
            // Positional: assign values to columns in order
            for expr in value_row {
                let val = eval_expr(expr, &[], &[], &dummy_table)?;
                col_values.push(match val {
                    Value::Text(s) => s,
                    Value::Null => String::new(),
                    other => format!("{}", other),
                });
            }
            // Pad with empty strings if fewer values than columns
            while col_values.len() < columns.len() {
                col_values.push(String::new());
            }
        }

        fts5::fts5_insert(&ins.table, col_values)?;
        inserted += 1;
    }

    Ok(inserted)
}

// ---- INSERT ----

fn execute_insert(
    ins: &InsertStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    // Check if this is an FTS5 virtual table
    if fts5::fts5_table_exists(&ins.table) {
        return execute_fts5_insert(ins);
    }

    let table = catalog.get_table(&ins.table)?.clone();
    let mut tree = BTree::open(table.root_page);
    let mut next_rowid = table.next_rowid;
    let _txn_id = txn_mgr.auto_commit();
    let mut inserted = 0;

    // Fire BEFORE INSERT triggers
    views_triggers::fire_triggers(&ins.table, &TriggerEventKind::Insert, &TriggerTimingKind::Before, pool, catalog, txn_mgr)?;

    for value_row in &ins.values {
        // Determine column ordering, filtering out generated columns
        let col_order: Vec<usize> = if let Some(ref col_names) = ins.columns {
            let mut order = Vec::new();
            for name in col_names {
                let idx = table.find_column_index(name).ok_or_else(|| {
                    HorizonError::ColumnNotFound(format!("{}.{}", ins.table, name))
                })?;
                // Reject user-supplied values for generated columns
                if table.columns[idx].generated_expr.is_some() {
                    return Err(HorizonError::InvalidSql(format!(
                        "cannot INSERT into generated column: {}", name
                    )));
                }
                order.push(idx);
            }
            order
        } else {
            // When no column list is specified, exclude generated columns
            // so the user only provides values for non-generated columns
            let non_gen: Vec<usize> = (0..table.columns.len())
                .filter(|&i| table.columns[i].generated_expr.is_none())
                .collect();
            non_gen
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
            if !col_order.contains(&i) && col.generated_expr.is_none() {
                if let Some(ref default_val) = col.default_value {
                    row_values[i] = default_val.clone();
                }
            }
        }

        // Evaluate STORED generated column expressions
        for (i, col) in table.columns.iter().enumerate() {
            if let Some(ref gen_expr) = col.generated_expr {
                if col.is_stored {
                    let val = eval_expr(gen_expr, &row_values, &table.columns, &table)?;
                    row_values[i] = val.apply_affinity(col.affinity);
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

        // Check NOT NULL constraints (skip virtual generated columns)
        for (i, col) in table.columns.iter().enumerate() {
            // Virtual generated columns are not stored; their value is NULL on disk
            if col.generated_expr.is_some() && !col.is_stored {
                continue;
            }
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

    // Fire AFTER INSERT triggers
    views_triggers::fire_triggers(&ins.table, &TriggerEventKind::Insert, &TriggerTimingKind::After, pool, catalog, txn_mgr)?;

    Ok(inserted)
}

// ---- SELECT ----

fn execute_select(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<QueryResult> {
    // --- Phase 1: Process CTEs ---
    let cte_store = if select.ctes.is_empty() {
        CteStore::new()
    } else {
        execute_ctes(&select.ctes, pool, catalog)?
    };

    // --- Phase 2: Execute base SELECT body ---
    let (col_names, mut rows) = execute_select_body_inner(select, pool, catalog, &cte_store)?;

    // --- Phase 3: Handle compound operators (UNION/INTERSECT/EXCEPT) ---
    if !select.compound.is_empty() {
        for compound_op in &select.compound {
            let rhs_stmt = select_body_to_statement(&compound_op.select);
            let (_rhs_cols, rhs_rows) = execute_select_body_inner(&rhs_stmt, pool, catalog, &cte_store)?;

            match compound_op.op {
                CompoundType::UnionAll => {
                    rows.extend(rhs_rows);
                }
                CompoundType::Union => {
                    rows.extend(rhs_rows);
                    let mut seen: Vec<Vec<Value>> = Vec::new();
                    let mut unique = Vec::new();
                    for row in rows {
                        if !seen.contains(&row) {
                            seen.push(row.clone());
                            unique.push(row);
                        }
                    }
                    rows = unique;
                }
                CompoundType::Intersect => {
                    let mut result = Vec::new();
                    for row in &rows {
                        if rhs_rows.contains(row) && !result.contains(row) {
                            result.push(row.clone());
                        }
                    }
                    rows = result;
                }
                CompoundType::Except => {
                    let mut result = Vec::new();
                    for row in &rows {
                        if !rhs_rows.contains(row) && !result.contains(row) {
                            result.push(row.clone());
                        }
                    }
                    rows = result;
                }
            }
        }

        let columns = Arc::new(col_names);
        let mut result_rows: Vec<Row> = rows
            .into_iter()
            .map(|values| Row { columns: columns.clone(), values })
            .collect();

        if !select.order_by.is_empty() {
            sort_rows_by_index(&mut result_rows, &select.order_by, &columns)?;
        }
        if let Some(ref offset_expr) = select.offset {
            let offset = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
            if offset < result_rows.len() { result_rows = result_rows.into_iter().skip(offset).collect(); }
            else { result_rows.clear(); }
        }
        if let Some(ref limit_expr) = select.limit {
            let limit = eval_const_expr(limit_expr).as_integer().unwrap_or(i64::MAX) as usize;
            result_rows.truncate(limit);
        }
        return Ok(QueryResult { columns, rows: result_rows });
    }

    // --- Phase 4: No compound -- wrap up ---
    let columns = Arc::new(col_names);
    let result_rows: Vec<Row> = rows
        .into_iter()
        .map(|values| Row { columns: columns.clone(), values })
        .collect();
    Ok(QueryResult { columns, rows: result_rows })
}

fn select_body_to_statement(body: &SelectBody) -> SelectStatement {
    SelectStatement {
        ctes: vec![], distinct: body.distinct, columns: body.columns.clone(),
        from: body.from.clone(), where_clause: body.where_clause.clone(),
        group_by: body.group_by.clone(), having: body.having.clone(),
        order_by: vec![], limit: None, offset: None, compound: vec![],
    }
}

fn execute_ctes(ctes: &[Cte], pool: &mut BufferPool, catalog: &mut Catalog) -> Result<CteStore> {
    let mut store = CteStore::new();
    for cte in ctes {
        if cte.recursive {
            execute_recursive_cte(cte, pool, catalog, &mut store)?;
        } else {
            let (col_names, rows) = execute_cte_query(&cte.query, pool, catalog, &store)?;
            let final_col_names = if let Some(ref cte_cols) = cte.columns { cte_cols.clone() } else { col_names };
            store.insert(cte.name.to_lowercase(), (final_col_names, rows));
        }
    }
    Ok(store)
}

fn execute_recursive_cte(cte: &Cte, pool: &mut BufferPool, catalog: &mut Catalog, store: &mut CteStore) -> Result<()> {
    let anchor_stmt = &cte.query;
    let anchor_only = SelectStatement {
        ctes: vec![], distinct: anchor_stmt.distinct, columns: anchor_stmt.columns.clone(),
        from: anchor_stmt.from.clone(), where_clause: anchor_stmt.where_clause.clone(),
        group_by: anchor_stmt.group_by.clone(), having: anchor_stmt.having.clone(),
        order_by: vec![], limit: None, offset: None, compound: vec![],
    };
    let (anchor_cols, anchor_rows) = execute_cte_query(&anchor_only, pool, catalog, store)?;
    let col_names = if let Some(ref cte_cols) = cte.columns { cte_cols.clone() } else { anchor_cols };
    let mut all_rows = anchor_rows.clone();
    let mut working_table = anchor_rows;
    const MAX_RECURSION_DEPTH: usize = 10_000;
    let mut depth = 0;
    while !working_table.is_empty() && depth < MAX_RECURSION_DEPTH {
        depth += 1;
        store.insert(cte.name.to_lowercase(), (col_names.clone(), working_table.clone()));
        let mut new_rows = Vec::new();
        for compound_op in &anchor_stmt.compound {
            let rhs_stmt = select_body_to_statement(&compound_op.select);
            let (_rhs_cols, rhs_rows) = execute_cte_query(&rhs_stmt, pool, catalog, store)?;
            new_rows.extend(rhs_rows);
        }
        if new_rows.is_empty() { break; }
        all_rows.extend(new_rows.clone());
        working_table = new_rows;
    }
    store.insert(cte.name.to_lowercase(), (col_names, all_rows));
    Ok(())
}

fn execute_cte_query(select: &SelectStatement, pool: &mut BufferPool, catalog: &mut Catalog, cte_store: &CteStore) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    execute_select_body_inner(select, pool, catalog, cte_store)
}

fn execute_select_body_inner(
    select: &SelectStatement, pool: &mut BufferPool, catalog: &mut Catalog, cte_store: &CteStore,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if let Some(ref from) = select.from {
        if let Some(cte_result) = try_resolve_cte_from(from, cte_store) {
            return execute_select_from_cte(select, cte_result, pool, catalog);
        }
        if from_contains_cte(from, cte_store) {
            return execute_select_with_cte_join(select, pool, catalog, cte_store);
        }
    }
    if select_has_window_function(&select.columns) {
        let result = execute_select_with_window_functions(select, pool, catalog)?;
        let col_names = result.columns.as_ref().clone();
        let rows = result.rows.into_iter().map(|r| r.values).collect();
        return Ok((col_names, rows));
    }
    // COUNT(*) fast path: use BTree::count() instead of scanning all rows
    if let Some(count_result) = try_count_star_fast_path(select, pool, catalog)? {
        return Ok(count_result);
    }
    let needs_plan = matches!(&select.from, Some(FromClause::Join { .. }))
        || !select.group_by.is_empty() || select.having.is_some()
        || select_has_aggregate(&select.columns);
    if needs_plan {
        let stmt = Statement::Select(select.clone());
        let plan = plan_statement(&stmt, catalog)?;
        let result = execute_plan_select(&plan, pool, catalog)?;
        let col_names = result.columns.as_ref().clone();
        let rows = result.rows.into_iter().map(|r| r.values).collect();
        return Ok((col_names, rows));
    }
    // Handle FTS5 table function syntax: FROM table('query')
    if let Some(FromClause::TableFunction { name, args, .. }) = &select.from {
        if fts5::fts5_table_exists(name) {
            return execute_fts5_select(select, name, Some(args), pool, catalog);
        }
    }

    let (table_name, _alias) = match &select.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        Some(FromClause::Join { .. }) => { unreachable!(); }
        Some(FromClause::Subquery { .. }) => { return Err(HorizonError::NotImplemented("subqueries in FROM".into())); }
        Some(FromClause::TableFunction { .. }) => { return Err(HorizonError::NotImplemented("table functions in FROM".into())); }
        None => {
            let result = execute_select_no_from(select)?;
            let col_names = result.columns.as_ref().clone();
            let rows = result.rows.into_iter().map(|r| r.values).collect();
            return Ok((col_names, rows));
        }
    };
    // Check if this is an FTS5 virtual table
    if fts5::fts5_table_exists(&table_name) {
        return execute_fts5_select(select, &table_name, None, pool, catalog);
    }
    // Check if this is an R-tree virtual table
    if catalog.rtree_exists(&table_name) {
        let result = rtree::execute_rtree_select(select, pool, catalog)?;
        let col_names = result.columns.as_ref().clone();
        let rows = result.rows.into_iter().map(|r| r.values).collect();
        return Ok((col_names, rows));
    }
    // Check if this is a view and expand it
    if !catalog.table_exists(&table_name) {
        if let Some(view) = catalog.get_view(&table_name).cloned() {
            return execute_view_select(select, &view, pool, catalog);
        }
    }
    let table = catalog.get_table(&table_name)?.clone();
    let data_tree = BTree::open(table.root_page);
    let entries = scan_with_index(
        select.where_clause.as_ref(), &table_name, &table, &data_tree, pool, catalog,
    )?;
    let column_names = resolve_column_names(&select.columns, &table)?;
    let has_virtual = table_has_virtual_columns(&table);
    let mut rows = Vec::new();
    for entry in &entries {
        let mut row_values = deserialize_row(&entry.value, table.columns.len())?;
        if has_virtual {
            fill_virtual_columns(&mut row_values, &table)?;
        }
        if let Some(ref where_clause) = select.where_clause {
            let result = eval_expr_with_ctx(where_clause, &row_values, &table.columns, &table, pool, catalog)?;
            if !result.to_bool() { continue; }
        }
        rows.push(project_row_with_ctx(&select.columns, &row_values, &table, pool, catalog)?);
    }
    if !select.order_by.is_empty() {
        let columns_arc = Arc::new(column_names.clone());
        let mut result_rows: Vec<Row> = rows.into_iter()
            .map(|values| Row { columns: columns_arc.clone(), values }).collect();
        sort_rows(&mut result_rows, &select.order_by, &table)?;
        rows = result_rows.into_iter().map(|r| r.values).collect();
    }
    if select.distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new();
        let mut unique = Vec::new();
        for row in rows { if !seen.contains(&row) { seen.push(row.clone()); unique.push(row); } }
        rows = unique;
    }
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
        if offset < rows.len() { rows = rows.into_iter().skip(offset).collect(); } else { rows.clear(); }
    }
    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_expr(limit_expr).as_integer().unwrap_or(i64::MAX) as usize;
        rows.truncate(limit);
    }
    Ok((column_names, rows))
}

fn execute_view_select(
    outer_select: &SelectStatement, view: &ViewInfo, pool: &mut BufferPool, catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let view_stmts = Parser::parse(&view.sql)?;
    let view_select = match view_stmts.into_iter().next() {
        Some(Statement::Select(sel)) => sel,
        _ => return Err(HorizonError::Internal("view SQL is not a SELECT".into())),
    };
    let cte_store = CteStore::new();
    let (mut view_col_names, view_rows) = execute_select_body_inner(&view_select, pool, catalog, &cte_store)?;
    if let Some(ref aliases) = view.columns {
        for (i, alias) in aliases.iter().enumerate() {
            if i < view_col_names.len() { view_col_names[i] = alias.clone(); }
        }
    }
    let is_star = outer_select.columns.len() == 1 && matches!(outer_select.columns[0], SelectColumn::AllColumns);
    if is_star && outer_select.where_clause.is_none() && outer_select.order_by.is_empty()
        && outer_select.limit.is_none() && outer_select.offset.is_none() {
        return Ok((view_col_names, view_rows));
    }
    let filtered = if let Some(ref wh) = outer_select.where_clause {
        let mut r = Vec::new();
        for row in &view_rows {
            if eval_expr_dynamic(wh, row, &view_col_names)?.to_bool() { r.push(row.clone()); }
        }
        r
    } else { view_rows };
    let mut col_names = Vec::new();
    let mut proj_rows = Vec::new();
    if is_star {
        col_names = view_col_names.clone();
        proj_rows = filtered;
    } else {
        for sc in &outer_select.columns {
            match sc {
                SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => col_names.extend(view_col_names.clone()),
                SelectColumn::Expr { expr, alias } => col_names.push(alias.clone().unwrap_or_else(|| {
                    if let Expr::Column { name, .. } = expr { name.clone() } else { format!("{:?}", expr) }
                })),
            }
        }
        for row in &filtered {
            let mut pr = Vec::new();
            for sc in &outer_select.columns {
                match sc {
                    SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => pr.extend(row.clone()),
                    SelectColumn::Expr { expr, .. } => pr.push(eval_expr_dynamic(expr, row, &view_col_names)?),
                }
            }
            proj_rows.push(pr);
        }
    }
    if !outer_select.order_by.is_empty() {
        let ca = Arc::new(col_names.clone());
        let mut rr: Vec<Row> = proj_rows.into_iter().map(|v| Row { columns: ca.clone(), values: v }).collect();
        sort_rows_by_index(&mut rr, &outer_select.order_by, &col_names)?;
        proj_rows = rr.into_iter().map(|r| r.values).collect();
    }
    if let Some(ref oe) = outer_select.offset {
        let o = eval_const_expr(oe).as_integer().unwrap_or(0) as usize;
        if o < proj_rows.len() { proj_rows = proj_rows.into_iter().skip(o).collect(); } else { proj_rows.clear(); }
    }
    if let Some(ref le) = outer_select.limit {
        let l = eval_const_expr(le).as_integer().unwrap_or(i64::MAX) as usize;
        proj_rows.truncate(l);
    }
    Ok((col_names, proj_rows))
}

fn try_resolve_cte_from<'a>(from: &FromClause, cte_store: &'a CteStore) -> Option<&'a (Vec<String>, Vec<Vec<Value>>)> {
    match from { FromClause::Table { name, .. } => cte_store.get(&name.to_lowercase()), _ => None }
}

fn from_contains_cte(from: &FromClause, cte_store: &CteStore) -> bool {
    match from {
        FromClause::Table { name, .. } => cte_store.contains_key(&name.to_lowercase()),
        FromClause::Join { left, right, .. } => from_contains_cte(left, cte_store) || from_contains_cte(right, cte_store),
        FromClause::Subquery { .. } | FromClause::TableFunction { .. } => false,
    }
}

fn execute_select_from_cte(
    select: &SelectStatement, cte_data: &(Vec<String>, Vec<Vec<Value>>), pool: &mut BufferPool, catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let (cte_col_names, cte_rows) = cte_data;
    if !select.group_by.is_empty() || select.having.is_some() || select_has_aggregate(&select.columns) {
        return execute_cte_with_aggregates(select, cte_col_names, cte_rows, pool, catalog);
    }
    let out_col_names = resolve_column_names_from_cte(&select.columns, cte_col_names)?;
    let mut rows = Vec::new();
    for row_values in cte_rows {
        if let Some(ref where_clause) = select.where_clause {
            let result = eval_expr_dynamic_with_ctx(where_clause, row_values, cte_col_names, pool, catalog)?;
            if !result.to_bool() { continue; }
        }
        rows.push(project_row_dynamic(&select.columns, row_values, cte_col_names)?);
    }
    if !select.order_by.is_empty() { sort_rows_dynamic(&mut rows, &select.order_by, &out_col_names)?; }
    if select.distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new(); let mut unique = Vec::new();
        for row in rows { if !seen.contains(&row) { seen.push(row.clone()); unique.push(row); } }
        rows = unique;
    }
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
        if offset < rows.len() { rows = rows.into_iter().skip(offset).collect(); } else { rows.clear(); }
    }
    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_expr(limit_expr).as_integer().unwrap_or(i64::MAX) as usize; rows.truncate(limit);
    }
    Ok((out_col_names, rows))
}

fn execute_cte_with_aggregates(
    select: &SelectStatement, cte_col_names: &[String], cte_rows: &[Vec<Value>], pool: &mut BufferPool, catalog: &mut Catalog,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut filtered_rows = Vec::new();
    for row_values in cte_rows {
        if let Some(ref where_clause) = select.where_clause {
            let result = eval_expr_dynamic_with_ctx(where_clause, row_values, cte_col_names, pool, catalog)?;
            if !result.to_bool() { continue; }
        }
        filtered_rows.push(row_values.clone());
    }
    let groups = group_rows(&filtered_rows, &select.group_by, cte_col_names)?;
    let out_col_names = resolve_column_names_dynamic(&select.columns, cte_col_names)?;
    let mut result = Vec::new();
    for (_key, group) in &groups {
        let representative = if group.is_empty() { vec![Value::Null; cte_col_names.len()] } else { group[0].clone() };
        if let Some(ref having_expr) = select.having {
            if !eval_aggregate_expr(having_expr, &representative, cte_col_names, group)?.to_bool() { continue; }
        }
        let mut out_row = Vec::new();
        for col in &select.columns {
            match col {
                SelectColumn::AllColumns => out_row.extend(representative.iter().cloned()),
                SelectColumn::TableAllColumns(_) => out_row.extend(representative.iter().cloned()),
                SelectColumn::Expr { expr, .. } => { out_row.push(eval_aggregate_expr(expr, &representative, cte_col_names, group)?); }
            }
        }
        result.push(out_row);
    }
    Ok((out_col_names, result))
}

fn execute_select_with_cte_join(
    select: &SelectStatement, pool: &mut BufferPool, catalog: &mut Catalog, cte_store: &CteStore,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let from = select.from.as_ref().unwrap();
    let (merged_cols, mut result_rows) = execute_from_with_ctes(from, pool, catalog, cte_store)?;
    if let Some(ref where_clause) = select.where_clause {
        let mut filtered = Vec::new();
        for row in result_rows { if eval_expr_dynamic(where_clause, &row, &merged_cols)?.to_bool() { filtered.push(row); } }
        result_rows = filtered;
    }
    if !select.group_by.is_empty() || select.having.is_some() || select_has_aggregate(&select.columns) {
        let groups = group_rows(&result_rows, &select.group_by, &merged_cols)?;
        let out_col_names = resolve_column_names_dynamic(&select.columns, &merged_cols)?;
        let mut result = Vec::new();
        for (_key, group) in &groups {
            let representative = if group.is_empty() { vec![Value::Null; merged_cols.len()] } else { group[0].clone() };
            if let Some(ref having_expr) = select.having {
                if !eval_aggregate_expr(having_expr, &representative, &merged_cols, group)?.to_bool() { continue; }
            }
            let mut out_row = Vec::new();
            for col in &select.columns { match col {
                SelectColumn::AllColumns => out_row.extend(representative.iter().cloned()),
                SelectColumn::TableAllColumns(_) => out_row.extend(representative.iter().cloned()),
                SelectColumn::Expr { expr, .. } => { out_row.push(eval_aggregate_expr(expr, &representative, &merged_cols, group)?); }
            }}
            result.push(out_row);
        }
        return Ok((out_col_names, result));
    }
    let out_col_names = resolve_column_names_dynamic(&select.columns, &merged_cols)?;
    let mut out_rows = Vec::new();
    for row in &result_rows { out_rows.push(project_row_dynamic(&select.columns, row, &merged_cols)?); }
    if !select.order_by.is_empty() { sort_rows_dynamic(&mut out_rows, &select.order_by, &out_col_names)?; }
    if select.distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new(); let mut unique = Vec::new();
        for row in out_rows { if !seen.contains(&row) { seen.push(row.clone()); unique.push(row); } }
        out_rows = unique;
    }
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_expr(offset_expr).as_integer().unwrap_or(0) as usize;
        if offset < out_rows.len() { out_rows = out_rows.into_iter().skip(offset).collect(); } else { out_rows.clear(); }
    }
    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_expr(limit_expr).as_integer().unwrap_or(i64::MAX) as usize; out_rows.truncate(limit);
    }
    Ok((out_col_names, out_rows))
}

fn execute_from_with_ctes(from: &FromClause, pool: &mut BufferPool, catalog: &mut Catalog, cte_store: &CteStore) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    match from {
        FromClause::Table { name, alias } => {
            if let Some((cte_cols, cte_rows)) = cte_store.get(&name.to_lowercase()) {
                let prefix = alias.as_deref().unwrap_or(name);
                Ok((cte_cols.iter().map(|c| format!("{}.{}", prefix, c)).collect(), cte_rows.clone()))
            } else {
                let table_info = catalog.get_table(name)?.clone();
                let data_tree = BTree::open(table_info.root_page);
                let entries = data_tree.scan_all(pool)?;
                let prefix = alias.as_deref().unwrap_or(name);
                let col_names: Vec<String> = table_info.columns.iter().map(|c| format!("{}.{}", prefix, c.name)).collect();
                let mut rows = Vec::new();
                for entry in &entries { rows.push(deserialize_row(&entry.value, table_info.columns.len())?); }
                Ok((col_names, rows))
            }
        }
        FromClause::Join { left, right, join_type, on } => {
            let (left_cols, left_rows) = execute_from_with_ctes(left, pool, catalog, cte_store)?;
            let (right_cols, right_rows) = execute_from_with_ctes(right, pool, catalog, cte_store)?;
            let num_right = right_cols.len(); let num_left = left_cols.len();
            let mut merged_cols = left_cols.clone(); merged_cols.extend(right_cols.clone());
            let null_right: Vec<Value> = vec![Value::Null; num_right];
            let null_left: Vec<Value> = vec![Value::Null; num_left];
            let mut result = Vec::new();
            match join_type {
                JoinType::Inner => { for l in &left_rows { for r in &right_rows {
                    let mut m = l.clone(); m.extend(r.iter().cloned());
                    if let Some(ref e) = on { if !eval_expr_dynamic(e, &m, &merged_cols)?.to_bool() { continue; } }
                    result.push(m);
                }}}
                JoinType::Left => { for l in &left_rows { let mut matched = false; for r in &right_rows {
                    let mut m = l.clone(); m.extend(r.iter().cloned());
                    if let Some(ref e) = on { if !eval_expr_dynamic(e, &m, &merged_cols)?.to_bool() { continue; } }
                    matched = true; result.push(m);
                } if !matched { let mut m = l.clone(); m.extend(null_right.iter().cloned()); result.push(m); } }}
                JoinType::Right => { for r in &right_rows { let mut matched = false; for l in &left_rows {
                    let mut m = l.clone(); m.extend(r.iter().cloned());
                    if let Some(ref e) = on { if !eval_expr_dynamic(e, &m, &merged_cols)?.to_bool() { continue; } }
                    matched = true; result.push(m);
                } if !matched { let mut m = null_left.clone(); m.extend(r.iter().cloned()); result.push(m); } }}
                JoinType::Cross => { for l in &left_rows { for r in &right_rows {
                    let mut m = l.clone(); m.extend(r.iter().cloned()); result.push(m);
                }}}
            }
            Ok((merged_cols, result))
        }
        FromClause::Subquery { .. } => Err(HorizonError::NotImplemented("subquery in FROM with CTEs".into())),
        FromClause::TableFunction { .. } => Err(HorizonError::NotImplemented("table function in FROM with CTEs".into())),
    }
}

fn resolve_column_names_from_cte(select_cols: &[SelectColumn], cte_col_names: &[String]) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for col in select_cols { match col {
        SelectColumn::AllColumns => names.extend(cte_col_names.iter().cloned()),
        SelectColumn::TableAllColumns(prefix) => {
            let prefix_dot = format!("{}.", prefix);
            for c in cte_col_names {
                if c.to_lowercase().starts_with(&prefix_dot.to_lowercase()) { names.push(c[prefix_dot.len()..].to_string()); }
                else { names.push(c.clone()); }
            }
        }
        SelectColumn::Expr { expr, alias } => {
            if let Some(a) = alias { names.push(a.clone()); }
            else if let Expr::Column { name, .. } = expr { names.push(name.clone()); }
            else if let Expr::Function { name, .. } = expr { names.push(name.clone()); }
            else { names.push(format!("{:?}", expr)); }
        }
    }}
    Ok(names)
}

fn sort_rows_by_index(rows: &mut [Row], order_by: &[OrderByItem], col_names: &[String]) -> Result<()> {
    rows.sort_by(|a, b| {
        for item in order_by {
            let va = eval_expr_dynamic(&item.expr, &a.values, col_names).unwrap_or(Value::Null);
            let vb = eval_expr_dynamic(&item.expr, &b.values, col_names).unwrap_or(Value::Null);
            let cmp = if let Some(coll) = extract_collation(&item.expr) {
                compare_with_collation(&va, &vb, coll)
            } else {
                va.cmp(&vb)
            };
            let cmp = if item.desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal { return cmp; }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

/// Fast path for `SELECT COUNT(*) FROM table` with no WHERE, GROUP BY, HAVING,
/// DISTINCT, JOINs, or compound queries. Uses BTree::count() which traverses
/// leaf pages counting entries without deserializing any row data.
fn try_count_star_fast_path(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<Option<(Vec<String>, Vec<Vec<Value>>)>> {
    // Must be a single table, no WHERE, no GROUP BY, no HAVING, no DISTINCT, no compound
    if select.where_clause.is_some() || !select.group_by.is_empty()
        || select.having.is_some() || select.distinct || !select.compound.is_empty() {
        return Ok(None);
    }
    // Must have exactly one column: COUNT(*)
    if select.columns.len() != 1 {
        return Ok(None);
    }
    let is_count_star = match &select.columns[0] {
        SelectColumn::Expr { expr, .. } => match expr {
            Expr::Function { name, args, .. } => {
                name.eq_ignore_ascii_case("count")
                    && args.len() == 1
                    && matches!(&args[0], Expr::Column { name: col_name, .. } if col_name == "*")
            }
            _ => false,
        },
        _ => false,
    };
    if !is_count_star {
        return Ok(None);
    }
    // Must be FROM a single real table (not a join, subquery, or virtual table)
    let table_name = match &select.from {
        Some(FromClause::Table { name, .. }) => name.clone(),
        _ => return Ok(None),
    };
    // Skip virtual tables
    if fts5::fts5_table_exists(&table_name) || catalog.rtree_exists(&table_name) {
        return Ok(None);
    }
    // Skip views
    if !catalog.table_exists(&table_name) {
        return Ok(None);
    }
    let table = catalog.get_table(&table_name)?;
    let tree = BTree::open(table.root_page);
    let count = tree.count(pool)? as i64;

    let col_name = match &select.columns[0] {
        SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
        _ => "COUNT(*)".to_string(),
    };
    Ok(Some((vec![col_name], vec![vec![Value::Integer(count)]])))
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

/// Check if any select columns contain window function calls.
fn select_has_window_function(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|col| {
        if let SelectColumn::Expr { expr, .. } = col { expr_has_window_fn(expr) } else { false }
    })
}

/// Recursively check if an expression contains a window function.
fn expr_has_window_fn(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::BinaryOp { left, right, .. } => expr_has_window_fn(left) || expr_has_window_fn(right),
        Expr::UnaryOp { expr, .. } => expr_has_window_fn(expr),
        Expr::Cast { expr, .. } => expr_has_window_fn(expr),
        _ => false,
    }
}

// ---- Window Function Execution ----

fn execute_select_with_window_functions(
    select: &SelectStatement, pool: &mut BufferPool, catalog: &mut Catalog,
) -> Result<QueryResult> {
    let (table_name, _alias) = match &select.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        Some(FromClause::Join { .. }) => return Err(HorizonError::NotImplemented("window functions with JOINs".into())),
        Some(FromClause::Subquery { .. }) => return Err(HorizonError::NotImplemented("window functions with subqueries".into())),
        Some(FromClause::TableFunction { .. }) => return Err(HorizonError::NotImplemented("window functions with table functions".into())),
        None => return Err(HorizonError::InvalidSql("window functions require a FROM clause".into())),
    };
    let table = catalog.get_table(&table_name)?.clone();
    let data_tree = BTree::open(table.root_page);
    let entries = data_tree.scan_all(pool)?;
    let mut base_rows: Vec<Vec<Value>> = Vec::new();
    for entry in &entries {
        let row_values = deserialize_row(&entry.value, table.columns.len())?;
        if let Some(ref wc) = select.where_clause {
            if !eval_expr(wc, &row_values, &table.columns, &table)?.to_bool() { continue; }
        }
        base_rows.push(row_values);
    }
    let column_names = resolve_column_names_for_window(&select.columns, &table)?;
    let columns_arc = Arc::new(column_names);
    let total = base_rows.len();
    let mut result_rows: Vec<Vec<Value>> = vec![Vec::new(); total];
    for sel_col in &select.columns {
        match sel_col {
            SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => {
                for (i, row) in base_rows.iter().enumerate() { result_rows[i].extend(row.iter().cloned()); }
            }
            SelectColumn::Expr { expr, .. } => {
                if expr_has_window_fn(expr) {
                    let wv = evaluate_window_expr(expr, &base_rows, &table)?;
                    for (i, val) in wv.into_iter().enumerate() { result_rows[i].push(val); }
                } else {
                    for (i, row) in base_rows.iter().enumerate() {
                        result_rows[i].push(eval_expr(expr, row, &table.columns, &table)?);
                    }
                }
            }
        }
    }
    let mut rows: Vec<Row> = result_rows.into_iter()
        .map(|values| Row { columns: columns_arc.clone(), values }).collect();
    if !select.order_by.is_empty() { sort_rows(&mut rows, &select.order_by, &table)?; }
    if select.distinct { rows.dedup_by(|a, b| a.values == b.values); }
    if let Some(ref oe) = select.offset {
        let o = eval_const_expr(oe).as_integer().unwrap_or(0) as usize;
        if o < rows.len() { rows = rows.into_iter().skip(o).collect(); } else { rows.clear(); }
    }
    if let Some(ref le) = select.limit {
        rows.truncate(eval_const_expr(le).as_integer().unwrap_or(i64::MAX) as usize);
    }
    Ok(QueryResult { columns: columns_arc, rows })
}

fn resolve_column_names_for_window(cols: &[SelectColumn], table: &TableInfo) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for col in cols {
        match col {
            SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => {
                for c in &table.columns { names.push(c.name.clone()); }
            }
            SelectColumn::Expr { expr, alias } => {
                if let Some(a) = alias { names.push(a.clone()); }
                else if let Expr::Column { name, .. } = expr { names.push(name.clone()); }
                else if let Expr::WindowFunction { function, .. } = expr {
                    if let Expr::Function { name, .. } = function.as_ref() { names.push(name.clone()); }
                    else { names.push(format!("{:?}", expr)); }
                } else if let Expr::Function { name, .. } = expr { names.push(name.clone()); }
                else { names.push(format!("{:?}", expr)); }
            }
        }
    }
    Ok(names)
}

fn evaluate_window_expr(expr: &Expr, all_rows: &[Vec<Value>], table: &TableInfo) -> Result<Vec<Value>> {
    if let Expr::WindowFunction { function, partition_by, order_by, frame } = expr {
        compute_window_function(function, partition_by, order_by, frame, all_rows, table)
    } else {
        all_rows.iter().map(|row| eval_expr(expr, row, &table.columns, table)).collect()
    }
}

fn compute_window_function(
    function: &Expr, partition_by: &[Expr], order_by: &[OrderByItem],
    frame: &Option<WindowFrame>, all_rows: &[Vec<Value>], table: &TableInfo,
) -> Result<Vec<Value>> {
    let total = all_rows.len();
    let mut results = vec![Value::Null; total];
    let mut partition_map: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
    for (i, row) in all_rows.iter().enumerate() {
        let key: Vec<Value> = partition_by.iter()
            .map(|e| eval_expr(e, row, &table.columns, table)).collect::<Result<_>>()?;
        if let Some((_, indices)) = partition_map.iter_mut().find(|(k, _)| *k == key) {
            indices.push(i);
        } else { partition_map.push((key, vec![i])); }
    }
    let (func_name, func_args) = match function {
        Expr::Function { name, args, .. } => (name.to_uppercase(), args.clone()),
        _ => return Err(HorizonError::InvalidSql("expected function call in window function".into())),
    };
    for (_key, mut indices) in partition_map {
        if !order_by.is_empty() {
            indices.sort_by(|&a, &b| {
                for item in order_by {
                    let va = eval_expr(&item.expr, &all_rows[a], &table.columns, table).unwrap_or(Value::Null);
                    let vb = eval_expr(&item.expr, &all_rows[b], &table.columns, table).unwrap_or(Value::Null);
                    let cmp = if item.desc { va.cmp(&vb).reverse() } else { va.cmp(&vb) };
                    if cmp != std::cmp::Ordering::Equal { return cmp; }
                }
                std::cmp::Ordering::Equal
            });
        }
        let pl = indices.len();
        match func_name.as_str() {
            "ROW_NUMBER" => {
                for (r, &oi) in indices.iter().enumerate() { results[oi] = Value::Integer((r + 1) as i64); }
            }
            "RANK" => {
                let mut rank = 1i64;
                for pos in 0..pl {
                    if pos > 0 && !ob_eq(order_by, &all_rows[indices[pos]], &all_rows[indices[pos-1]], table) { rank = (pos+1) as i64; }
                    results[indices[pos]] = Value::Integer(rank);
                }
            }
            "DENSE_RANK" => {
                let mut rank = 1i64;
                for pos in 0..pl {
                    if pos > 0 && !ob_eq(order_by, &all_rows[indices[pos]], &all_rows[indices[pos-1]], table) { rank += 1; }
                    results[indices[pos]] = Value::Integer(rank);
                }
            }
            "LAG" => {
                let off = if func_args.len() > 1 { eval_const_expr(&func_args[1]).as_integer().unwrap_or(1) as usize } else { 1 };
                let def = if func_args.len() > 2 { eval_const_expr(&func_args[2]) } else { Value::Null };
                for pos in 0..pl {
                    results[indices[pos]] = if pos >= off && !func_args.is_empty() {
                        eval_expr(&func_args[0], &all_rows[indices[pos-off]], &table.columns, table)?
                    } else if pos >= off { Value::Null } else { def.clone() };
                }
            }
            "LEAD" => {
                let off = if func_args.len() > 1 { eval_const_expr(&func_args[1]).as_integer().unwrap_or(1) as usize } else { 1 };
                let def = if func_args.len() > 2 { eval_const_expr(&func_args[2]) } else { Value::Null };
                for pos in 0..pl {
                    results[indices[pos]] = if pos + off < pl && !func_args.is_empty() {
                        eval_expr(&func_args[0], &all_rows[indices[pos+off]], &table.columns, table)?
                    } else if pos + off < pl { Value::Null } else { def.clone() };
                }
            }
            "FIRST_VALUE" => {
                for pos in 0..pl {
                    let (fs, _) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    results[indices[pos]] = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fs]], &table.columns, table)? } else { Value::Null };
                }
            }
            "LAST_VALUE" => {
                for pos in 0..pl {
                    let (_, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    results[indices[pos]] = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fe]], &table.columns, table)? } else { Value::Null };
                }
            }
            "SUM" => {
                for pos in 0..pl {
                    let (fs, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    let mut is = 0i64; let mut rs = 0.0f64; let mut hr = false; let mut an = true;
                    for fi in fs..=fe {
                        if let Ok(v) = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fi]], &table.columns, table) } else { Ok(Value::Null) } {
                            match v { Value::Integer(n) => { is += n; an = false; } Value::Real(r) => { rs += r; hr = true; an = false; } _ => {} }
                        }
                    }
                    results[indices[pos]] = if an { Value::Null } else if hr { Value::Real(rs + is as f64) } else { Value::Integer(is) };
                }
            }
            "COUNT" => {
                let star = func_args.len() == 1 && matches!(&func_args[0], Expr::Column { table: None, name } if name == "*");
                for pos in 0..pl {
                    let (fs, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    let mut c = 0i64;
                    for fi in fs..=fe {
                        if func_args.is_empty() || star { c += 1; }
                        else if let Ok(v) = eval_expr(&func_args[0], &all_rows[indices[fi]], &table.columns, table) { if !v.is_null() { c += 1; } }
                    }
                    results[indices[pos]] = Value::Integer(c);
                }
            }
            "AVG" => {
                for pos in 0..pl {
                    let (fs, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    let mut s = 0.0f64; let mut c = 0i64;
                    for fi in fs..=fe {
                        if let Ok(v) = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fi]], &table.columns, table) } else { Ok(Value::Null) } {
                            match v { Value::Integer(n) => { s += n as f64; c += 1; } Value::Real(r) => { s += r; c += 1; } _ => {} }
                        }
                    }
                    results[indices[pos]] = if c == 0 { Value::Null } else { Value::Real(s / c as f64) };
                }
            }
            "MIN" => {
                for pos in 0..pl {
                    let (fs, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    let mut mv: Option<Value> = None;
                    for fi in fs..=fe {
                        if let Ok(v) = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fi]], &table.columns, table) } else { Ok(Value::Null) } {
                            if !v.is_null() { mv = Some(mv.map_or(v.clone(), |cur| if v < cur { v } else { cur })); }
                        }
                    }
                    results[indices[pos]] = mv.unwrap_or(Value::Null);
                }
            }
            "MAX" => {
                for pos in 0..pl {
                    let (fs, fe) = wf_frame(frame, pos, pl, !order_by.is_empty());
                    let mut mv: Option<Value> = None;
                    for fi in fs..=fe {
                        if let Ok(v) = if !func_args.is_empty() { eval_expr(&func_args[0], &all_rows[indices[fi]], &table.columns, table) } else { Ok(Value::Null) } {
                            if !v.is_null() { mv = Some(mv.map_or(v.clone(), |cur| if v > cur { v } else { cur })); }
                        }
                    }
                    results[indices[pos]] = mv.unwrap_or(Value::Null);
                }
            }
            _ => return Err(HorizonError::NotImplemented(format!("window function: {}", func_name))),
        }
    }
    Ok(results)
}

fn ob_eq(order_by: &[OrderByItem], a: &[Value], b: &[Value], table: &TableInfo) -> bool {
    order_by.iter().all(|item| {
        eval_expr(&item.expr, a, &table.columns, table).unwrap_or(Value::Null)
            == eval_expr(&item.expr, b, &table.columns, table).unwrap_or(Value::Null)
    })
}

fn wf_frame(frame: &Option<WindowFrame>, pos: usize, pl: usize, has_order_by: bool) -> (usize, usize) {
    match frame {
        Some(f) => {
            let s = wf_bound(&f.start, pos, pl);
            let e = f.end.as_ref().map_or(pos, |b| wf_bound(b, pos, pl));
            (s, e)
        }
        // SQL standard: without ORDER BY the default frame is the entire partition;
        // with ORDER BY the default is UNBOUNDED PRECEDING to CURRENT ROW.
        None if has_order_by => (0, pos),
        None => (0, pl.saturating_sub(1)),
    }
}

fn wf_bound(bound: &WindowFrameBound, pos: usize, pl: usize) -> usize {
    match bound {
        WindowFrameBound::CurrentRow => pos,
        WindowFrameBound::Preceding(None) => 0,
        WindowFrameBound::Preceding(Some(e)) => pos.saturating_sub(eval_const_expr(e).as_integer().unwrap_or(0) as usize),
        WindowFrameBound::Following(None) => pl.saturating_sub(1),
        WindowFrameBound::Following(Some(e)) => (pos + eval_const_expr(e).as_integer().unwrap_or(0) as usize).min(pl.saturating_sub(1)),
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

// ---- JOIN Execution (Hash Join + Nested Loop fallback) ----

/// Try to extract equi-join key column indices from an ON expression like `a.col = b.col`.
/// Returns (left_key_idx, right_key_idx) as indices into the merged column list,
/// where right_key_idx is relative to right_cols (i.e. adjusted by subtracting left_cols.len()).
fn extract_equi_join_keys(
    on_expr: &Expr,
    left_cols: &[String],
    right_cols: &[String],
) -> Option<(usize, usize)> {
    if let Expr::BinaryOp { left, op: BinaryOp::Eq, right } = on_expr {
        if let (Expr::Column { name: ln, table: lt, .. }, Expr::Column { name: rn, table: rt, .. }) =
            (left.as_ref(), right.as_ref())
        {
            // Try both orderings: left=left_table, right=right_table AND reversed
            if let Some(pair) = match_join_cols(ln, lt, rn, rt, left_cols, right_cols) {
                return Some(pair);
            }
            if let Some(pair) = match_join_cols(rn, rt, ln, lt, left_cols, right_cols) {
                return Some(pair);
            }
        }
    }
    None
}

/// Match column names to left/right column lists, using suffix matching for qualified names.
fn match_join_cols(
    left_name: &str, _left_table: &Option<String>,
    right_name: &str, _right_table: &Option<String>,
    left_cols: &[String], right_cols: &[String],
) -> Option<(usize, usize)> {
    let left_idx = find_col_index(left_name, left_cols)?;
    let right_idx = find_col_index(right_name, right_cols)?;
    Some((left_idx, right_idx))
}

/// Find a column index by name, supporting both unqualified and qualified (table.col) names.
fn find_col_index(name: &str, cols: &[String]) -> Option<usize> {
    // Exact match first
    if let Some(idx) = cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
        return Some(idx);
    }
    // Suffix match: "table.col" matches "col" in the column list, or
    // "col" matches "table.col" in the column list
    let unqualified = name.rsplit('.').next().unwrap_or(name);
    cols.iter().position(|c| {
        let c_unqualified = c.rsplit('.').next().unwrap_or(c);
        c_unqualified.eq_ignore_ascii_case(unqualified)
    })
}

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

    let mut merged_cols = left_cols.clone();
    merged_cols.extend(right_cols.clone());

    // Try hash join for equi-join conditions (not CROSS joins)
    if !matches!(join_type, JoinType::Cross) {
        if let Some(ref on_expr) = on {
            if let Some((left_key_idx, right_key_idx)) = extract_equi_join_keys(on_expr, &left_cols, &right_cols) {
                return execute_hash_join(
                    &left_rows, &right_rows, left_key_idx, right_key_idx,
                    num_left, num_right, join_type, &merged_cols,
                );
            }
        }
    }

    // Fallback: nested-loop join
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
                        if !val.to_bool() { continue; }
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
                        if !val.to_bool() { continue; }
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
                        if !val.to_bool() { continue; }
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

/// Hash join implementation for equi-join conditions.
/// Builds a HashMap on the smaller side for O(n+m) join instead of O(n*m).
fn execute_hash_join(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    left_key_idx: usize,
    right_key_idx: usize,
    num_left: usize,
    num_right: usize,
    join_type: &JoinType,
    _merged_cols: &[String],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let null_right: Vec<Value> = vec![Value::Null; num_right];
    let null_left: Vec<Value> = vec![Value::Null; num_left];
    let mut result = Vec::new();

    match join_type {
        JoinType::Inner => {
            // Build hash table on right side
            let mut hash_map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (i, row) in right_rows.iter().enumerate() {
                let key = &row[right_key_idx];
                if !key.is_null() {
                    hash_map.entry(key.clone()).or_default().push(i);
                }
            }
            // Probe with left side
            for l_row in left_rows {
                let key = &l_row[left_key_idx];
                if key.is_null() { continue; }
                if let Some(matches) = hash_map.get(key) {
                    for &ri in matches {
                        let mut merged = l_row.clone();
                        merged.extend(right_rows[ri].iter().cloned());
                        result.push(merged);
                    }
                }
            }
        }
        JoinType::Left => {
            // Build hash table on right side
            let mut hash_map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (i, row) in right_rows.iter().enumerate() {
                let key = &row[right_key_idx];
                if !key.is_null() {
                    hash_map.entry(key.clone()).or_default().push(i);
                }
            }
            // Probe with left side
            for l_row in left_rows {
                let key = &l_row[left_key_idx];
                let matches = if key.is_null() { None } else { hash_map.get(key) };
                if let Some(matches) = matches {
                    for &ri in matches {
                        let mut merged = l_row.clone();
                        merged.extend(right_rows[ri].iter().cloned());
                        result.push(merged);
                    }
                } else {
                    let mut merged = l_row.clone();
                    merged.extend(null_right.iter().cloned());
                    result.push(merged);
                }
            }
        }
        JoinType::Right => {
            // Build hash table on left side, probe with right
            let mut hash_map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (i, row) in left_rows.iter().enumerate() {
                let key = &row[left_key_idx];
                if !key.is_null() {
                    hash_map.entry(key.clone()).or_default().push(i);
                }
            }
            for r_row in right_rows {
                let key = &r_row[right_key_idx];
                let matches = if key.is_null() { None } else { hash_map.get(key) };
                if let Some(matches) = matches {
                    for &li in matches {
                        let mut merged = left_rows[li].clone();
                        merged.extend(r_row.iter().cloned());
                        result.push(merged);
                    }
                } else {
                    let mut merged = null_left.clone();
                    merged.extend(r_row.iter().cloned());
                    result.push(merged);
                }
            }
        }
        JoinType::Cross => {
            unreachable!("CROSS JOIN should not reach hash join path");
        }
    }

    Ok((_merged_cols.to_vec(), result))
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
        Expr::WindowFunction { .. } => {
            // Window functions are evaluated at a higher level, not per-row.
            Err(HorizonError::Internal(
                "window functions must be evaluated via the window execution path".into(),
            ))
        }
        Expr::Collate { expr: inner, .. } => {
            // Evaluate the inner expression, ignoring collation for now
            eval_expr_dynamic(inner, row, col_names)
        }
        Expr::Match { .. } => {
            // MATCH expressions are handled at the FTS5 query level, not per-row in dynamic context.
            // If we reach here, it means we're evaluating it outside FTS context - return true
            // so it doesn't filter rows that have already been matched.
            Ok(Value::Integer(1))
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
        // -- JSON functions --
        "JSON" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => Ok(Value::Text(jv.to_json_string())),
                        None => Err(HorizonError::InvalidSql("malformed JSON".into())),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Err(HorizonError::InvalidSql("JSON() requires a text argument".into())),
            }
        }
        "JSON_EXTRACT" => {
            if args.len() < 2 { return Ok(Value::Null); }
            let json_val = eval_expr_dynamic(&args[0], row, col_names)?;
            let path_val = eval_expr_dynamic(&args[1], row, col_names)?;
            match (json_val, path_val) {
                (Value::Text(s), Value::Text(path)) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            match jv.extract_path(&path) {
                                Some(extracted) => Ok(json::json_value_to_sql(extracted)),
                                None => Ok(Value::Null),
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        "JSON_TYPE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let json_val = eval_expr_dynamic(&args[0], row, col_names)?;
            match json_val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            if args.len() >= 2 {
                                let path_val = eval_expr_dynamic(&args[1], row, col_names)?;
                                if let Value::Text(path) = path_val {
                                    match jv.extract_path(&path) {
                                        Some(extracted) => Ok(Value::Text(extracted.json_type_name().to_string())),
                                        None => Ok(Value::Null),
                                    }
                                } else {
                                    Ok(Value::Null)
                                }
                            } else {
                                Ok(Value::Text(jv.json_type_name().to_string()))
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "JSON_ARRAY" => {
            let mut items = Vec::new();
            for arg in args {
                let val = eval_expr_dynamic(arg, row, col_names)?;
                items.push(json::sql_value_to_json(&val));
            }
            let arr = json::JsonValue::Array(items);
            Ok(Value::Text(arr.to_json_string()))
        }
        "JSON_OBJECT" => {
            if args.len() % 2 != 0 {
                return Err(HorizonError::InvalidSql(
                    "JSON_OBJECT requires an even number of arguments".into(),
                ));
            }
            let mut pairs = Vec::new();
            let mut i = 0;
            while i < args.len() {
                let key_val = eval_expr_dynamic(&args[i], row, col_names)?;
                let val_val = eval_expr_dynamic(&args[i + 1], row, col_names)?;
                let key = match key_val {
                    Value::Text(s) => s,
                    Value::Integer(n) => n.to_string(),
                    Value::Real(r) => r.to_string(),
                    Value::Null => "null".to_string(),
                    Value::Blob(_) => return Err(HorizonError::InvalidSql("JSON_OBJECT keys must be text".into())),
                };
                pairs.push((key, json::sql_value_to_json(&val_val)));
                i += 2;
            }
            let obj = json::JsonValue::Object(pairs);
            Ok(Value::Text(obj.to_json_string()))
        }
        "JSON_ARRAY_LENGTH" => {
            if args.is_empty() { return Ok(Value::Null); }
            let json_val = eval_expr_dynamic(&args[0], row, col_names)?;
            match json_val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            let target = if args.len() >= 2 {
                                let path_val = eval_expr_dynamic(&args[1], row, col_names)?;
                                if let Value::Text(path) = path_val {
                                    match jv.extract_path(&path) {
                                        Some(v) => v.clone(),
                                        None => return Ok(Value::Null),
                                    }
                                } else {
                                    jv
                                }
                            } else {
                                jv
                            };
                            match target.array_length() {
                                Some(len) => Ok(Value::Integer(len as i64)),
                                None => Ok(Value::Null),
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "JSON_VALID" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => {
                    Ok(Value::Integer(if json::JsonParser::parse(&s).is_some() { 1 } else { 0 }))
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Integer(0)),
            }
        }
        // -- Additional utility functions --
        "PRINTF" => {
            if args.is_empty() { return Ok(Value::Null); }
            let fmt_val = eval_expr_dynamic(&args[0], row, col_names)?;
            let fmt_str = match fmt_val {
                Value::Text(s) => s,
                _ => return Ok(Value::Null),
            };
            // Simplified PRINTF: supports %d, %f, %s, %%
            let mut result = String::new();
            let chars: Vec<char> = fmt_str.chars().collect();
            let mut ci = 0;
            let mut arg_idx = 1;
            while ci < chars.len() {
                if chars[ci] == '%' && ci + 1 < chars.len() {
                    ci += 1;
                    match chars[ci] {
                        '%' => { result.push('%'); ci += 1; }
                        'd' | 'i' => {
                            if arg_idx < args.len() {
                                let v = eval_expr_dynamic(&args[arg_idx], row, col_names)?;
                                arg_idx += 1;
                                match v {
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Real(r) => result.push_str(&(r as i64).to_string()),
                                    _ => result.push_str("0"),
                                }
                            }
                            ci += 1;
                        }
                        'f' => {
                            if arg_idx < args.len() {
                                let v = eval_expr_dynamic(&args[arg_idx], row, col_names)?;
                                arg_idx += 1;
                                match v {
                                    Value::Real(r) => result.push_str(&format!("{:.6}", r)),
                                    Value::Integer(i) => result.push_str(&format!("{:.6}", i as f64)),
                                    _ => result.push_str("0.000000"),
                                }
                            }
                            ci += 1;
                        }
                        's' => {
                            if arg_idx < args.len() {
                                let v = eval_expr_dynamic(&args[arg_idx], row, col_names)?;
                                arg_idx += 1;
                                match v {
                                    Value::Text(s) => result.push_str(&s),
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Real(r) => result.push_str(&r.to_string()),
                                    Value::Null => result.push_str("NULL"),
                                    Value::Blob(_) => result.push_str("(blob)"),
                                }
                            }
                            ci += 1;
                        }
                        _ => {
                            result.push('%');
                            result.push(chars[ci]);
                            ci += 1;
                        }
                    }
                } else {
                    result.push(chars[ci]);
                    ci += 1;
                }
            }
            Ok(Value::Text(result))
        }
        "QUOTE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            Ok(Value::Text(quote_value(&val)))
        }
        "UNICODE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr_dynamic(&args[0], row, col_names)?;
            match val {
                Value::Text(s) => {
                    match s.chars().next() {
                        Some(c) => Ok(Value::Integer(c as i64)),
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "CHAR" => {
            let mut result = String::new();
            for arg in args {
                let val = eval_expr_dynamic(arg, row, col_names)?;
                if let Value::Integer(code) = val {
                    if let Some(c) = char::from_u32(code as u32) {
                        result.push(c);
                    }
                }
            }
            Ok(Value::Text(result))
        }
        // -- Date/Time functions --
        "DATE" | "TIME" | "DATETIME" | "STRFTIME" | "JULIANDAY" => {
            let mut arg_values = Vec::new();
            for arg in args {
                arg_values.push(eval_expr_dynamic(arg, row, col_names)?);
            }
            Ok(eval_datetime_function(&upper, &arg_values))
        }
        _ => Err(HorizonError::NotImplemented(format!("function: {}", name))),
    }
}

// =========================================================================
// Date/Time helpers (no external dependencies -- pure manual parsing)
// =========================================================================

/// A parsed date-time value used internally by date/time functions.
#[derive(Debug, Clone)]
struct DateTime {
    year: i64,
    month: u32,  // 1..=12
    day: u32,    // 1..=31
    hour: u32,   // 0..=23
    minute: u32, // 0..=59
    second: u32, // 0..=59
    millisecond: u32, // 0..=999
}

impl DateTime {
    /// Format as 'YYYY-MM-DD'.
    fn format_date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// Format as 'HH:MM:SS'.
    fn format_time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    /// Format as 'YYYY-MM-DD HH:MM:SS'.
    fn format_datetime(&self) -> String {
        format!("{} {}", self.format_date(), self.format_time())
    }

    /// Compute Julian Day Number as f64.
    fn to_julian_day(&self) -> f64 {
        // Algorithm from Meeus "Astronomical Algorithms"
        let y = self.year as f64;
        let m = self.month as f64;
        let d = self.day as f64;

        let (y2, m2) = if m <= 2.0 {
            (y - 1.0, m + 12.0)
        } else {
            (y, m)
        };

        let a = (y2 / 100.0).floor();
        let b = 2.0 - a + (a / 4.0).floor();

        let jd = (365.25 * (y2 + 4716.0)).floor()
            + (30.6001 * (m2 + 1.0)).floor()
            + d
            + b
            - 1524.5;

        // Add time fraction
        let time_frac = (self.hour as f64) / 24.0
            + (self.minute as f64) / 1440.0
            + (self.second as f64) / 86400.0
            + (self.millisecond as f64) / 86400000.0;

        jd + time_frac
    }

    /// Apply a strftime format string.
    fn strftime(&self, fmt: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = fmt.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '%' && i + 1 < chars.len() {
                i += 1;
                match chars[i] {
                    'Y' => result.push_str(&format!("{:04}", self.year)),
                    'm' => result.push_str(&format!("{:02}", self.month)),
                    'd' => result.push_str(&format!("{:02}", self.day)),
                    'H' => result.push_str(&format!("{:02}", self.hour)),
                    'M' => result.push_str(&format!("{:02}", self.minute)),
                    'S' => result.push_str(&format!("{:02}", self.second)),
                    'f' => {
                        // Fractional seconds: SS.SSS
                        result.push_str(&format!("{:02}.{:03}", self.second, self.millisecond));
                    }
                    'j' => {
                        // Day of year (001-366)
                        let doy = day_of_year(self.year, self.month, self.day);
                        result.push_str(&format!("{:03}", doy));
                    }
                    'J' => {
                        // Julian day number
                        let jd = self.to_julian_day();
                        result.push_str(&format!("{:.6}", jd));
                    }
                    'w' => {
                        // Day of week 0=Sunday, 6=Saturday
                        let dow = day_of_week(self.year, self.month, self.day);
                        result.push_str(&format!("{}", dow));
                    }
                    'W' => {
                        // Week of year (00-53, Monday is the first day of the week)
                        let doy = day_of_year(self.year, self.month, self.day);
                        let dow = day_of_week(self.year, self.month, self.day);
                        // Convert Sunday=0 to Monday=0 based
                        let monday_dow = if dow == 0 { 6 } else { dow - 1 };
                        let week = (doy as i32 + 6 - monday_dow as i32) / 7;
                        result.push_str(&format!("{:02}", week));
                    }
                    's' => {
                        // Seconds since 1970-01-01 00:00:00 UTC
                        let epoch = DateTime {
                            year: 1970, month: 1, day: 1,
                            hour: 0, minute: 0, second: 0, millisecond: 0,
                        };
                        let diff_jd = self.to_julian_day() - epoch.to_julian_day();
                        let secs = (diff_jd * 86400.0) as i64;
                        result.push_str(&format!("{}", secs));
                    }
                    '%' => result.push('%'),
                    c => {
                        result.push('%');
                        result.push(c);
                    }
                }
            } else {
                result.push(chars[i]);
            }
            i += 1;
        }
        result
    }

    /// Add N days (can be negative).
    fn add_days(&mut self, n: i64) {
        if n >= 0 {
            for _ in 0..n {
                self.add_one_day();
            }
        } else {
            for _ in 0..(-n) {
                self.subtract_one_day();
            }
        }
    }

    /// Add N months (can be negative).
    fn add_months(&mut self, n: i64) {
        let total_months = self.year * 12 + (self.month as i64 - 1) + n;
        self.year = total_months.div_euclid(12);
        self.month = (total_months.rem_euclid(12) + 1) as u32;
        let max_day = days_in_month(self.year, self.month);
        if self.day > max_day {
            self.day = max_day;
        }
    }

    /// Add N years (can be negative).
    fn add_years(&mut self, n: i64) {
        self.year += n;
        let max_day = days_in_month(self.year, self.month);
        if self.day > max_day {
            self.day = max_day;
        }
    }

    fn add_one_day(&mut self) {
        self.day += 1;
        let max = days_in_month(self.year, self.month);
        if self.day > max {
            self.day = 1;
            self.month += 1;
            if self.month > 12 {
                self.month = 1;
                self.year += 1;
            }
        }
    }

    fn subtract_one_day(&mut self) {
        if self.day > 1 {
            self.day -= 1;
        } else {
            if self.month > 1 {
                self.month -= 1;
            } else {
                self.month = 12;
                self.year -= 1;
            }
            self.day = days_in_month(self.year, self.month);
        }
    }
}

/// Whether a year is a leap year.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Number of days in a month.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 => 31,
        2 => if is_leap_year(year) { 29 } else { 28 },
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        _ => 30,
    }
}

/// Day of year (1-based).
fn day_of_year(year: i64, month: u32, day: u32) -> u32 {
    let mut doy = 0u32;
    for m in 1..month {
        doy += days_in_month(year, m);
    }
    doy + day
}

/// Day of week: 0 = Sunday, 1 = Monday, ..., 6 = Saturday.
/// Uses Tomohiko Sakamoto's algorithm.
fn day_of_week(year: i64, month: u32, day: u32) -> u32 {
    let t = [0i64, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let y = if month < 3 { year - 1 } else { year };
    let result = (y + y / 4 - y / 100 + y / 400 + t[(month - 1) as usize] + day as i64) % 7;
    result.rem_euclid(7) as u32
}

/// Parse a time string into a DateTime. Supported formats:
/// - 'now'
/// - 'YYYY-MM-DD'
/// - 'YYYY-MM-DD HH:MM:SS'
/// - 'YYYY-MM-DD HH:MM:SS.SSS'
fn parse_timestring(s: &str) -> Option<DateTime> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("now") {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let total_secs = now.as_secs();
        let millis = now.subsec_millis();

        let total_days = (total_secs / 86400) as i64;
        let time_of_day = total_secs % 86400;
        let hour = (time_of_day / 3600) as u32;
        let minute = ((time_of_day % 3600) / 60) as u32;
        let second = (time_of_day % 60) as u32;

        let (year, month, day) = civil_from_days(total_days);

        return Some(DateTime {
            year, month, day, hour, minute, second,
            millisecond: millis,
        });
    }

    if s.len() < 10 {
        return None;
    }

    let year = dt_parse_i64(&s[0..4])?;
    if s.as_bytes().get(4) != Some(&b'-') { return None; }
    let month = dt_parse_u32(&s[5..7])?;
    if s.as_bytes().get(7) != Some(&b'-') { return None; }
    let day = dt_parse_u32(&s[8..10])?;

    if month < 1 || month > 12 { return None; }
    if day < 1 || day > days_in_month(year, month) { return None; }

    let mut dt = DateTime {
        year, month, day,
        hour: 0, minute: 0, second: 0, millisecond: 0,
    };

    if s.len() == 10 {
        return Some(dt);
    }

    let sep = s.as_bytes().get(10)?;
    if *sep != b' ' && *sep != b'T' { return None; }

    if s.len() < 19 { return None; }

    dt.hour = dt_parse_u32(&s[11..13])?;
    if s.as_bytes().get(13) != Some(&b':') { return None; }
    dt.minute = dt_parse_u32(&s[14..16])?;
    if s.as_bytes().get(16) != Some(&b':') { return None; }
    dt.second = dt_parse_u32(&s[17..19])?;

    if dt.hour > 23 || dt.minute > 59 || dt.second > 59 {
        return None;
    }

    if s.len() > 19 && s.as_bytes().get(19) == Some(&b'.') {
        let frac_str = &s[20..];
        let frac_digits = frac_str.len().min(3);
        if frac_digits > 0 {
            let mut ms = dt_parse_u32(&frac_str[..frac_digits])?;
            for _ in frac_digits..3 {
                ms *= 10;
            }
            dt.millisecond = ms;
        }
    }

    Some(dt)
}

/// Parse a modifier string and apply it to a DateTime.
fn apply_modifier(dt: &mut DateTime, modifier: &str) -> bool {
    let s = modifier.trim();

    if s.eq_ignore_ascii_case("start of month") {
        dt.day = 1;
        dt.hour = 0;
        dt.minute = 0;
        dt.second = 0;
        dt.millisecond = 0;
        return true;
    }

    if s.eq_ignore_ascii_case("start of year") {
        dt.month = 1;
        dt.day = 1;
        dt.hour = 0;
        dt.minute = 0;
        dt.second = 0;
        dt.millisecond = 0;
        return true;
    }

    if s.eq_ignore_ascii_case("start of day") {
        dt.hour = 0;
        dt.minute = 0;
        dt.second = 0;
        dt.millisecond = 0;
        return true;
    }

    // '+N days', '-N days', '+N months', '-N months', '+N years', '-N years'
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    if parts.len() == 2 {
        let unit = parts[1].to_lowercase();
        if let Some(n) = dt_parse_modifier_number(parts[0]) {
            match unit.as_str() {
                "days" | "day" => {
                    dt.add_days(n);
                    return true;
                }
                "months" | "month" => {
                    dt.add_months(n);
                    return true;
                }
                "years" | "year" => {
                    dt.add_years(n);
                    return true;
                }
                _ => {}
            }
        }
    }

    false
}

/// Parse a modifier number like "+5", "-3", "7".
fn dt_parse_modifier_number(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() { return None; }
    if s.starts_with('+') {
        dt_parse_i64(&s[1..])
    } else if s.starts_with('-') {
        dt_parse_i64(&s[1..]).map(|v| -v)
    } else {
        dt_parse_i64(s)
    }
}

/// Simple integer parser for date/time (no external deps).
fn dt_parse_i64(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() { return None; }
    let mut result: i64 = 0;
    for &b in s.as_bytes() {
        if !b.is_ascii_digit() { return None; }
        result = result.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(result)
}

/// Simple u32 parser for date/time.
fn dt_parse_u32(s: &str) -> Option<u32> {
    let v = dt_parse_i64(s)?;
    if v < 0 || v > u32::MAX as i64 { return None; }
    Some(v as u32)
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
/// Civil date algorithm from Howard Hinnant.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y_final = if m <= 2 { y + 1 } else { y };
    (y_final, m as u32, d as u32)
}

/// Evaluate a date/time function given its name and already-evaluated arguments.
fn eval_datetime_function(name: &str, arg_values: &[Value]) -> Value {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "DATE" => {
            if arg_values.is_empty() {
                return Value::Null;
            }
            let ts = match &arg_values[0] {
                Value::Text(s) => s.as_str(),
                _ => return Value::Null,
            };
            let mut dt = match parse_timestring(ts) {
                Some(d) => d,
                None => return Value::Null,
            };
            for arg in &arg_values[1..] {
                if let Value::Text(m) = arg {
                    if !apply_modifier(&mut dt, m) {
                        return Value::Null;
                    }
                } else {
                    return Value::Null;
                }
            }
            Value::Text(dt.format_date())
        }
        "TIME" => {
            if arg_values.is_empty() {
                return Value::Null;
            }
            let ts = match &arg_values[0] {
                Value::Text(s) => s.as_str(),
                _ => return Value::Null,
            };
            let mut dt = match parse_timestring(ts) {
                Some(d) => d,
                None => return Value::Null,
            };
            for arg in &arg_values[1..] {
                if let Value::Text(m) = arg {
                    if !apply_modifier(&mut dt, m) {
                        return Value::Null;
                    }
                } else {
                    return Value::Null;
                }
            }
            Value::Text(dt.format_time())
        }
        "DATETIME" => {
            if arg_values.is_empty() {
                return Value::Null;
            }
            let ts = match &arg_values[0] {
                Value::Text(s) => s.as_str(),
                _ => return Value::Null,
            };
            let mut dt = match parse_timestring(ts) {
                Some(d) => d,
                None => return Value::Null,
            };
            for arg in &arg_values[1..] {
                if let Value::Text(m) = arg {
                    if !apply_modifier(&mut dt, m) {
                        return Value::Null;
                    }
                } else {
                    return Value::Null;
                }
            }
            Value::Text(dt.format_datetime())
        }
        "STRFTIME" => {
            if arg_values.len() < 2 {
                return Value::Null;
            }
            let fmt = match &arg_values[0] {
                Value::Text(s) => s.clone(),
                _ => return Value::Null,
            };
            let ts = match &arg_values[1] {
                Value::Text(s) => s.as_str(),
                _ => return Value::Null,
            };
            let mut dt = match parse_timestring(ts) {
                Some(d) => d,
                None => return Value::Null,
            };
            for arg in &arg_values[2..] {
                if let Value::Text(m) = arg {
                    if !apply_modifier(&mut dt, m) {
                        return Value::Null;
                    }
                } else {
                    return Value::Null;
                }
            }
            Value::Text(dt.strftime(&fmt))
        }
        "JULIANDAY" => {
            if arg_values.is_empty() {
                return Value::Null;
            }
            let ts = match &arg_values[0] {
                Value::Text(s) => s.as_str(),
                _ => return Value::Null,
            };
            let mut dt = match parse_timestring(ts) {
                Some(d) => d,
                None => return Value::Null,
            };
            for arg in &arg_values[1..] {
                if let Value::Text(m) = arg {
                    if !apply_modifier(&mut dt, m) {
                        return Value::Null;
                    }
                } else {
                    return Value::Null;
                }
            }
            Value::Real(dt.to_julian_day())
        }
        _ => Value::Null,
    }
}

/// Return the SQL literal representation of a value (used by QUOTE function).
fn quote_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => {
            if r.fract() == 0.0 && r.is_finite() {
                format!("{:.1}", r)
            } else {
                r.to_string()
            }
        }
        Value::Text(s) => {
            // SQL string literal: single-quote delimited, escape ' as ''
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("X'{}'", hex)
        }
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
                "GROUP_CONCAT" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let separator = if args.len() >= 2 {
                        let sep_val = eval_expr_dynamic(&args[1], &group[0], col_names)?;
                        match sep_val {
                            Value::Text(s) => s,
                            _ => ",".to_string(),
                        }
                    } else {
                        ",".to_string()
                    };
                    let mut parts = Vec::new();
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        if !val.is_null() {
                            let s = match &val {
                                Value::Text(s) => s.clone(),
                                Value::Integer(i) => i.to_string(),
                                Value::Real(r) => {
                                    if r.fract() == 0.0 && r.is_finite() {
                                        format!("{:.1}", r)
                                    } else {
                                        r.to_string()
                                    }
                                }
                                _ => format!("{}", val),
                            };
                            parts.push(s);
                        }
                    }
                    if parts.is_empty() {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Text(parts.join(&separator)))
                    }
                }
                "TOTAL" => {
                    if args.is_empty() {
                        return Ok(Value::Real(0.0));
                    }
                    let mut sum: f64 = 0.0;
                    for row in group {
                        let val = eval_expr_dynamic(&args[0], row, col_names)?;
                        match val {
                            Value::Integer(i) => sum += i as f64,
                            Value::Real(r) => sum += r,
                            _ => {}
                        }
                    }
                    Ok(Value::Real(sum))
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
            let cmp = if let Some(coll) = extract_collation(&item.expr) {
                compare_with_collation(&val_a, &val_b, coll)
            } else {
                val_a.cmp(&val_b)
            };
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

// ---- UNIFIED SCAN WITH INDEX ----

/// Scan entries from a table using the best available method:
/// 1. Primary key seek (for INTEGER PK equality/range)
/// 2. Secondary index scan
/// 3. Full table scan (fallback)
fn scan_with_index(
    where_clause: Option<&Expr>,
    table_name: &str,
    table: &TableInfo,
    tree: &BTree,
    pool: &mut BufferPool,
    catalog: &Catalog,
) -> Result<Vec<crate::btree::BTreeEntry>> {
    if let Some(where_expr) = where_clause {
        if let Some(pk_entries) = try_pk_seek(where_expr, table, tree, pool)? {
            return Ok(pk_entries);
        }
        if let Some(index_entries) = try_index_scan(where_expr, table_name, table, pool, catalog)? {
            return Ok(index_entries);
        }
    }
    tree.scan_all(pool)
}

// ---- PRIMARY KEY SEEK ----

/// Attempt to use the primary key B+Tree directly for point lookups and range scans.
/// For INTEGER PRIMARY KEY tables, the data B+Tree key *is* the rowid, so we can
/// use BTree::search() for O(log n) exact lookups instead of scanning all rows.
fn try_pk_seek(
    where_clause: &Expr,
    table: &TableInfo,
    tree: &BTree,
    pool: &mut BufferPool,
) -> Result<Option<Vec<crate::btree::BTreeEntry>>> {
    let pk_idx = match table.pk_column {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let pk_col = &table.columns[pk_idx];
    if pk_col.affinity != DataType::Integer {
        return Ok(None);
    }
    let pk_name = &pk_col.name;

    // Try to extract a PK predicate from the WHERE clause
    if let Some(entries) = try_pk_predicate(where_clause, pk_name, tree, pool)? {
        return Ok(Some(entries));
    }

    // Try to extract PK = N from an AND chain (e.g. `id = 5 AND name = 'foo'`)
    if let Expr::BinaryOp { left, op: BinaryOp::And, right } = where_clause {
        if let Some(entries) = try_pk_predicate(left, pk_name, tree, pool)? {
            return Ok(Some(entries));
        }
        if let Some(entries) = try_pk_predicate(right, pk_name, tree, pool)? {
            return Ok(Some(entries));
        }
    }

    Ok(None)
}

/// Try to match a single expression against `pk_col = N` or range patterns.
fn try_pk_predicate(
    expr: &Expr,
    pk_name: &str,
    tree: &BTree,
    pool: &mut BufferPool,
) -> Result<Option<Vec<crate::btree::BTreeEntry>>> {
    let (col_name, op, val) = match extract_index_predicate(expr) {
        Some(p) => p,
        None => return Ok(None),
    };
    if !col_name.eq_ignore_ascii_case(pk_name) {
        // Also handle qualified names like "table.id"
        let unqualified = col_name.rsplit('.').next().unwrap_or(&col_name);
        if !unqualified.eq_ignore_ascii_case(pk_name) {
            return Ok(None);
        }
    }
    let rowid = match &val {
        Value::Integer(n) => *n,
        _ => return Ok(None),
    };

    match op {
        BinaryOp::Eq => {
            let key = rowid.to_be_bytes();
            if let Some(data) = tree.search(pool, &key)? {
                Ok(Some(vec![crate::btree::BTreeEntry { key: key.to_vec(), value: data }]))
            } else {
                Ok(Some(vec![]))
            }
        }
        BinaryOp::Gt => {
            let start_key = (rowid + 1).to_be_bytes();
            Ok(Some(tree.scan_from(pool, &start_key)?))
        }
        BinaryOp::GtEq => {
            let start_key = rowid.to_be_bytes();
            Ok(Some(tree.scan_from(pool, &start_key)?))
        }
        BinaryOp::Lt => {
            let end_key = rowid.to_be_bytes();
            // Rowids are positive, so [0u8; 8] (rowid 0) is before all valid entries
            let start_key = 0i64.to_be_bytes();
            Ok(Some(tree.scan_range(pool, &start_key, &end_key)?))
        }
        BinaryOp::LtEq => {
            let end_key = (rowid + 1).to_be_bytes();
            let start_key = 0i64.to_be_bytes();
            Ok(Some(tree.scan_range(pool, &start_key, &end_key)?))
        }
        _ => Ok(None),
    }
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

    // Fire BEFORE UPDATE triggers
    views_triggers::fire_triggers(&upd.table, &TriggerEventKind::Update, &TriggerTimingKind::Before, pool, catalog, txn_mgr)?;

    let entries = scan_with_index(upd.where_clause.as_ref(), &upd.table, &table, &tree, pool, catalog)?;
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

    // Fire AFTER UPDATE triggers
    views_triggers::fire_triggers(&upd.table, &TriggerEventKind::Update, &TriggerTimingKind::After, pool, catalog, txn_mgr)?;

    Ok(updated)
}

// ---- DELETE ----

/// Extract the search query string from a MATCH expression like:
/// `table_name MATCH 'query'`
fn extract_fts5_match_query(expr: &Expr, table_name: &str) -> Option<String> {
    match expr {
        Expr::Match { table, pattern } => {
            // table should be a column reference to the table name
            if let Expr::Column { name, .. } = table.as_ref() {
                if name.eq_ignore_ascii_case(table_name) {
                    if let Expr::Literal(LiteralValue::String(s)) = pattern.as_ref() {
                        return Some(s.clone());
                    }
                }
            }
            None
        }
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            // Check both sides for MATCH
            extract_fts5_match_query(left, table_name)
                .or_else(|| extract_fts5_match_query(right, table_name))
        }
        _ => None,
    }
}

/// Extract rowid from a `rowid = N` expression.
fn extract_rowid_eq(expr: &Expr) -> Option<i64> {
    if let Expr::BinaryOp { left, op: BinaryOp::Eq, right } = expr {
        if let Expr::Column { name, .. } = left.as_ref() {
            if name.eq_ignore_ascii_case("rowid") {
                if let Expr::Literal(LiteralValue::Integer(n)) = right.as_ref() {
                    return Some(*n);
                }
            }
        }
    }
    None
}

fn execute_fts5_delete(del: &DeleteStatement) -> Result<usize> {
    if let Some(ref where_clause) = del.where_clause {
        // Check for MATCH expression in WHERE clause
        if let Some(query_text) = extract_fts5_match_query(where_clause, &del.table) {
            return fts5::fts5_delete_matching(&del.table, &query_text);
        }
        // Check for rowid = N
        if let Some(rowid) = extract_rowid_eq(where_clause) {
            let deleted = fts5::fts5_delete(&del.table, rowid)?;
            return Ok(if deleted { 1 } else { 0 });
        }
        // For other WHERE clauses, do a scan and filter
        let all_rows = fts5::fts5_scan_all(&del.table)?;
        let columns = fts5::fts5_get_columns(&del.table)?;
        let mut deleted = 0;
        for (rowid, values) in &all_rows {
            // Evaluate the WHERE clause using dynamic evaluation
            let col_names: Vec<String> = columns.clone();
            if eval_expr_dynamic(where_clause, values, &col_names)?.to_bool() {
                fts5::fts5_delete(&del.table, *rowid)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    } else {
        // DELETE all
        fts5::fts5_delete_all(&del.table)
    }
}

fn execute_delete(
    del: &DeleteStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    // Check if this is an FTS5 virtual table
    if fts5::fts5_table_exists(&del.table) {
        return execute_fts5_delete(del);
    }

    // Fire BEFORE DELETE triggers
    views_triggers::fire_triggers(&del.table, &TriggerEventKind::Delete, &TriggerTimingKind::Before, pool, catalog, txn_mgr)?;

    let table = catalog.get_table(&del.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let entries = scan_with_index(del.where_clause.as_ref(), &del.table, &table, &tree, pool, catalog)?;
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

    // Fire AFTER DELETE triggers
    views_triggers::fire_triggers(&del.table, &TriggerEventKind::Delete, &TriggerTimingKind::After, pool, catalog, txn_mgr)?;

    Ok(deleted)
}

// ---- INSERT/UPDATE/DELETE with RETURNING ----

/// Execute an INSERT with RETURNING clause, returning the inserted rows.
fn execute_insert_returning(
    ins: &InsertStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<QueryResult> {
    let returning_cols = ins.returning.as_ref().unwrap();
    let table = catalog.get_table(&ins.table)?.clone();

    // Resolve output column names from the RETURNING clause
    let column_names = resolve_column_names(returning_cols, &table)?;
    let columns = Arc::new(column_names);

    let mut rows = Vec::new();

    // Execute the insert normally first (we re-use the insert logic)
    let mut tree = BTree::open(table.root_page);
    let mut next_rowid = table.next_rowid;
    let _txn_id = txn_mgr.auto_commit();

    for value_row in &ins.values {
        let col_order: Vec<usize> = if let Some(ref col_names) = ins.columns {
            col_names
                .iter()
                .map(|name| {
                    table.find_column_index(name).ok_or_else(|| {
                        HorizonError::ColumnNotFound(format!("{}.{}", ins.table, name))
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

        let mut row_values = vec![Value::Null; table.columns.len()];
        for (val_idx, &col_idx) in col_order.iter().enumerate() {
            let val = eval_expr(&value_row[val_idx], &[], &[], &table)?;
            let affinity = table.columns[col_idx].affinity;
            row_values[col_idx] = val.apply_affinity(affinity);
        }

        for (i, col) in table.columns.iter().enumerate() {
            if !col_order.contains(&i) && col.generated_expr.is_none() {
                if let Some(ref default_val) = col.default_value {
                    row_values[i] = default_val.clone();
                }
            }
        }

        // Evaluate STORED generated columns
        for (i, col) in table.columns.iter().enumerate() {
            if let Some(ref gen_expr) = col.generated_expr {
                if col.is_stored {
                    let val = eval_expr(gen_expr, &row_values, &table.columns, &table)?;
                    row_values[i] = val.apply_affinity(col.affinity);
                }
            }
        }

        let rowid = if let Some(pk_idx) = table.pk_column {
            if table.columns[pk_idx].affinity == DataType::Integer {
                match &row_values[pk_idx] {
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

        for (i, col) in table.columns.iter().enumerate() {
            if col.not_null && row_values[i].is_null() {
                return Err(HorizonError::ConstraintViolation(format!(
                    "NOT NULL constraint failed: {}.{}",
                    ins.table, col.name
                )));
            }
        }

        let key = rowid.to_be_bytes();
        let existing = tree.search(pool, &key)?;
        if let Some(ref old_value) = existing {
            if ins.or_replace {
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
            txn_mgr.record_undo(UndoEntry::Insert {
                table: ins.table.clone(),
                root_page: tree.root_page(),
                key: key.to_vec(),
            });
        }

        let row_data = serialize_row(&row_values);
        tree.insert(pool, &key, &row_data)?;

        // Fill virtual generated columns for RETURNING
        if table_has_virtual_columns(&table) {
            fill_virtual_columns(&mut row_values, &table)?;
        }

        // Project the RETURNING columns from the inserted row
        let projected = project_row_returning(returning_cols, &row_values, &table)?;
        rows.push(Row {
            columns: columns.clone(),
            values: projected,
        });
    }

    // Update catalog metadata
    let mut updated_table = catalog.get_table(&ins.table)?.clone();
    updated_table.next_rowid = next_rowid;
    if tree.root_page() != updated_table.root_page {
        updated_table.root_page = tree.root_page();
    }
    catalog.update_table_meta(pool, &ins.table, &updated_table)?;

    Ok(QueryResult { columns, rows })
}

/// Execute an UPDATE with RETURNING clause, returning the updated rows.
fn execute_update_returning(
    upd: &UpdateStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<QueryResult> {
    let returning_cols = upd.returning.as_ref().unwrap();
    let table = catalog.get_table(&upd.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let column_names = resolve_column_names(returning_cols, &table)?;
    let columns = Arc::new(column_names);

    let entries = scan_with_index(upd.where_clause.as_ref(), &upd.table, &table, &tree, pool, catalog)?;
    let mut rows = Vec::new();

    for entry in &entries {
        let mut row_values = deserialize_row(&entry.value, table.columns.len())?;

        if let Some(ref where_clause) = upd.where_clause {
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if !result.to_bool() {
                continue;
            }
        }

        txn_mgr.record_undo(UndoEntry::Update {
            table: upd.table.clone(),
            root_page: tree.root_page(),
            key: entry.key.clone(),
            old_value: entry.value.clone(),
        });

        for (col_name, expr) in &upd.assignments {
            let col_idx = table.find_column_index(col_name).ok_or_else(|| {
                HorizonError::ColumnNotFound(format!("{}.{}", upd.table, col_name))
            })?;
            let new_val = eval_expr(expr, &row_values, &table.columns, &table)?;
            let affinity = table.columns[col_idx].affinity;
            row_values[col_idx] = new_val.apply_affinity(affinity);
        }

        let row_data = serialize_row(&row_values);
        tree.insert(pool, &entry.key, &row_data)?;

        // Project the RETURNING columns from the updated row
        let projected = project_row_returning(returning_cols, &row_values, &table)?;
        rows.push(Row {
            columns: columns.clone(),
            values: projected,
        });
    }

    if tree.root_page() != table.root_page {
        let mut updated_table = table.clone();
        updated_table.root_page = tree.root_page();
        catalog.update_table_meta(pool, &upd.table, &updated_table)?;
    }

    Ok(QueryResult { columns, rows })
}

/// Execute a DELETE with RETURNING clause, returning the deleted rows.
fn execute_delete_returning(
    del: &DeleteStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<QueryResult> {
    let returning_cols = del.returning.as_ref().unwrap();
    let table = catalog.get_table(&del.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let column_names = resolve_column_names(returning_cols, &table)?;
    let columns = Arc::new(column_names);

    let entries = scan_with_index(del.where_clause.as_ref(), &del.table, &table, &tree, pool, catalog)?;
    let mut to_delete: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();

    for entry in &entries {
        let row_values = deserialize_row(&entry.value, table.columns.len())?;
        if let Some(ref where_clause) = del.where_clause {
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if !result.to_bool() {
                continue;
            }
        }
        to_delete.push((entry.key.clone(), row_values));
    }

    let mut rows = Vec::new();
    for (key, row_values) in &to_delete {
        // Project RETURNING columns BEFORE deleting
        let projected = project_row_returning(returning_cols, row_values, &table)?;
        rows.push(Row {
            columns: columns.clone(),
            values: projected,
        });

        // Find the serialized value for undo
        let row_data = serialize_row(row_values);
        txn_mgr.record_undo(UndoEntry::Delete {
            table: del.table.clone(),
            root_page: tree.root_page(),
            key: key.clone(),
            old_value: row_data,
        });
        tree.delete(pool, key)?;
    }

    if tree.root_page() != table.root_page {
        let mut updated_table = table.clone();
        updated_table.root_page = tree.root_page();
        catalog.update_table_meta(pool, &del.table, &updated_table)?;
    }

    Ok(QueryResult { columns, rows })
}

/// Project a row for RETURNING clause evaluation.
fn project_row_returning(
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
            let (gen_expr, gen_stored) = if let Some(ref gen) = col_def.generated {
                (Some(gen.expr.clone()), gen.stored)
            } else {
                (None, false)
            };
            let col_info = ColumnInfo {
                name: col_def.name.clone(), type_name, affinity,
                primary_key: false, autoincrement: false,
                not_null: col_def.not_null, unique: col_def.unique,
                default_value: col_def.default.as_ref().map(|e| eval_const_expr(e)),
                position,
                generated_expr: gen_expr,
                is_stored: gen_stored,
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
        "table_list" => {
            let columns = Arc::new(vec![
                "schema".into(), "name".into(), "type".into(),
                "ncol".into(), "wr".into(), "strict".into(),
            ]);
            let mut rows = Vec::new();
            let mut table_names = catalog.list_tables();
            table_names.sort();
            for tname in table_names {
                let table = catalog.get_table(tname)?;
                let ncol = table.columns.len() as i64;
                rows.push(Row { columns: columns.clone(), values: vec![
                    Value::Text("main".into()),
                    Value::Text(tname.to_string()),
                    Value::Text("table".into()),
                    Value::Integer(ncol),
                    Value::Integer(0),
                    Value::Integer(0),
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
        "index_info" => {
            let index_name = pragma_extract_table_name(pragma)?;
            let index = catalog.get_index(&index_name)?;
            let table = catalog.get_table(&index.table_name)?;
            let columns = Arc::new(vec!["seqno".into(), "cid".into(), "name".into()]);
            let mut rows = Vec::new();
            for (i, col_name) in index.columns.iter().enumerate() {
                let cid = table.find_column_index(col_name).unwrap_or(0) as i64;
                rows.push(Row { columns: columns.clone(), values: vec![
                    Value::Integer(i as i64), Value::Integer(cid), Value::Text(col_name.clone()),
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
        "journal_mode" => {
            let columns = Arc::new(vec!["journal_mode".into()]);
            Ok(QueryResult { columns: columns.clone(), rows: vec![Row { columns, values: vec![Value::Text("wal".into())] }] })
        }
        "encoding" => {
            let columns = Arc::new(vec!["encoding".into()]);
            Ok(QueryResult { columns: columns.clone(), rows: vec![Row { columns, values: vec![Value::Text("UTF-8".into())] }] })
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
    let columns = Arc::new(vec![
        "addr".to_string(), "opcode".to_string(),
        "p1".to_string(), "p2".to_string(), "p3".to_string(), "p4".to_string(),
    ]);
    let mut rows = Vec::new();
    let mut addr: i64 = 0;
    explain_plan_opcodes(&plan, &columns, &mut rows, &mut addr);
    Ok(QueryResult { columns, rows })
}

/// Generate opcode-style rows for EXPLAIN output by walking the logical plan.
fn explain_plan_opcodes(
    plan: &LogicalPlan,
    columns: &Arc<Vec<String>>,
    rows: &mut Vec<Row>,
    addr: &mut i64,
) {
    match plan {
        LogicalPlan::SeqScan { table, alias } => {
            let detail = alias.as_deref()
                .map(|a| format!("{} AS {}", table, a))
                .unwrap_or_else(|| table.clone());
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("OpenRead".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(detail),
            ] });
            *addr += 1;
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Rewind".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(format!("table {}", table)),
            ] });
            *addr += 1;
        }
        LogicalPlan::Filter { input, predicate } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Filter".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(format!("{:?}", predicate)),
            ] });
            *addr += 1;
        }
        LogicalPlan::Project { input, .. } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Column".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("ResultRow".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Sort { input, .. } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("SorterOpen".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("SorterSort".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Limit { input, .. } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Limit".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Aggregate { input, .. } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("AggStep".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("AggFinal".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Join { left, right, join_type, .. } => {
            explain_plan_opcodes(left, columns, rows, addr);
            explain_plan_opcodes(right, columns, rows, addr);
            let jt = match join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Cross => "CROSS JOIN",
            };
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Join".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(jt.into()),
            ] });
            *addr += 1;
        }
        LogicalPlan::Distinct { input } => {
            explain_plan_opcodes(input, columns, rows, addr);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Distinct".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Insert { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Insert".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(table.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::Update { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Update".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(table.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::Delete { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Delete".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(table.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::CreateTable(ct) => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("CreateTable".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(ct.name.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::DropTable(dt) => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("DropTable".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(dt.name.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::CreateIndex(ci) => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("CreateIndex".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(format!("{} ON {}", ci.name, ci.table)),
            ] });
            *addr += 1;
        }
        LogicalPlan::DropIndex(di) => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("DropIndex".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Text(di.name.clone()),
            ] });
            *addr += 1;
        }
        LogicalPlan::Begin => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("AutoCommit".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Commit | LogicalPlan::Rollback => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Halt".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
        LogicalPlan::Empty => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(*addr), Value::Text("Init".into()),
                Value::Integer(0), Value::Integer(0), Value::Integer(0), Value::Null,
            ] });
            *addr += 1;
        }
    }
    // Always add a Halt row at the end if we're at the top level (addr == total)
}

fn execute_explain_query_plan(inner_stmt: &Statement, catalog: &Catalog) -> Result<QueryResult> {
    let plan = plan_statement(inner_stmt, catalog)?;
    let columns = Arc::new(vec![
        "selectid".to_string(), "order".to_string(), "from".to_string(), "detail".to_string(),
    ]);
    let mut rows = Vec::new();
    let mut order: i64 = 0;
    eqp_walk(&plan, &columns, &mut rows, 0, &mut order);
    Ok(QueryResult { columns, rows })
}

/// Walk the logical plan tree and produce EXPLAIN QUERY PLAN rows.
fn eqp_walk(
    plan: &LogicalPlan,
    columns: &Arc<Vec<String>>,
    rows: &mut Vec<Row>,
    selectid: i64,
    order: &mut i64,
) {
    match plan {
        LogicalPlan::SeqScan { table, alias } => {
            let detail = if let Some(a) = alias {
                format!("SCAN TABLE {} AS {}", table, a)
            } else {
                format!("SCAN TABLE {}", table)
            };
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text(detail),
            ] });
            *order += 1;
        }
        LogicalPlan::Filter { input, .. } => {
            eqp_walk(input, columns, rows, selectid, order);
        }
        LogicalPlan::Project { input, .. } => {
            eqp_walk(input, columns, rows, selectid, order);
        }
        LogicalPlan::Sort { input, .. } => {
            eqp_walk(input, columns, rows, selectid, order);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text("USE TEMP B-TREE FOR ORDER BY".into()),
            ] });
            *order += 1;
        }
        LogicalPlan::Limit { input, .. } => {
            eqp_walk(input, columns, rows, selectid, order);
        }
        LogicalPlan::Aggregate { input, group_by, .. } => {
            eqp_walk(input, columns, rows, selectid, order);
            if !group_by.is_empty() {
                rows.push(Row { columns: columns.clone(), values: vec![
                    Value::Integer(selectid), Value::Integer(*order),
                    Value::Integer(0), Value::Text("USE TEMP B-TREE FOR GROUP BY".into()),
                ] });
                *order += 1;
            }
        }
        LogicalPlan::Join { left, right, join_type, .. } => {
            eqp_walk(left, columns, rows, selectid, order);
            eqp_walk(right, columns, rows, selectid, order);
            let _ = join_type; // join type is reflected in scan details
        }
        LogicalPlan::Distinct { input } => {
            eqp_walk(input, columns, rows, selectid, order);
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text("USE TEMP B-TREE FOR DISTINCT".into()),
            ] });
            *order += 1;
        }
        LogicalPlan::Insert { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text(format!("SCAN TABLE {}", table)),
            ] });
            *order += 1;
        }
        LogicalPlan::Update { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text(format!("SCAN TABLE {}", table)),
            ] });
            *order += 1;
        }
        LogicalPlan::Delete { table, .. } => {
            rows.push(Row { columns: columns.clone(), values: vec![
                Value::Integer(selectid), Value::Integer(*order),
                Value::Integer(0), Value::Text(format!("SCAN TABLE {}", table)),
            ] });
            *order += 1;
        }
        _ => {
            // DDL, Begin, Commit, Rollback, Empty -- no query plan detail
        }
    }
}

#[allow(dead_code)]
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
        Expr::WindowFunction { .. } => {
            Err(HorizonError::Internal(
                "window functions must be evaluated via the window execution path".into(),
            ))
        }
        Expr::Collate { expr: inner, .. } => {
            // Evaluate the inner expression, ignoring collation for now
            eval_expr(inner, row, columns, table)
        }
        Expr::Match { .. } => {
            // MATCH expressions are handled at the FTS5 query level.
            // If we reach here during row evaluation, the row was already matched.
            Ok(Value::Integer(1))
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
        Expr::InList { expr: inner, list, negated } => {
            let val = eval_expr_with_ctx(inner, row, columns, table, pool, catalog)?;
            // Check if the list contains a subquery (parser produces a single
            // Expr::Subquery item for `IN (SELECT ...)`)
            if list.len() == 1 {
                if let Expr::Subquery(subquery) = &list[0] {
                    let result = execute_select(subquery, pool, catalog)?;
                    let mut found = false;
                    for sub_row in &result.rows {
                        if let Some(sub_val) = sub_row.values.first() {
                            if val == *sub_val {
                                found = true;
                                break;
                            }
                        }
                    }
                    return Ok(Value::Integer(if found != *negated { 1 } else { 0 }));
                }
            }
            // Regular literal list
            let mut found = false;
            for item in list {
                let item_val = eval_expr_with_ctx(item, row, columns, table, pool, catalog)?;
                if val == item_val {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if found != *negated { 1 } else { 0 }))
        }
        Expr::Between { expr: inner, low, high, negated } => {
            let val = eval_expr_with_ctx(inner, row, columns, table, pool, catalog)?;
            let lo = eval_expr_with_ctx(low, row, columns, table, pool, catalog)?;
            let hi = eval_expr_with_ctx(high, row, columns, table, pool, catalog)?;
            let in_range = val >= lo && val <= hi;
            Ok(Value::Integer(if in_range != *negated { 1 } else { 0 }))
        }
        Expr::Like { expr: inner, pattern, negated } => {
            let val = eval_expr_with_ctx(inner, row, columns, table, pool, catalog)?;
            let pat = eval_expr_with_ctx(pattern, row, columns, table, pool, catalog)?;
            let matches = match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => sql_like_match(s, p),
                _ => false,
            };
            Ok(Value::Integer(if matches != *negated { 1 } else { 0 }))
        }
        // For expressions that don't contain subqueries (literals, columns, functions, casts, etc.),
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
        Expr::InList { expr: inner, list, negated } => {
            let val = eval_expr_dynamic_with_ctx(inner, row, col_names, pool, catalog)?;
            // Check if the list contains a subquery (parser produces a single
            // Expr::Subquery item for `IN (SELECT ...)`)
            if list.len() == 1 {
                if let Expr::Subquery(subquery) = &list[0] {
                    let result = execute_select(subquery, pool, catalog)?;
                    let mut found = false;
                    for sub_row in &result.rows {
                        if let Some(sub_val) = sub_row.values.first() {
                            if val == *sub_val {
                                found = true;
                                break;
                            }
                        }
                    }
                    return Ok(Value::Integer(if found != *negated { 1 } else { 0 }));
                }
            }
            // Regular literal list
            let mut found = false;
            for item in list {
                let item_val = eval_expr_dynamic_with_ctx(item, row, col_names, pool, catalog)?;
                if val == item_val {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if found != *negated { 1 } else { 0 }))
        }
        Expr::Between { expr: inner, low, high, negated } => {
            let val = eval_expr_dynamic_with_ctx(inner, row, col_names, pool, catalog)?;
            let lo = eval_expr_dynamic_with_ctx(low, row, col_names, pool, catalog)?;
            let hi = eval_expr_dynamic_with_ctx(high, row, col_names, pool, catalog)?;
            let in_range = val >= lo && val <= hi;
            Ok(Value::Integer(if in_range != *negated { 1 } else { 0 }))
        }
        Expr::Like { expr: inner, pattern, negated } => {
            let val = eval_expr_dynamic_with_ctx(inner, row, col_names, pool, catalog)?;
            let pat = eval_expr_dynamic_with_ctx(pattern, row, col_names, pool, catalog)?;
            let matches = match (&val, &pat) {
                (Value::Text(s), Value::Text(p)) => sql_like_match(s, p),
                _ => false,
            };
            Ok(Value::Integer(if matches != *negated { 1 } else { 0 }))
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

/// Compare two values using the specified collation sequence.
/// Supported collations: BINARY (default), NOCASE (case-insensitive), RTRIM (ignore trailing spaces).
fn compare_with_collation(a: &Value, b: &Value, collation: &str) -> std::cmp::Ordering {
    let coll_upper = collation.to_uppercase();
    match coll_upper.as_str() {
        "NOCASE" => {
            // Case-insensitive comparison for text values
            match (a, b) {
                (Value::Text(sa), Value::Text(sb)) => {
                    let la = sa.to_lowercase();
                    let lb = sb.to_lowercase();
                    la.cmp(&lb)
                }
                _ => a.cmp(b),
            }
        }
        "RTRIM" => {
            // Ignore trailing spaces for text values
            match (a, b) {
                (Value::Text(sa), Value::Text(sb)) => {
                    let ta = sa.trim_end();
                    let tb = sb.trim_end();
                    ta.cmp(tb)
                }
                _ => a.cmp(b),
            }
        }
        _ => {
            // BINARY or unknown — default comparison
            a.cmp(b)
        }
    }
}

/// Extract collation name from an expression if it has one.
fn extract_collation(expr: &Expr) -> Option<&str> {
    if let Expr::Collate { collation, .. } = expr {
        Some(collation.as_str())
    } else {
        None
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
        // -- JSON functions --
        "JSON" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => Ok(Value::Text(jv.to_json_string())),
                        None => Err(HorizonError::InvalidSql("malformed JSON".into())),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Err(HorizonError::InvalidSql("JSON() requires a text argument".into())),
            }
        }
        "JSON_EXTRACT" => {
            if args.len() < 2 { return Ok(Value::Null); }
            let json_val = eval_expr(&args[0], row, columns, table)?;
            let path_val = eval_expr(&args[1], row, columns, table)?;
            match (json_val, path_val) {
                (Value::Text(s), Value::Text(path)) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            match jv.extract_path(&path) {
                                Some(extracted) => Ok(json::json_value_to_sql(extracted)),
                                None => Ok(Value::Null),
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        "JSON_TYPE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let json_val = eval_expr(&args[0], row, columns, table)?;
            match json_val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            if args.len() >= 2 {
                                let path_val = eval_expr(&args[1], row, columns, table)?;
                                if let Value::Text(path) = path_val {
                                    match jv.extract_path(&path) {
                                        Some(extracted) => Ok(Value::Text(extracted.json_type_name().to_string())),
                                        None => Ok(Value::Null),
                                    }
                                } else {
                                    Ok(Value::Null)
                                }
                            } else {
                                Ok(Value::Text(jv.json_type_name().to_string()))
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "JSON_ARRAY" => {
            let mut items = Vec::new();
            for arg in args {
                let val = eval_expr(arg, row, columns, table)?;
                items.push(json::sql_value_to_json(&val));
            }
            let arr = json::JsonValue::Array(items);
            Ok(Value::Text(arr.to_json_string()))
        }
        "JSON_OBJECT" => {
            if args.len() % 2 != 0 {
                return Err(HorizonError::InvalidSql(
                    "JSON_OBJECT requires an even number of arguments".into(),
                ));
            }
            let mut pairs = Vec::new();
            let mut i = 0;
            while i < args.len() {
                let key_val = eval_expr(&args[i], row, columns, table)?;
                let val_val = eval_expr(&args[i + 1], row, columns, table)?;
                let key = match key_val {
                    Value::Text(s) => s,
                    Value::Integer(n) => n.to_string(),
                    Value::Real(r) => r.to_string(),
                    Value::Null => "null".to_string(),
                    Value::Blob(_) => return Err(HorizonError::InvalidSql("JSON_OBJECT keys must be text".into())),
                };
                pairs.push((key, json::sql_value_to_json(&val_val)));
                i += 2;
            }
            let obj = json::JsonValue::Object(pairs);
            Ok(Value::Text(obj.to_json_string()))
        }
        "JSON_ARRAY_LENGTH" => {
            if args.is_empty() { return Ok(Value::Null); }
            let json_val = eval_expr(&args[0], row, columns, table)?;
            match json_val {
                Value::Text(s) => {
                    match json::JsonParser::parse(&s) {
                        Some(jv) => {
                            let target = if args.len() >= 2 {
                                let path_val = eval_expr(&args[1], row, columns, table)?;
                                if let Value::Text(path) = path_val {
                                    match jv.extract_path(&path) {
                                        Some(v) => v.clone(),
                                        None => return Ok(Value::Null),
                                    }
                                } else {
                                    jv
                                }
                            } else {
                                jv
                            };
                            match target.array_length() {
                                Some(len) => Ok(Value::Integer(len as i64)),
                                None => Ok(Value::Null),
                            }
                        }
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "JSON_VALID" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => {
                    Ok(Value::Integer(if json::JsonParser::parse(&s).is_some() { 1 } else { 0 }))
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Integer(0)),
            }
        }
        // -- Additional utility functions --
        "PRINTF" => {
            if args.is_empty() { return Ok(Value::Null); }
            let fmt_val = eval_expr(&args[0], row, columns, table)?;
            let fmt_str = match fmt_val {
                Value::Text(s) => s,
                _ => return Ok(Value::Null),
            };
            let mut result = String::new();
            let chars: Vec<char> = fmt_str.chars().collect();
            let mut ci = 0;
            let mut arg_idx = 1;
            while ci < chars.len() {
                if chars[ci] == '%' && ci + 1 < chars.len() {
                    ci += 1;
                    match chars[ci] {
                        '%' => { result.push('%'); ci += 1; }
                        'd' | 'i' => {
                            if arg_idx < args.len() {
                                let v = eval_expr(&args[arg_idx], row, columns, table)?;
                                arg_idx += 1;
                                match v {
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Real(r) => result.push_str(&(r as i64).to_string()),
                                    _ => result.push_str("0"),
                                }
                            }
                            ci += 1;
                        }
                        'f' => {
                            if arg_idx < args.len() {
                                let v = eval_expr(&args[arg_idx], row, columns, table)?;
                                arg_idx += 1;
                                match v {
                                    Value::Real(r) => result.push_str(&format!("{:.6}", r)),
                                    Value::Integer(i) => result.push_str(&format!("{:.6}", i as f64)),
                                    _ => result.push_str("0.000000"),
                                }
                            }
                            ci += 1;
                        }
                        's' => {
                            if arg_idx < args.len() {
                                let v = eval_expr(&args[arg_idx], row, columns, table)?;
                                arg_idx += 1;
                                match v {
                                    Value::Text(s) => result.push_str(&s),
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Real(r) => result.push_str(&r.to_string()),
                                    Value::Null => result.push_str("NULL"),
                                    Value::Blob(_) => result.push_str("(blob)"),
                                }
                            }
                            ci += 1;
                        }
                        _ => {
                            result.push('%');
                            result.push(chars[ci]);
                            ci += 1;
                        }
                    }
                } else {
                    result.push(chars[ci]);
                    ci += 1;
                }
            }
            Ok(Value::Text(result))
        }
        "QUOTE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            Ok(Value::Text(quote_value(&val)))
        }
        "UNICODE" => {
            if args.is_empty() { return Ok(Value::Null); }
            let val = eval_expr(&args[0], row, columns, table)?;
            match val {
                Value::Text(s) => {
                    match s.chars().next() {
                        Some(c) => Ok(Value::Integer(c as i64)),
                        None => Ok(Value::Null),
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "CHAR" => {
            let mut result = String::new();
            for arg in args {
                let val = eval_expr(arg, row, columns, table)?;
                if let Value::Integer(code) = val {
                    if let Some(c) = char::from_u32(code as u32) {
                        result.push(c);
                    }
                }
            }
            Ok(Value::Text(result))
        }
        // -- Date/Time functions --
        "DATE" | "TIME" | "DATETIME" | "STRFTIME" | "JULIANDAY" => {
            let mut arg_values = Vec::new();
            for arg in args {
                arg_values.push(eval_expr(arg, row, columns, table)?);
            }
            Ok(eval_datetime_function(&upper, &arg_values))
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
            let cmp = if let Some(coll) = extract_collation(&item.expr) {
                compare_with_collation(&val_a, &val_b, coll)
            } else {
                val_a.cmp(&val_b)
            };
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
