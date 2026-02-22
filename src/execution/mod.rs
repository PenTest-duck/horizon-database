//! # Execution Engine
//!
//! Executes SQL statements against the storage layer. This module bridges
//! the SQL parser/planner with the B+Tree storage, catalog, and MVCC layers.

use std::sync::Arc;
use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, ColumnInfo, TableInfo};
use crate::error::{HorizonError, Result};
use crate::mvcc::TransactionManager;
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
        Statement::DropIndex(di) => execute_drop_index(di, catalog),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            // Transaction control — handled at a higher level for now
            Ok(0)
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
        _ => Err(HorizonError::Internal("execute_query requires a SELECT statement".into())),
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

        // Check for duplicate primary key
        if !ins.or_replace {
            let key = rowid.to_be_bytes();
            if tree.search(pool, &key)?.is_some() {
                return Err(HorizonError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {}.rowid",
                    ins.table
                )));
            }
        }

        // Serialize row values
        let row_data = serialize_row(&row_values);

        // Insert into B+Tree: key = rowid (big-endian i64), value = serialized row
        let key = rowid.to_be_bytes();
        tree.insert(pool, &key, &row_data)?;

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
    // Get the table to scan
    let (table_name, _alias) = match &select.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        Some(FromClause::Join { .. }) => {
            return Err(HorizonError::NotImplemented("JOINs".into()));
        }
        Some(FromClause::Subquery { .. }) => {
            return Err(HorizonError::NotImplemented("subqueries in FROM".into()));
        }
        None => {
            // SELECT without FROM — evaluate expressions directly
            return execute_select_no_from(select);
        }
    };

    let table = catalog.get_table(&table_name)?.clone();
    let tree = BTree::open(table.root_page);

    // Scan all entries from the B+Tree
    let entries = tree.scan_all(pool)?;

    // Determine output column names
    let column_names = resolve_column_names(&select.columns, &table)?;
    let columns = Arc::new(column_names);

    let mut rows = Vec::new();

    for entry in &entries {
        // Deserialize row values
        let row_values = deserialize_row(&entry.value, table.columns.len())?;

        // Evaluate WHERE clause
        if let Some(ref where_clause) = select.where_clause {
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if !result.to_bool() {
                continue;
            }
        }

        // Project columns
        let projected = project_row(&select.columns, &row_values, &table)?;

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

// ---- UPDATE ----

fn execute_update(
    upd: &UpdateStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    _txn_mgr: &mut TransactionManager,
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
    _txn_mgr: &mut TransactionManager,
) -> Result<usize> {
    let table = catalog.get_table(&del.table)?.clone();
    let mut tree = BTree::open(table.root_page);

    let entries = tree.scan_all(pool)?;
    let mut to_delete = Vec::new();

    for entry in &entries {
        if let Some(ref where_clause) = del.where_clause {
            let row_values = deserialize_row(&entry.value, table.columns.len())?;
            let result = eval_expr(where_clause, &row_values, &table.columns, &table)?;
            if result.to_bool() {
                to_delete.push(entry.key.clone());
            }
        } else {
            to_delete.push(entry.key.clone());
        }
    }

    let deleted = to_delete.len();
    for key in to_delete {
        tree.delete(pool, &key)?;
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

    // Verify the table exists
    let _table = catalog.get_table(&ci.table)?;

    // Create a B+Tree for the index
    let tree = BTree::create(pool)?;

    let index_info = crate::catalog::IndexInfo {
        name: ci.name.clone(),
        table_name: ci.table.clone(),
        columns: ci.columns.iter().map(|c| {
            if let Expr::Column { name, .. } = &c.expr {
                name.clone()
            } else {
                format!("{:?}", c.expr)
            }
        }).collect(),
        unique: ci.unique,
        root_page: tree.root_page(),
    };

    catalog.create_index(pool, index_info)?;
    Ok(0)
}

// ---- DROP INDEX ----

fn execute_drop_index(
    _di: &DropIndexStatement,
    _catalog: &mut Catalog,
) -> Result<usize> {
    // TODO: implement index removal from catalog
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
pub fn deserialize_row(data: &[u8], expected_cols: usize) -> Result<Vec<Value>> {
    if data.len() < 2 {
        return Err(HorizonError::CorruptDatabase("row data too short".into()));
    }
    let col_count = u16::from_be_bytes(data[0..2].try_into().unwrap()) as usize;
    if col_count != expected_cols {
        return Err(HorizonError::CorruptDatabase(format!(
            "expected {} columns but row has {}",
            expected_cols, col_count
        )));
    }

    let mut offset = 2;
    let mut values = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let (val, consumed) = Value::deserialize(&data[offset..])?;
        values.push(val);
        offset += consumed;
    }

    Ok(values)
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
        Expr::BinaryOp { left, op, right } => {
            let l = eval_const_expr(left);
            let r = eval_const_expr(right);
            eval_binary_op(&l, op, &r)
        }
        _ => Value::Null,
    }
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
            Err(HorizonError::NotImplemented("subqueries in expressions".into()))
        }
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
        "MAX" | "MIN" | "COUNT" | "SUM" | "AVG" | "TOTAL" | "GROUP_CONCAT" => {
            // These are aggregate functions — evaluated per-row they just return the value
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
                    let result = if let Some(l) = len {
                        s.chars().skip(start_idx).take(l as usize).collect::<String>()
                    } else {
                        s.chars().skip(start_idx).collect::<String>()
                    };
                    Ok(Value::Text(result))
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
