//! R-tree spatial indexing support for Horizon DB.
//!
//! Provides a simplified R-tree virtual table compatible with SQLite's rtree
//! extension. Entries are stored in a B+Tree keyed by integer rowid.
//! Each entry contains the id and coordinate pairs (min/max for each dimension).

use std::sync::Arc;

use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, RTreeInfo};
use crate::error::{HorizonError, Result};
use crate::sql::ast::*;
use crate::types::Value;
use crate::{QueryResult, Row};

/// An R-tree entry: id plus coordinate pairs.
#[derive(Debug, Clone)]
pub struct RTreeEntry {
    pub id: i64,
    pub coords: Vec<f64>,
}

impl RTreeEntry {
    /// Serialize an R-tree entry to bytes.
    ///
    /// Format: [id: 8 bytes BE] [num_coords: 2 bytes BE] [coord0: 8 bytes BE] ...
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 2 + self.coords.len() * 8);
        buf.extend_from_slice(&self.id.to_be_bytes());
        buf.extend_from_slice(&(self.coords.len() as u16).to_be_bytes());
        for &c in &self.coords {
            buf.extend_from_slice(&c.to_be_bytes());
        }
        buf
    }

    /// Deserialize an R-tree entry from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 10 {
            return Err(HorizonError::CorruptDatabase(
                "rtree entry too short".into(),
            ));
        }
        let id = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let num_coords = u16::from_be_bytes(data[8..10].try_into().unwrap()) as usize;
        let expected = 10 + num_coords * 8;
        if data.len() < expected {
            return Err(HorizonError::CorruptDatabase(
                "rtree entry truncated".into(),
            ));
        }
        let mut coords = Vec::with_capacity(num_coords);
        for i in 0..num_coords {
            let offset = 10 + i * 8;
            let c = f64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
            coords.push(c);
        }
        Ok(RTreeEntry { id, coords })
    }
}

// ---------------------------------------------------------------------------
// CREATE VIRTUAL TABLE ... USING rtree(...)
// ---------------------------------------------------------------------------

/// Execute a CREATE VIRTUAL TABLE ... USING rtree(...) statement.
pub fn execute_create_virtual_table_rtree(
    stmt: &CreateVirtualTableStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    if stmt.module_name.to_lowercase() != "rtree" {
        return Err(HorizonError::InvalidSql(format!(
            "unknown virtual table module: {}",
            stmt.module_name
        )));
    }

    if stmt.if_not_exists && catalog.rtree_exists(&stmt.name) {
        return Ok(0);
    }

    let args = &stmt.module_args;

    // Validate: first arg is id column, then pairs of min/max columns.
    // So total args must be odd and >= 3 (id + at least 1 dimension = 3 columns).
    if args.len() < 3 || args.len() % 2 != 1 {
        return Err(HorizonError::InvalidSql(
            "rtree virtual table requires an id column followed by pairs of min/max coordinate columns".into(),
        ));
    }

    let num_dimensions = (args.len() - 1) / 2;
    if num_dimensions > 5 {
        return Err(HorizonError::InvalidSql(
            "rtree virtual table supports at most 5 dimensions".into(),
        ));
    }

    // Create a B+Tree for storage
    let tree = BTree::create(pool)?;
    let root_page = tree.root_page();

    let rtree_info = RTreeInfo {
        name: stmt.name.clone(),
        column_names: args.clone(),
        num_dimensions,
        root_page,
    };

    catalog.create_rtree(rtree_info)?;
    Ok(0)
}

// ---------------------------------------------------------------------------
// INSERT INTO rtree
// ---------------------------------------------------------------------------

/// Execute an INSERT into an R-tree virtual table.
pub fn execute_rtree_insert(
    ins: &InsertStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    let rtree = catalog.get_rtree(&ins.table)
        .ok_or_else(|| HorizonError::TableNotFound(ins.table.clone()))?
        .clone();

    let expected_cols = rtree.column_names.len();
    let mut tree = BTree::open(rtree.root_page);
    let mut inserted = 0;

    for value_row in &ins.values {
        if value_row.len() != expected_cols {
            return Err(HorizonError::InvalidSql(format!(
                "rtree INSERT: expected {} values but got {}",
                expected_cols,
                value_row.len()
            )));
        }

        // Evaluate each expression to a value
        let mut vals = Vec::with_capacity(expected_cols);
        for expr in value_row {
            vals.push(eval_const_expr_rtree(expr)?);
        }

        // First value is the integer id
        let id = match &vals[0] {
            Value::Integer(i) => *i,
            Value::Real(r) => *r as i64,
            _ => {
                return Err(HorizonError::InvalidSql(
                    "rtree id must be an integer".into(),
                ));
            }
        };

        // Remaining values are coordinate pairs (must be real/numeric)
        let mut coords = Vec::with_capacity(expected_cols - 1);
        for val in &vals[1..] {
            let f = match val {
                Value::Real(r) => *r,
                Value::Integer(i) => *i as f64,
                _ => {
                    return Err(HorizonError::InvalidSql(
                        "rtree coordinates must be numeric".into(),
                    ));
                }
            };
            coords.push(f);
        }

        // Validate min <= max for each dimension
        let num_dims = rtree.num_dimensions;
        for d in 0..num_dims {
            let min_val = coords[d * 2];
            let max_val = coords[d * 2 + 1];
            if min_val > max_val {
                return Err(HorizonError::InvalidSql(format!(
                    "rtree constraint: {} must be <= {} (dimension {})",
                    rtree.column_names[1 + d * 2],
                    rtree.column_names[2 + d * 2],
                    d
                )));
            }
        }

        let entry = RTreeEntry { id, coords };
        let key = id.to_be_bytes();
        let value = entry.serialize();
        tree.insert(pool, &key, &value)?;
        inserted += 1;
    }

    // Update root page if it changed
    if tree.root_page() != rtree.root_page {
        if let Some(rt) = catalog.get_rtree_mut(&ins.table) {
            rt.root_page = tree.root_page();
        }
    }

    Ok(inserted)
}

// ---------------------------------------------------------------------------
// DELETE FROM rtree
// ---------------------------------------------------------------------------

/// Execute a DELETE from an R-tree virtual table.
pub fn execute_rtree_delete(
    del: &DeleteStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<usize> {
    let rtree = catalog.get_rtree(&del.table)
        .ok_or_else(|| HorizonError::TableNotFound(del.table.clone()))?
        .clone();

    let mut tree = BTree::open(rtree.root_page);
    let entries = tree.scan_all(pool)?;
    let mut to_delete: Vec<Vec<u8>> = Vec::new();

    for entry in &entries {
        let rtree_entry = RTreeEntry::deserialize(&entry.value)?;
        let row_values = rtree_entry_to_values(&rtree_entry, &rtree);

        if let Some(ref where_clause) = del.where_clause {
            let result = eval_rtree_where(where_clause, &row_values, &rtree.column_names)?;
            if result {
                to_delete.push(entry.key.clone());
            }
        } else {
            to_delete.push(entry.key.clone());
        }
    }

    let deleted = to_delete.len();
    for key in &to_delete {
        tree.delete(pool, key)?;
    }

    // Update root page if it changed
    if tree.root_page() != rtree.root_page {
        if let Some(rt) = catalog.get_rtree_mut(&del.table) {
            rt.root_page = tree.root_page();
        }
    }

    Ok(deleted)
}

// ---------------------------------------------------------------------------
// SELECT FROM rtree
// ---------------------------------------------------------------------------

/// Execute a SELECT from an R-tree virtual table.
pub fn execute_rtree_select(
    select: &SelectStatement,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
) -> Result<QueryResult> {
    let table_name = match &select.from {
        Some(FromClause::Table { name, .. }) => name.clone(),
        _ => {
            return Err(HorizonError::Internal(
                "rtree select requires a table FROM clause".into(),
            ));
        }
    };

    let rtree = catalog.get_rtree(&table_name)
        .ok_or_else(|| HorizonError::TableNotFound(table_name.clone()))?
        .clone();

    let tree = BTree::open(rtree.root_page);
    let entries = tree.scan_all(pool)?;

    // Resolve column names for the output
    let output_col_names = resolve_rtree_columns(&select.columns, &rtree)?;
    let columns = Arc::new(output_col_names);

    let mut rows = Vec::new();

    for entry in &entries {
        let rtree_entry = RTreeEntry::deserialize(&entry.value)?;
        let row_values = rtree_entry_to_values(&rtree_entry, &rtree);

        // Apply WHERE filter
        let passes = if let Some(ref where_clause) = select.where_clause {
            eval_rtree_where(where_clause, &row_values, &rtree.column_names)?
        } else {
            true
        };

        if passes {
            // Project columns
            let projected = project_rtree_row(&select.columns, &row_values, &rtree)?;
            rows.push(Row {
                columns: columns.clone(),
                values: projected,
            });
        }
    }

    // Apply ORDER BY
    if !select.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for item in &select.order_by {
                if let Expr::Column { ref name, .. } = item.expr {
                    let col_idx = columns.iter().position(|c| c.eq_ignore_ascii_case(name));
                    if let Some(idx) = col_idx {
                        let cmp = a.values[idx].cmp(&b.values[idx]);
                        let cmp = if item.desc { cmp.reverse() } else { cmp };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Apply LIMIT / OFFSET
    if let Some(ref offset_expr) = select.offset {
        let offset = eval_const_i64(offset_expr).unwrap_or(0) as usize;
        if offset < rows.len() {
            rows = rows.into_iter().skip(offset).collect();
        } else {
            rows.clear();
        }
    }
    if let Some(ref limit_expr) = select.limit {
        let limit = eval_const_i64(limit_expr).unwrap_or(i64::MAX) as usize;
        rows.truncate(limit);
    }

    Ok(QueryResult { columns, rows })
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Convert an R-tree entry to a row of Values matching the column definitions.
fn rtree_entry_to_values(entry: &RTreeEntry, rtree: &RTreeInfo) -> Vec<Value> {
    let mut values = Vec::with_capacity(rtree.column_names.len());
    values.push(Value::Integer(entry.id));
    for &c in &entry.coords {
        values.push(Value::Real(c));
    }
    values
}

/// Resolve column names for the SELECT output.
fn resolve_rtree_columns(
    select_cols: &[SelectColumn],
    rtree: &RTreeInfo,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns => {
                names.extend(rtree.column_names.iter().cloned());
            }
            SelectColumn::TableAllColumns(_) => {
                names.extend(rtree.column_names.iter().cloned());
            }
            SelectColumn::Expr { expr, alias } => {
                if let Some(ref a) = alias {
                    names.push(a.clone());
                } else if let Expr::Column { ref name, .. } = expr {
                    names.push(name.clone());
                } else {
                    names.push(format!("{:?}", expr));
                }
            }
        }
    }
    Ok(names)
}

/// Project selected columns from an R-tree row.
fn project_rtree_row(
    select_cols: &[SelectColumn],
    row_values: &[Value],
    rtree: &RTreeInfo,
) -> Result<Vec<Value>> {
    let mut projected = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::AllColumns | SelectColumn::TableAllColumns(_) => {
                projected.extend_from_slice(row_values);
            }
            SelectColumn::Expr { expr, .. } => {
                let val = eval_rtree_expr(expr, row_values, &rtree.column_names)?;
                projected.push(val);
            }
        }
    }
    Ok(projected)
}

/// Evaluate a WHERE clause expression against an R-tree row.
/// Returns true if the row passes the filter.
fn eval_rtree_where(
    expr: &Expr,
    row_values: &[Value],
    column_names: &[String],
) -> Result<bool> {
    let val = eval_rtree_expr(expr, row_values, column_names)?;
    Ok(val.to_bool())
}

/// Evaluate an expression in the context of an R-tree row.
fn eval_rtree_expr(
    expr: &Expr,
    row_values: &[Value],
    column_names: &[String],
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Column { name, .. } => {
            let idx = column_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    HorizonError::ColumnNotFound(name.clone())
                })?;
            Ok(row_values[idx].clone())
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_rtree_expr(left, row_values, column_names)?;
            let right_val = eval_rtree_expr(right, row_values, column_names)?;
            eval_binary_op(&left_val, op, &right_val)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_rtree_expr(inner, row_values, column_names)?;
            match op {
                UnaryOp::Neg => match val {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(r) => Ok(Value::Real(-r)),
                    _ => Ok(Value::Null),
                },
                UnaryOp::Not => Ok(Value::Integer(if val.to_bool() { 0 } else { 1 })),
                UnaryOp::BitNot => match val {
                    Value::Integer(i) => Ok(Value::Integer(!i)),
                    _ => Ok(Value::Null),
                },
            }
        }
        Expr::IsNull { expr: inner, negated } => {
            let val = eval_rtree_expr(inner, row_values, column_names)?;
            let is_null = val.is_null();
            if *negated {
                Ok(Value::Integer(if !is_null { 1 } else { 0 }))
            } else {
                Ok(Value::Integer(if is_null { 1 } else { 0 }))
            }
        }
        Expr::Between { expr: inner, low, high, negated } => {
            let val = eval_rtree_expr(inner, row_values, column_names)?;
            let low_val = eval_rtree_expr(low, row_values, column_names)?;
            let high_val = eval_rtree_expr(high, row_values, column_names)?;
            let in_range = val >= low_val && val <= high_val;
            let result = if *negated { !in_range } else { in_range };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        Expr::Function { name, args, .. } => {
            // Minimal function support for rtree queries
            let evaluated_args: Vec<Value> = args
                .iter()
                .map(|a| eval_rtree_expr(a, row_values, column_names))
                .collect::<Result<Vec<_>>>()?;
            match name.to_uppercase().as_str() {
                "ABS" if evaluated_args.len() == 1 => {
                    match &evaluated_args[0] {
                        Value::Integer(i) => Ok(Value::Integer(i.abs())),
                        Value::Real(r) => Ok(Value::Real(r.abs())),
                        _ => Ok(Value::Null),
                    }
                }
                _ => Err(HorizonError::NotImplemented(format!(
                    "function {} in rtree context", name
                ))),
            }
        }
        _ => Err(HorizonError::NotImplemented(format!(
            "expression type {:?} in rtree context",
            expr
        ))),
    }
}

/// Evaluate a binary operation on two values.
fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    match op {
        BinaryOp::And => {
            Ok(Value::Integer(if left.to_bool() && right.to_bool() { 1 } else { 0 }))
        }
        BinaryOp::Or => {
            Ok(Value::Integer(if left.to_bool() || right.to_bool() { 1 } else { 0 }))
        }
        BinaryOp::Eq => Ok(Value::Integer(if left == right { 1 } else { 0 })),
        BinaryOp::NotEq => Ok(Value::Integer(if left != right { 1 } else { 0 })),
        BinaryOp::Lt => Ok(Value::Integer(if left < right { 1 } else { 0 })),
        BinaryOp::Gt => Ok(Value::Integer(if left > right { 1 } else { 0 })),
        BinaryOp::LtEq => Ok(Value::Integer(if left <= right { 1 } else { 0 })),
        BinaryOp::GtEq => Ok(Value::Integer(if left >= right { 1 } else { 0 })),
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            // Avoid division by zero
            match (left, right) {
                (Value::Integer(_), Value::Integer(0)) => Ok(Value::Null),
                (Value::Real(_), Value::Real(r)) if *r == 0.0 => Ok(Value::Null),
                _ => numeric_op(left, right, |a, b| a / b, |a, b| a / b),
            }
        }
        BinaryOp::Mod => numeric_op(left, right, |a, b| if b != 0 { a % b } else { 0 }, |a, b| a % b),
        _ => Err(HorizonError::NotImplemented(format!(
            "binary op {:?} in rtree context", op
        ))),
    }
}

/// Apply a numeric operation to two values.
fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> i64,
    real_op: fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_op(*a, *b))),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(real_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(real_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(real_op(*a, *b as f64))),
        _ => Ok(Value::Null),
    }
}

/// Convert an AST literal to a Value.
fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Integer(i) => Value::Integer(*i),
        LiteralValue::Real(r) => Value::Real(*r),
        LiteralValue::String(s) => Value::Text(s.clone()),
        LiteralValue::Blob(b) => Value::Blob(b.clone()),
        LiteralValue::Null => Value::Null,
        LiteralValue::True => Value::Integer(1),
        LiteralValue::False => Value::Integer(0),
    }
}

/// Evaluate a constant expression (used for INSERT values).
fn eval_const_expr_rtree(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::UnaryOp { op: UnaryOp::Neg, expr: inner } => {
            let val = eval_const_expr_rtree(inner)?;
            match val {
                Value::Integer(i) => Ok(Value::Integer(-i)),
                Value::Real(r) => Ok(Value::Real(-r)),
                _ => Err(HorizonError::InvalidSql(
                    "cannot negate non-numeric value".into(),
                )),
            }
        }
        _ => Err(HorizonError::InvalidSql(format!(
            "unsupported expression in rtree INSERT: {:?}",
            expr
        ))),
    }
}

/// Evaluate a constant expression to an i64 (for LIMIT/OFFSET).
fn eval_const_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(LiteralValue::Integer(i)) => Some(*i),
        Expr::Literal(LiteralValue::Real(r)) => Some(*r as i64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtree_entry_round_trip() {
        let entry = RTreeEntry {
            id: 42,
            coords: vec![1.0, 10.0, 2.0, 20.0],
        };
        let bytes = entry.serialize();
        let decoded = RTreeEntry::deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.coords, vec![1.0, 10.0, 2.0, 20.0]);
    }

    #[test]
    fn rtree_entry_1d() {
        let entry = RTreeEntry {
            id: 1,
            coords: vec![5.0, 15.0],
        };
        let bytes = entry.serialize();
        let decoded = RTreeEntry::deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(decoded.coords, vec![5.0, 15.0]);
    }

    #[test]
    fn rtree_entry_empty_coords() {
        let entry = RTreeEntry {
            id: 99,
            coords: vec![],
        };
        let bytes = entry.serialize();
        let decoded = RTreeEntry::deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, 99);
        assert!(decoded.coords.is_empty());
    }

    #[test]
    fn rtree_entry_deserialize_truncated() {
        assert!(RTreeEntry::deserialize(&[0u8; 5]).is_err());
    }

    #[test]
    fn rtree_entry_to_values_test() {
        let rtree = RTreeInfo {
            name: "test".to_string(),
            column_names: vec!["id".into(), "minX".into(), "maxX".into(), "minY".into(), "maxY".into()],
            num_dimensions: 2,
            root_page: 1,
        };
        let entry = RTreeEntry {
            id: 1,
            coords: vec![0.0, 10.0, 0.0, 20.0],
        };
        let values = rtree_entry_to_values(&entry, &rtree);
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], Value::Integer(1));
        assert_eq!(values[1], Value::Real(0.0));
        assert_eq!(values[2], Value::Real(10.0));
        assert_eq!(values[3], Value::Real(0.0));
        assert_eq!(values[4], Value::Real(20.0));
    }

    #[test]
    fn literal_to_value_test() {
        assert_eq!(literal_to_value(&LiteralValue::Integer(42)), Value::Integer(42));
        assert_eq!(literal_to_value(&LiteralValue::Real(3.14)), Value::Real(3.14));
        assert_eq!(literal_to_value(&LiteralValue::Null), Value::Null);
        assert_eq!(literal_to_value(&LiteralValue::True), Value::Integer(1));
        assert_eq!(literal_to_value(&LiteralValue::False), Value::Integer(0));
    }

    #[test]
    fn eval_binary_and_or() {
        let t = Value::Integer(1);
        let f = Value::Integer(0);
        assert_eq!(eval_binary_op(&t, &BinaryOp::And, &t).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&t, &BinaryOp::And, &f).unwrap(), Value::Integer(0));
        assert_eq!(eval_binary_op(&f, &BinaryOp::Or, &t).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&f, &BinaryOp::Or, &f).unwrap(), Value::Integer(0));
    }

    #[test]
    fn eval_comparison_ops() {
        let a = Value::Real(5.0);
        let b = Value::Real(10.0);
        assert_eq!(eval_binary_op(&a, &BinaryOp::Lt, &b).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&a, &BinaryOp::Gt, &b).unwrap(), Value::Integer(0));
        assert_eq!(eval_binary_op(&a, &BinaryOp::LtEq, &a).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&b, &BinaryOp::GtEq, &a).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&a, &BinaryOp::Eq, &a).unwrap(), Value::Integer(1));
        assert_eq!(eval_binary_op(&a, &BinaryOp::NotEq, &b).unwrap(), Value::Integer(1));
    }
}
