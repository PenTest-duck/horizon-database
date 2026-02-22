// View and trigger execution support for Horizon DB.
// This is included from mod.rs.

use super::*;

// ---- CREATE VIEW / DROP VIEW ----

pub(super) fn execute_create_view(cv: &CreateViewStatement, catalog: &mut Catalog) -> Result<usize> {
    if cv.if_not_exists && catalog.view_exists(&cv.name) { return Ok(0); }
    if catalog.table_exists(&cv.name) { return Err(HorizonError::DuplicateTable(cv.name.clone())); }
    let sql = select_to_sql(&cv.query);
    catalog.create_view(ViewInfo { name: cv.name.clone(), sql, columns: cv.columns.clone() })?;
    Ok(0)
}

pub(super) fn execute_drop_view(dv: &DropViewStatement, catalog: &mut Catalog) -> Result<usize> {
    if dv.if_exists && !catalog.view_exists(&dv.name) { return Ok(0); }
    catalog.drop_view(&dv.name)?;
    Ok(0)
}

// ---- CREATE TRIGGER / DROP TRIGGER ----

pub(super) fn execute_create_trigger(ct: &CreateTriggerStatement, catalog: &mut Catalog) -> Result<usize> {
    if ct.if_not_exists && catalog.trigger_exists(&ct.name) { return Ok(0); }
    let timing = match ct.timing {
        TriggerTiming::Before => TriggerTimingKind::Before,
        TriggerTiming::After => TriggerTimingKind::After,
        TriggerTiming::InsteadOf => TriggerTimingKind::InsteadOf,
    };
    let event = match ct.event {
        TriggerEvent::Insert => TriggerEventKind::Insert,
        TriggerEvent::Update(_) => TriggerEventKind::Update,
        TriggerEvent::Delete => TriggerEventKind::Delete,
    };
    let body_sql: Vec<String> = ct.body.iter().map(|s| statement_to_sql(s)).collect();
    catalog.create_trigger(TriggerInfo {
        name: ct.name.clone(), timing, event, table: ct.table.clone(),
        for_each_row: ct.for_each_row, body_sql,
    })?;
    Ok(0)
}

pub(super) fn execute_drop_trigger(dt: &DropTriggerStatement, catalog: &mut Catalog) -> Result<usize> {
    if dt.if_exists && !catalog.trigger_exists(&dt.name) { return Ok(0); }
    catalog.drop_trigger(&dt.name)?;
    Ok(0)
}

pub(super) fn fire_triggers(
    table_name: &str,
    event: &TriggerEventKind,
    timing: &TriggerTimingKind,
    pool: &mut BufferPool,
    catalog: &mut Catalog,
    txn_mgr: &mut TransactionManager,
) -> Result<()> {
    let triggers: Vec<TriggerInfo> = catalog
        .get_triggers_for_table(table_name, event, timing)
        .into_iter()
        .cloned()
        .collect();
    for trigger in &triggers {
        for body_sql in &trigger.body_sql {
            let stmts = Parser::parse(body_sql)?;
            for stmt in &stmts {
                match stmt {
                    Statement::Select(_) => {
                        let _ = super::execute_query(stmt, pool, catalog, txn_mgr)?;
                    }
                    _ => {
                        super::execute_statement(stmt, pool, catalog, txn_mgr)?;
                    }
                }
            }
        }
    }
    Ok(())
}

// ---- SQL Reconstruction Helpers ----

pub(super) fn select_to_sql(select: &SelectStatement) -> String {
    let mut sql = String::from("SELECT ");
    if select.distinct {
        sql.push_str("DISTINCT ");
    }
    let cols: Vec<String> = select.columns.iter().map(|c| select_column_to_sql(c)).collect();
    sql.push_str(&cols.join(", "));
    if let Some(ref from) = select.from {
        sql.push_str(" FROM ");
        sql.push_str(&from_clause_to_sql(from));
    }
    if let Some(ref wh) = select.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&expr_to_sql(wh));
    }
    if !select.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        let g: Vec<String> = select.group_by.iter().map(|e| expr_to_sql(e)).collect();
        sql.push_str(&g.join(", "));
    }
    if let Some(ref having) = select.having {
        sql.push_str(" HAVING ");
        sql.push_str(&expr_to_sql(having));
    }
    if !select.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        let items: Vec<String> = select.order_by.iter().map(|o| {
            let mut s = expr_to_sql(&o.expr);
            if o.desc { s.push_str(" DESC"); }
            s
        }).collect();
        sql.push_str(&items.join(", "));
    }
    if let Some(ref limit) = select.limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&expr_to_sql(limit));
    }
    if let Some(ref offset) = select.offset {
        sql.push_str(" OFFSET ");
        sql.push_str(&expr_to_sql(offset));
    }
    sql
}

fn select_column_to_sql(col: &SelectColumn) -> String {
    match col {
        SelectColumn::AllColumns => "*".to_string(),
        SelectColumn::TableAllColumns(t) => format!("{}.*", t),
        SelectColumn::Expr { expr, alias } => {
            let mut s = expr_to_sql(expr);
            if let Some(ref a) = alias {
                s.push_str(" AS ");
                s.push_str(a);
            }
            s
        }
    }
}

fn from_clause_to_sql(from: &FromClause) -> String {
    match from {
        FromClause::Table { name, alias } => {
            if let Some(ref a) = alias {
                format!("{} AS {}", name, a)
            } else {
                name.clone()
            }
        }
        FromClause::Join { left, join_type, right, on } => {
            let jt = match join_type {
                JoinType::Inner => "JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Cross => "CROSS JOIN",
            };
            let mut s = format!("{} {} {}", from_clause_to_sql(left), jt, from_clause_to_sql(right));
            if let Some(ref on_expr) = on {
                s.push_str(" ON ");
                s.push_str(&expr_to_sql(on_expr));
            }
            s
        }
        FromClause::Subquery { query, alias } => {
            format!("({}) AS {}", select_to_sql(query), alias)
        }
        FromClause::TableFunction { name, args, alias } => {
            let args_str: Vec<String> = args.iter().map(|a| expr_to_sql(a)).collect();
            let base = format!("{}({})", name, args_str.join(", "));
            if let Some(ref a) = alias {
                format!("{} AS {}", base, a)
            } else {
                base
            }
        }
    }
}

fn expr_to_sql(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit) => literal_to_sql_repr(lit),
        Expr::Column { table, name } => {
            if let Some(ref t) = table {
                format!("{}.{}", t, name)
            } else {
                name.clone()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinaryOp::Add => "+", BinaryOp::Sub => "-", BinaryOp::Mul => "*",
                BinaryOp::Div => "/", BinaryOp::Mod => "%", BinaryOp::Eq => "=",
                BinaryOp::NotEq => "!=", BinaryOp::Lt => "<", BinaryOp::Gt => ">",
                BinaryOp::LtEq => "<=", BinaryOp::GtEq => ">=", BinaryOp::And => "AND",
                BinaryOp::Or => "OR", BinaryOp::Concat => "||", BinaryOp::BitAnd => "&",
                BinaryOp::BitOr => "|", BinaryOp::ShiftLeft => "<<", BinaryOp::ShiftRight => ">>",
            };
            format!("({} {} {})", expr_to_sql(left), op_str, expr_to_sql(right))
        }
        Expr::UnaryOp { op, expr: inner } => {
            let op_str = match op {
                UnaryOp::Neg => "-", UnaryOp::Not => "NOT ", UnaryOp::BitNot => "~",
            };
            format!("({}{})", op_str, expr_to_sql(inner))
        }
        Expr::IsNull { expr: inner, negated } => {
            if *negated { format!("({} IS NOT NULL)", expr_to_sql(inner)) }
            else { format!("({} IS NULL)", expr_to_sql(inner)) }
        }
        Expr::Between { expr: inner, low, high, negated } => {
            if *negated { format!("({} NOT BETWEEN {} AND {})", expr_to_sql(inner), expr_to_sql(low), expr_to_sql(high)) }
            else { format!("({} BETWEEN {} AND {})", expr_to_sql(inner), expr_to_sql(low), expr_to_sql(high)) }
        }
        Expr::InList { expr: inner, list, negated } => {
            let items: Vec<String> = list.iter().map(|e| expr_to_sql(e)).collect();
            if *negated { format!("({} NOT IN ({}))", expr_to_sql(inner), items.join(", ")) }
            else { format!("({} IN ({}))", expr_to_sql(inner), items.join(", ")) }
        }
        Expr::Like { expr: inner, pattern, negated } => {
            if *negated { format!("({} NOT LIKE {})", expr_to_sql(inner), expr_to_sql(pattern)) }
            else { format!("({} LIKE {})", expr_to_sql(inner), expr_to_sql(pattern)) }
        }
        Expr::Match { table, pattern } => {
            format!("({} MATCH {})", expr_to_sql(table), expr_to_sql(pattern))
        }
        Expr::Function { name, args, distinct } => {
            let a: Vec<String> = args.iter().map(|x| expr_to_sql(x)).collect();
            if *distinct { format!("{}(DISTINCT {})", name, a.join(", ")) }
            else { format!("{}({})", name, a.join(", ")) }
        }
        Expr::Cast { expr: inner, type_name } => {
            format!("CAST({} AS {})", expr_to_sql(inner), type_name)
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            let mut s = String::from("CASE");
            if let Some(ref op) = operand { s.push(' '); s.push_str(&expr_to_sql(op)); }
            for (w, t) in when_clauses {
                s.push_str(" WHEN ");
                s.push_str(&expr_to_sql(w));
                s.push_str(" THEN ");
                s.push_str(&expr_to_sql(t));
            }
            if let Some(ref e) = else_clause { s.push_str(" ELSE "); s.push_str(&expr_to_sql(e)); }
            s.push_str(" END");
            s
        }
        Expr::Subquery(sel) => format!("({})", select_to_sql(sel)),
        Expr::Exists(sel) => format!("EXISTS ({})", select_to_sql(sel)),
        Expr::Placeholder(n) => format!("?{}", n),
        Expr::Collate { expr: inner, collation } => {
            format!("{} COLLATE {}", expr_to_sql(inner), collation)
        }
        Expr::WindowFunction { function, partition_by, order_by, .. } => {
            let mut s = expr_to_sql(function);
            s.push_str(" OVER (");
            if !partition_by.is_empty() {
                s.push_str("PARTITION BY ");
                let p: Vec<String> = partition_by.iter().map(|e| expr_to_sql(e)).collect();
                s.push_str(&p.join(", "));
            }
            if !order_by.is_empty() {
                if !partition_by.is_empty() { s.push(' '); }
                s.push_str("ORDER BY ");
                let o: Vec<String> = order_by.iter().map(|x| {
                    let mut os = expr_to_sql(&x.expr);
                    if x.desc { os.push_str(" DESC"); }
                    os
                }).collect();
                s.push_str(&o.join(", "));
            }
            s.push(')');
            s
        }
    }
}

fn literal_to_sql_repr(lit: &LiteralValue) -> String {
    match lit {
        LiteralValue::Integer(i) => i.to_string(),
        LiteralValue::Real(f) => format!("{}", f),
        LiteralValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        LiteralValue::Blob(b) => format!("X'{}'", b.iter().map(|byte| format!("{:02X}", byte)).collect::<String>()),
        LiteralValue::Null => "NULL".to_string(),
        LiteralValue::True => "TRUE".to_string(),
        LiteralValue::False => "FALSE".to_string(),
    }
}

pub(super) fn statement_to_sql(stmt: &Statement) -> String {
    match stmt {
        Statement::Insert(ins) => {
            let mut sql = format!("INSERT INTO {}", ins.table);
            if let Some(ref cols) = ins.columns {
                sql.push_str(&format!(" ({})", cols.join(", ")));
            }
            sql.push_str(" VALUES ");
            let rows: Vec<String> = ins.values.iter().map(|row| {
                let vals: Vec<String> = row.iter().map(|e| expr_to_sql(e)).collect();
                format!("({})", vals.join(", "))
            }).collect();
            sql.push_str(&rows.join(", "));
            sql
        }
        Statement::Update(upd) => {
            let mut sql = format!("UPDATE {} SET ", upd.table);
            let a: Vec<String> = upd.assignments.iter()
                .map(|(c, e)| format!("{} = {}", c, expr_to_sql(e)))
                .collect();
            sql.push_str(&a.join(", "));
            if let Some(ref wh) = upd.where_clause {
                sql.push_str(" WHERE ");
                sql.push_str(&expr_to_sql(wh));
            }
            sql
        }
        Statement::Delete(del) => {
            let mut sql = format!("DELETE FROM {}", del.table);
            if let Some(ref wh) = del.where_clause {
                sql.push_str(" WHERE ");
                sql.push_str(&expr_to_sql(wh));
            }
            sql
        }
        Statement::Select(sel) => select_to_sql(sel),
        _ => String::new(),
    }
}
