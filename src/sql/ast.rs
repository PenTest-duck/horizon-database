//! Abstract syntax tree definitions for Horizon DB SQL.
//!
//! Every SQL statement parsed by the [`super::parser::Parser`] is represented
//! as a tree of the types defined here. The AST is consumed downstream by
//! the query planner and execution engine.

/// A top-level SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    Begin,
    Commit,
    Rollback,
}

/// A `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

/// A single item in the SELECT column list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    /// An arbitrary expression, optionally aliased (`expr AS alias`).
    Expr { expr: Expr, alias: Option<String> },
    /// A bare `*`.
    AllColumns,
    /// A qualified `table.*`.
    TableAllColumns(String),
}

/// The `FROM` clause — a single table, a join tree, or a subquery.
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromClause>,
        join_type: JoinType,
        right: Box<FromClause>,
        on: Option<Expr>,
    },
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

/// The flavour of a `JOIN`.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

/// A single item in an `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub desc: bool,
}

/// An `INSERT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    /// Multiple value rows: `VALUES (a, b), (c, d)`.
    pub values: Vec<Vec<Expr>>,
    pub or_replace: bool,
}

/// An `UPDATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

/// A `DELETE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// A `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
}

/// A column definition inside `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<String>,
    pub primary_key: bool,
    pub autoincrement: bool,
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

/// A `DROP TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub name: String,
    pub if_exists: bool,
}

/// A `CREATE INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<OrderByItem>,
    pub unique: bool,
    pub if_not_exists: bool,
}

/// A `DROP INDEX` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub name: String,
    pub if_exists: bool,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// An expression node in the AST.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(LiteralValue),
    Column {
        table: Option<String>,
        name: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Cast {
        expr: Box<Expr>,
        type_name: String,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Subquery(Box<SelectStatement>),
    Exists(Box<SelectStatement>),
    Placeholder(usize),
}

/// A literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Integer(i64),
    Real(f64),
    String(String),
    Blob(Vec<u8>),
    Null,
    True,
    False,
}

/// Binary operators.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Concat,
    BitAnd,
    BitOr,
    ShiftLeft,
    ShiftRight,
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_statement_default_fields() {
        let stmt = SelectStatement {
            distinct: false,
            columns: vec![SelectColumn::AllColumns],
            from: Some(FromClause::Table {
                name: "users".into(),
                alias: None,
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        assert!(!stmt.distinct);
        assert_eq!(stmt.columns.len(), 1);
    }

    #[test]
    fn expr_binary_op_nesting() {
        // Represent: 1 + 2 * 3
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(LiteralValue::Integer(2))),
                op: BinaryOp::Mul,
                right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
            }),
        };
        if let Expr::BinaryOp { op, .. } = &expr {
            assert_eq!(*op, BinaryOp::Add);
        } else {
            panic!("expected BinaryOp");
        }
    }

    #[test]
    fn column_def_constraints() {
        let col = ColumnDef {
            name: "id".into(),
            type_name: Some("INTEGER".into()),
            primary_key: true,
            autoincrement: true,
            not_null: false,
            unique: false,
            default: None,
        };
        assert!(col.primary_key);
        assert!(col.autoincrement);
    }

    #[test]
    fn literal_value_variants() {
        let vals: Vec<LiteralValue> = vec![
            LiteralValue::Integer(42),
            LiteralValue::Real(3.14),
            LiteralValue::String("hello".into()),
            LiteralValue::Blob(vec![0xDE, 0xAD]),
            LiteralValue::Null,
            LiteralValue::True,
            LiteralValue::False,
        ];
        assert_eq!(vals.len(), 7);
    }
}
