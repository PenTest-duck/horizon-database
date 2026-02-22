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
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
    CreateTrigger(CreateTriggerStatement),
    DropTrigger(DropTriggerStatement),
    AlterTable(AlterTableStatement),
    Explain(Box<Statement>),
    Pragma(PragmaStatement),
    Begin,
    Commit,
    Rollback,
    AttachDatabase(AttachDatabaseStatement),
    DetachDatabase(DetachDatabaseStatement),
    Vacuum,
}

/// A `SELECT` statement, possibly with CTEs and compound operators.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub ctes: Vec<Cte>,
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    /// UNION / INTERSECT / EXCEPT chain (applied after this select body).
    pub compound: Vec<CompoundOp>,
}

/// A Common Table Expression: `name [(col1, col2)] AS (select)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: SelectStatement,
    pub recursive: bool,
}

/// A compound SELECT operator with its second operand.
#[derive(Debug, Clone, PartialEq)]
pub struct CompoundOp {
    pub op: CompoundType,
    pub select: SelectBody,
}

/// The type of compound query.
#[derive(Debug, Clone, PartialEq)]
pub enum CompoundType {
    Union,
    UnionAll,
    Intersect,
    Except,
}

/// A single SELECT body (the core part without CTEs or trailing compounds).
#[derive(Debug, Clone, PartialEq)]
pub struct SelectBody {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
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
    /// Optional `RETURNING` clause.
    pub returning: Option<Vec<SelectColumn>>,
}

/// An `UPDATE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
    /// Optional `RETURNING` clause.
    pub returning: Option<Vec<SelectColumn>>,
}

/// A `DELETE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
    /// Optional `RETURNING` clause.
    pub returning: Option<Vec<SelectColumn>>,
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
    pub collation: Option<String>,
    /// Generated column: `GENERATED ALWAYS AS (expr) STORED|VIRTUAL`
    /// or shorthand `AS (expr) STORED|VIRTUAL`.
    pub generated: Option<GeneratedColumn>,
}

/// Metadata for a generated column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedColumn {
    /// The expression that computes this column's value.
    pub expr: Expr,
    /// If `true` the value is stored on disk; if `false` it is computed on
    /// read (VIRTUAL).
    pub stored: bool,
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

/// An `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table: String,
    pub action: AlterTableAction,
}

/// The specific alteration to apply to a table.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    RenameTable(String),
    RenameColumn { old_name: String, new_name: String },
    DropColumn(String),
}

/// A `CREATE VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: SelectStatement,
    pub if_not_exists: bool,
}

/// A `DROP VIEW` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStatement {
    pub name: String,
    pub if_exists: bool,
}

/// A `CREATE TRIGGER` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStatement {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub for_each_row: bool,
    pub when: Option<Expr>,
    pub body: Vec<Statement>,
    pub if_not_exists: bool,
}

/// When the trigger fires relative to the event.
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// The event that activates a trigger.
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<String>>),
    Delete,
}

/// A `DROP TRIGGER` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStatement {
    pub name: String,
    pub if_exists: bool,
}

/// A `PRAGMA` statement — SQLite-compatible configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct PragmaStatement {
    pub name: String,
    pub value: Option<Expr>,
}

/// An `ATTACH DATABASE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachDatabaseStatement {
    pub path: String,
    pub schema_name: String,
}

/// A `DETACH DATABASE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DetachDatabaseStatement {
    pub schema_name: String,
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
    /// A `COLLATE` expression: `expr COLLATE collation_name`.
    Collate {
        expr: Box<Expr>,
        collation: String,
    },
    /// A window function call: `func(...) OVER (PARTITION BY ... ORDER BY ... frame)`.
    WindowFunction {
        function: Box<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByItem>,
        frame: Option<WindowFrame>,
    },
}

/// The frame specification for a window function.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

/// Whether the window frame is row-based or range-based.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameMode {
    Rows,
    Range,
}

/// A single bound in a window frame clause.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `[UNBOUNDED | expr] PRECEDING` -- `None` means UNBOUNDED.
    Preceding(Option<Box<Expr>>),
    /// `[UNBOUNDED | expr] FOLLOWING` -- `None` means UNBOUNDED.
    Following(Option<Box<Expr>>),
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
            ctes: vec![],
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
            compound: vec![],
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
            collation: None,
            generated: None,
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
