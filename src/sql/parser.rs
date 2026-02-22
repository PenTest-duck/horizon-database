//! Recursive-descent SQL parser for Horizon DB.
//!
//! The entry point is [`Parser::parse`], which tokenizes the input and then
//! parses one or more semicolon-separated statements into a `Vec<Statement>`.

use crate::error::{HorizonError, Result};
use crate::sql::ast::*;
use crate::sql::lexer::{Lexer, Token};

/// A recursive-descent parser that transforms a token stream into an AST.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    /// Parse a SQL string into a list of statements.
    pub fn parse(sql: &str) -> Result<Vec<Statement>> {
        let tokens = Lexer::new(sql).tokenize()?;
        let mut parser = Parser { tokens, pos: 0 };
        let mut stmts = Vec::new();
        loop {
            // Skip optional semicolons between statements.
            while parser.current() == &Token::Semicolon {
                parser.advance();
            }
            if parser.current() == &Token::Eof {
                break;
            }
            stmts.push(parser.parse_statement()?);
            // Consume trailing semicolon if present.
            if parser.current() == &Token::Semicolon {
                parser.advance();
            }
        }
        Ok(stmts)
    }

    // =======================================================================
    // Token helpers
    // =======================================================================

    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek_ahead(&self, offset: usize) -> &Token {
        self.tokens.get(self.pos + offset).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if self.current() == expected {
            self.advance();
            Ok(())
        } else {
            Err(self.error(format!("expected {expected:?}, got {:?}", self.current())))
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            // Allow keywords that are safe to use as identifiers.
            _ => {
                // Try to extract keyword-as-identifier
                if let Some(name) = self.keyword_as_identifier() {
                    self.advance();
                    Ok(name)
                } else {
                    Err(self.error(format!("expected identifier, got {:?}", self.current())))
                }
            }
        }
    }

    /// Many SQL keywords can also appear as identifiers in certain positions.
    /// Return the keyword text if the current token is one of those.
    fn keyword_as_identifier(&self) -> Option<String> {
        let name = match self.current() {
            Token::Table => "table",
            Token::Index => "index",
            Token::View => "view",
            Token::Key => "key",
            Token::Column => "column",
            Token::Add => "add",
            Token::Rename => "rename",
            Token::Replace => "replace",
            Token::Abort => "abort",
            Token::Fail => "fail",
            Token::Ignore => "ignore",
            Token::Conflict => "conflict",
            Token::Query => "query",
            Token::Plan => "plan",
            Token::Row => "row",
            Token::Rows => "rows",
            Token::Range => "range",
            Token::Window => "window",
            Token::Over => "over",
            Token::Partition => "partition",
            Token::Current => "current",
            Token::Preceding => "preceding",
            Token::Following => "following",
            Token::Unbounded => "unbounded",
            Token::Escape => "escape",
            Token::Collate => "collate",
            Token::Database => "database",
            Token::Returning => "returning",
            Token::IntegerKw => "integer",
            Token::TextKw => "text",
            Token::RealKw => "real",
            Token::BlobKw => "blob",
            Token::NumericKw => "numeric",
            Token::Transaction => "transaction",
            Token::Savepoint => "savepoint",
            Token::Release => "release",
            Token::Pragma => "pragma",
            Token::Vacuum => "vacuum",
            Token::Attach => "attach",
            Token::Detach => "detach",
            Token::Trigger => "trigger",
            Token::Begin => "begin",
            Token::End => "end",
            Token::To => "to",
            Token::If => "if",
            _ => return None,
        };
        Some(name.to_string())
    }

    fn error(&self, msg: String) -> HorizonError {
        HorizonError::InvalidSql(msg)
    }

    // =======================================================================
    // Statement dispatch
    // =======================================================================

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.current() {
            Token::Select => self.parse_select_stmt(),
            Token::Insert | Token::Replace => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Begin => {
                self.advance();
                // Optional TRANSACTION keyword.
                if self.current() == &Token::Transaction {
                    self.advance();
                }
                Ok(Statement::Begin)
            }
            Token::Commit => {
                self.advance();
                if self.current() == &Token::Transaction {
                    self.advance();
                }
                Ok(Statement::Commit)
            }
            Token::Rollback => {
                self.advance();
                if self.current() == &Token::Transaction {
                    self.advance();
                }
                Ok(Statement::Rollback)
            }
            _ => Err(self.error(format!(
                "unexpected token at start of statement: {:?}",
                self.current()
            ))),
        }
    }

    // =======================================================================
    // SELECT
    // =======================================================================

    fn parse_select_stmt(&mut self) -> Result<Statement> {
        Ok(Statement::Select(self.parse_select()?))
    }

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(&Token::Select)?;

        let distinct = if self.current() == &Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        let from = if self.current() == &Token::From {
            self.advance();
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.current() == &Token::Group {
            self.advance();
            self.expect(&Token::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.current() == &Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.current() == &Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.current() == &Token::Limit {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.current() == &Token::Offset {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SelectStatement {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut cols = vec![self.parse_select_column()?];
        while self.current() == &Token::Comma {
            self.advance();
            cols.push(self.parse_select_column()?);
        }
        Ok(cols)
    }

    fn parse_select_column(&mut self) -> Result<SelectColumn> {
        // Check for bare *
        if self.current() == &Token::Star {
            self.advance();
            return Ok(SelectColumn::AllColumns);
        }

        // Check for table.* — identifier followed by dot followed by star
        if let Token::Identifier(name) = self.current().clone() {
            if self.peek_ahead(1) == &Token::Dot && self.peek_ahead(2) == &Token::Star {
                self.advance(); // identifier
                self.advance(); // dot
                self.advance(); // star
                return Ok(SelectColumn::TableAllColumns(name));
            }
        }

        let expr = self.parse_expr()?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectColumn::Expr { expr, alias })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        if self.current() == &Token::As {
            self.advance();
            let name = self.expect_identifier()?;
            Ok(Some(name))
        } else if let Token::Identifier(_) = self.current() {
            // Implicit alias without AS — only if the next token looks like
            // an identifier and NOT a keyword that could start a clause.
            let name = self.expect_identifier()?;
            Ok(Some(name))
        } else {
            Ok(None)
        }
    }

    // =======================================================================
    // FROM clause + JOINs
    // =======================================================================

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let mut left = self.parse_from_item()?;

        loop {
            let join_type = match self.current() {
                Token::Join | Token::Inner => {
                    if self.current() == &Token::Inner {
                        self.advance();
                    }
                    self.expect(&Token::Join)?;
                    JoinType::Inner
                }
                Token::Left => {
                    self.advance();
                    if self.current() == &Token::Outer {
                        self.advance();
                    }
                    self.expect(&Token::Join)?;
                    JoinType::Left
                }
                Token::Right => {
                    self.advance();
                    if self.current() == &Token::Outer {
                        self.advance();
                    }
                    self.expect(&Token::Join)?;
                    JoinType::Right
                }
                Token::Cross => {
                    self.advance();
                    self.expect(&Token::Join)?;
                    JoinType::Cross
                }
                Token::Comma => {
                    self.advance();
                    let right = self.parse_from_item()?;
                    left = FromClause::Join {
                        left: Box::new(left),
                        join_type: JoinType::Cross,
                        right: Box::new(right),
                        on: None,
                    };
                    continue;
                }
                _ => break,
            };

            let right = self.parse_from_item()?;

            let on = if self.current() == &Token::On {
                self.advance();
                Some(self.parse_expr()?)
            } else {
                None
            };

            left = FromClause::Join {
                left: Box::new(left),
                join_type,
                right: Box::new(right),
                on,
            };
        }

        Ok(left)
    }

    fn parse_from_item(&mut self) -> Result<FromClause> {
        if self.current() == &Token::LeftParen {
            // Could be a subquery or a parenthesised from clause.
            if self.peek_ahead(1) == &Token::Select {
                self.advance(); // consume (
                let query = self.parse_select()?;
                self.expect(&Token::RightParen)?;
                // Alias is required for subqueries.
                if self.current() == &Token::As {
                    self.advance();
                }
                let alias = self.expect_identifier()?;
                return Ok(FromClause::Subquery {
                    query: Box::new(query),
                    alias,
                });
            }
        }

        let name = self.expect_identifier()?;
        let alias = if self.current() == &Token::As {
            self.advance();
            Some(self.expect_identifier()?)
        } else if let Token::Identifier(_) = self.current() {
            // Peek to make sure this isn't a keyword that starts the next
            // clause (WHERE, JOIN, etc. would have been matched as their
            // token variant, not Identifier).
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(FromClause::Table { name, alias })
    }

    // =======================================================================
    // INSERT
    // =======================================================================

    fn parse_insert(&mut self) -> Result<Statement> {
        let or_replace = if self.current() == &Token::Replace {
            self.advance();
            true
        } else {
            self.expect(&Token::Insert)?;
            if self.current() == &Token::Or {
                self.advance();
                self.expect(&Token::Replace)?;
                true
            } else {
                false
            }
        };

        self.expect(&Token::Into)?;
        let table = self.expect_identifier()?;

        let columns = if self.current() == &Token::LeftParen {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect(&Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(&Token::Values)?;
        let mut values = vec![self.parse_value_row()?];
        while self.current() == &Token::Comma {
            self.advance();
            values.push(self.parse_value_row()?);
        }

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
            or_replace,
        }))
    }

    fn parse_value_row(&mut self) -> Result<Vec<Expr>> {
        self.expect(&Token::LeftParen)?;
        let exprs = self.parse_expr_list()?;
        self.expect(&Token::RightParen)?;
        Ok(exprs)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut list = vec![self.expect_identifier()?];
        while self.current() == &Token::Comma {
            self.advance();
            list.push(self.expect_identifier()?);
        }
        Ok(list)
    }

    // =======================================================================
    // UPDATE
    // =======================================================================

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(&Token::Update)?;
        let table = self.expect_identifier()?;
        self.expect(&Token::Set)?;

        let mut assignments = vec![self.parse_assignment()?];
        while self.current() == &Token::Comma {
            self.advance();
            assignments.push(self.parse_assignment()?);
        }

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    fn parse_assignment(&mut self) -> Result<(String, Expr)> {
        let col = self.expect_identifier()?;
        self.expect(&Token::Eq)?;
        let expr = self.parse_expr()?;
        Ok((col, expr))
    }

    // =======================================================================
    // DELETE
    // =======================================================================

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.expect_identifier()?;

        let where_clause = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    // =======================================================================
    // CREATE TABLE / INDEX
    // =======================================================================

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;

        // CREATE UNIQUE INDEX ...
        if self.current() == &Token::Unique {
            self.advance();
            return self.parse_create_index(true);
        }

        match self.current() {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(false),
            _ => Err(self.error(format!(
                "expected TABLE or INDEX after CREATE, got {:?}",
                self.current()
            ))),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(&Token::Table)?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.expect_identifier()?;

        self.expect(&Token::LeftParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(&Token::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            if_not_exists,
            columns,
        }))
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        if self.current() == &Token::If {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_exists(&mut self) -> Result<bool> {
        if self.current() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut defs = vec![self.parse_column_def()?];
        while self.current() == &Token::Comma {
            // Peek ahead to see if the next thing looks like a table
            // constraint (PRIMARY, UNIQUE, CHECK, FOREIGN) rather than
            // a column name. If so, stop.
            if matches!(
                self.peek_ahead(1),
                Token::Primary | Token::Unique | Token::Check | Token::Foreign
            ) {
                // Skip table constraints for now.
                break;
            }
            self.advance();
            defs.push(self.parse_column_def()?);
        }
        // Consume any remaining tokens up to the closing paren (table
        // constraints we don't yet handle).
        let mut depth = 0i32;
        while self.current() != &Token::RightParen || depth > 0 {
            if self.current() == &Token::LeftParen {
                depth += 1;
            } else if self.current() == &Token::RightParen {
                depth -= 1;
            }
            if self.current() == &Token::Eof {
                return Err(self.error("unexpected end of input in column definitions".into()));
            }
            self.advance();
        }
        Ok(defs)
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.expect_identifier()?;

        // Optional type name — consume identifier tokens that look like a
        // type (INTEGER, TEXT, etc. or arbitrary identifiers).
        let type_name = self.parse_type_name()?;

        let mut primary_key = false;
        let mut autoincrement = false;
        let mut not_null = false;
        let mut unique = false;
        let mut default = None;

        // Column constraints
        loop {
            match self.current() {
                Token::Primary => {
                    self.advance();
                    self.expect(&Token::Key)?;
                    primary_key = true;
                    if self.current() == &Token::Autoincrement {
                        self.advance();
                        autoincrement = true;
                    }
                }
                Token::Not => {
                    self.advance();
                    self.expect(&Token::Null)?;
                    not_null = true;
                }
                Token::Unique => {
                    self.advance();
                    unique = true;
                }
                Token::Default => {
                    self.advance();
                    if self.current() == &Token::LeftParen {
                        self.advance();
                        default = Some(self.parse_expr()?);
                        self.expect(&Token::RightParen)?;
                    } else {
                        default = Some(self.parse_primary_expr()?);
                    }
                }
                Token::Check => {
                    // Skip CHECK constraints for now.
                    self.advance();
                    self.expect(&Token::LeftParen)?;
                    let mut depth = 1i32;
                    while depth > 0 {
                        match self.current() {
                            Token::LeftParen => depth += 1,
                            Token::RightParen => depth -= 1,
                            Token::Eof => {
                                return Err(
                                    self.error("unexpected EOF in CHECK constraint".into())
                                );
                            }
                            _ => {}
                        }
                        self.advance();
                    }
                }
                Token::References => {
                    // Skip REFERENCES clause for now.
                    self.advance();
                    let _ref_table = self.expect_identifier()?;
                    if self.current() == &Token::LeftParen {
                        self.advance();
                        let mut depth = 1i32;
                        while depth > 0 {
                            match self.current() {
                                Token::LeftParen => depth += 1,
                                Token::RightParen => depth -= 1,
                                Token::Eof => {
                                    return Err(
                                        self.error("unexpected EOF in REFERENCES".into())
                                    );
                                }
                                _ => {}
                            }
                            self.advance();
                        }
                    }
                }
                Token::Collate => {
                    self.advance();
                    let _collation = self.expect_identifier()?;
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name,
            type_name,
            primary_key,
            autoincrement,
            not_null,
            unique,
            default,
        })
    }

    fn parse_type_name(&mut self) -> Result<Option<String>> {
        let type_tok = match self.current() {
            Token::IntegerKw => Some("INTEGER"),
            Token::TextKw => Some("TEXT"),
            Token::RealKw => Some("REAL"),
            Token::BlobKw => Some("BLOB"),
            Token::NumericKw => Some("NUMERIC"),
            Token::Identifier(_) => {
                // Only treat as a type name if the identifier is followed
                // by something that looks like a constraint or comma/paren,
                // not an `=` or other operator.
                if let Token::Identifier(ref name) = self.current().clone() {
                    let upper = name.to_ascii_uppercase();
                    // Common SQL type names
                    match upper.as_str() {
                        "INT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
                        | "VARCHAR" | "CHAR" | "BOOLEAN" | "BOOL" | "DOUBLE"
                        | "FLOAT" | "DECIMAL" | "DATE" | "DATETIME" | "TIMESTAMP"
                        | "CLOB" | "NCHAR" | "NVARCHAR" => {
                            self.advance();
                            // Handle optional (N) or (N,M) size suffix
                            let mut full = upper.clone();
                            if self.current() == &Token::LeftParen {
                                self.advance();
                                full.push('(');
                                let mut first = true;
                                while self.current() != &Token::RightParen
                                    && self.current() != &Token::Eof
                                {
                                    if !first {
                                        full.push_str(", ");
                                    }
                                    first = false;
                                    if let Token::IntegerLiteral(n) = self.current() {
                                        full.push_str(&n.to_string());
                                        self.advance();
                                    } else {
                                        self.advance();
                                    }
                                    if self.current() == &Token::Comma {
                                        self.advance();
                                    }
                                }
                                self.expect(&Token::RightParen)?;
                                full.push(')');
                            }
                            return Ok(Some(full));
                        }
                        _ => return Ok(None),
                    }
                }
                return Ok(None);
            }
            _ => None,
        };

        if let Some(name) = type_tok {
            self.advance();
            let mut full = name.to_string();
            // Handle optional (N) or (N,M) size suffix
            if self.current() == &Token::LeftParen {
                self.advance();
                full.push('(');
                let mut first = true;
                while self.current() != &Token::RightParen && self.current() != &Token::Eof {
                    if !first {
                        full.push_str(", ");
                    }
                    first = false;
                    if let Token::IntegerLiteral(n) = self.current() {
                        full.push_str(&n.to_string());
                        self.advance();
                    } else {
                        self.advance();
                    }
                    if self.current() == &Token::Comma {
                        self.advance();
                    }
                }
                self.expect(&Token::RightParen)?;
                full.push(')');
            }
            Ok(Some(full))
        } else {
            Ok(None)
        }
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<Statement> {
        self.expect(&Token::Index)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.expect_identifier()?;
        self.expect(&Token::On)?;
        let table = self.expect_identifier()?;

        self.expect(&Token::LeftParen)?;
        let columns = self.parse_order_by_list()?;
        self.expect(&Token::RightParen)?;

        Ok(Statement::CreateIndex(CreateIndexStatement {
            name,
            table,
            columns,
            unique,
            if_not_exists,
        }))
    }

    // =======================================================================
    // DROP TABLE / INDEX
    // =======================================================================

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;
        match self.current() {
            Token::Table => {
                self.advance();
                let if_exists = self.parse_if_exists()?;
                let name = self.expect_identifier()?;
                Ok(Statement::DropTable(DropTableStatement { name, if_exists }))
            }
            Token::Index => {
                self.advance();
                let if_exists = self.parse_if_exists()?;
                let name = self.expect_identifier()?;
                Ok(Statement::DropIndex(DropIndexStatement { name, if_exists }))
            }
            _ => Err(self.error(format!(
                "expected TABLE or INDEX after DROP, got {:?}",
                self.current()
            ))),
        }
    }

    // =======================================================================
    // ORDER BY list
    // =======================================================================

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = vec![self.parse_order_by_item()?];
        while self.current() == &Token::Comma {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem> {
        let expr = self.parse_expr()?;
        let desc = if self.current() == &Token::Asc {
            self.advance();
            false
        } else if self.current() == &Token::Desc {
            self.advance();
            true
        } else {
            false
        };
        Ok(OrderByItem { expr, desc })
    }

    // =======================================================================
    // Expression parser (precedence climbing)
    // =======================================================================
    //
    // Precedence (lowest to highest):
    //   1. OR
    //   2. AND
    //   3. NOT (prefix)
    //   4. IS [NOT] NULL, BETWEEN, IN, LIKE, comparison (=, <>, <, >, <=, >=)
    //   5. Bitwise OR (|)
    //   6. Bitwise AND (&), Shift (<<, >>)
    //   7. Concatenation (||)
    //   8. Addition (+, -)
    //   9. Multiplication (*, /, %)
    //  10. Unary (-, ~)
    //  11. Primary (literals, columns, function calls, parens, CAST, CASE, etc.)

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;
        while self.current() == &Token::Or {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;
        while self.current() == &Token::And {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr> {
        if self.current() == &Token::Not {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expr()
        }
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitor_expr()?;

        // IS [NOT] NULL, BETWEEN, IN, LIKE, comparison operators
        loop {
            match self.current() {
                Token::Is => {
                    self.advance();
                    let negated = if self.current() == &Token::Not {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    self.expect(&Token::Null)?;
                    left = Expr::IsNull {
                        expr: Box::new(left),
                        negated,
                    };
                }
                Token::Between => {
                    self.advance();
                    let low = self.parse_bitor_expr()?;
                    self.expect(&Token::And)?;
                    let high = self.parse_bitor_expr()?;
                    left = Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    };
                }
                Token::In => {
                    self.advance();
                    self.expect(&Token::LeftParen)?;
                    // Check for subquery
                    if self.current() == &Token::Select {
                        let query = self.parse_select()?;
                        self.expect(&Token::RightParen)?;
                        left = Expr::InList {
                            expr: Box::new(left),
                            list: vec![Expr::Subquery(Box::new(query))],
                            negated: false,
                        };
                    } else {
                        let list = self.parse_expr_list()?;
                        self.expect(&Token::RightParen)?;
                        left = Expr::InList {
                            expr: Box::new(left),
                            list,
                            negated: false,
                        };
                    }
                }
                Token::Like => {
                    self.advance();
                    let pattern = self.parse_bitor_expr()?;
                    left = Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: false,
                    };
                }
                Token::Not => {
                    // NOT BETWEEN, NOT IN, NOT LIKE
                    match self.peek_ahead(1) {
                        Token::Between => {
                            self.advance(); // NOT
                            self.advance(); // BETWEEN
                            let low = self.parse_bitor_expr()?;
                            self.expect(&Token::And)?;
                            let high = self.parse_bitor_expr()?;
                            left = Expr::Between {
                                expr: Box::new(left),
                                low: Box::new(low),
                                high: Box::new(high),
                                negated: true,
                            };
                        }
                        Token::In => {
                            self.advance(); // NOT
                            self.advance(); // IN
                            self.expect(&Token::LeftParen)?;
                            let list = self.parse_expr_list()?;
                            self.expect(&Token::RightParen)?;
                            left = Expr::InList {
                                expr: Box::new(left),
                                list,
                                negated: true,
                            };
                        }
                        Token::Like => {
                            self.advance(); // NOT
                            self.advance(); // LIKE
                            let pattern = self.parse_bitor_expr()?;
                            left = Expr::Like {
                                expr: Box::new(left),
                                pattern: Box::new(pattern),
                                negated: true,
                            };
                        }
                        _ => break,
                    }
                }
                Token::Eq => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Eq,
                        right: Box::new(right),
                    };
                }
                Token::NotEq => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::NotEq,
                        right: Box::new(right),
                    };
                }
                Token::Lt => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Lt,
                        right: Box::new(right),
                    };
                }
                Token::Gt => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Gt,
                        right: Box::new(right),
                    };
                }
                Token::LtEq => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::LtEq,
                        right: Box::new(right),
                    };
                }
                Token::GtEq => {
                    self.advance();
                    let right = self.parse_bitor_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::GtEq,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }

        Ok(left)
    }

    fn parse_bitor_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_bitand_expr()?;
        while self.current() == &Token::Pipe {
            self.advance();
            let right = self.parse_bitand_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::BitOr,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_bitand_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_concat_expr()?;
        loop {
            match self.current() {
                Token::Ampersand => {
                    self.advance();
                    let right = self.parse_concat_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::BitAnd,
                        right: Box::new(right),
                    };
                }
                Token::ShiftLeft => {
                    self.advance();
                    let right = self.parse_concat_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::ShiftLeft,
                        right: Box::new(right),
                    };
                }
                Token::ShiftRight => {
                    self.advance();
                    let right = self.parse_concat_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::ShiftRight,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_concat_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_add_expr()?;
        while self.current() == &Token::PipePipe {
            self.advance();
            let right = self.parse_add_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::Concat,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_add_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_mul_expr()?;
        loop {
            match self.current() {
                Token::Plus => {
                    self.advance();
                    let right = self.parse_mul_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Add,
                        right: Box::new(right),
                    };
                }
                Token::Minus => {
                    self.advance();
                    let right = self.parse_mul_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Sub,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_mul_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;
        loop {
            match self.current() {
                Token::Star => {
                    self.advance();
                    let right = self.parse_unary_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Mul,
                        right: Box::new(right),
                    };
                }
                Token::Slash => {
                    self.advance();
                    let right = self.parse_unary_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Div,
                        right: Box::new(right),
                    };
                }
                Token::Percent => {
                    self.advance();
                    let right = self.parse_unary_expr()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOp::Mod,
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr> {
        match self.current() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                })
            }
            Token::Tilde => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::BitNot,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expr(),
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        match self.current().clone() {
            // Integer literal
            Token::IntegerLiteral(n) => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Integer(n)))
            }
            // Real literal
            Token::RealLiteral(n) => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Real(n)))
            }
            // String literal
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::String(s)))
            }
            // Blob literal
            Token::BlobLiteral(b) => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Blob(b)))
            }
            // NULL
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Null))
            }
            // TRUE
            Token::True => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::True))
            }
            // FALSE
            Token::False => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::False))
            }
            // Placeholder
            Token::Placeholder(n) => {
                self.advance();
                Ok(Expr::Placeholder(n))
            }
            // Parenthesised expression or subquery
            Token::LeftParen => {
                self.advance();
                if self.current() == &Token::Select {
                    let query = self.parse_select()?;
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::Subquery(Box::new(query)))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RightParen)?;
                    Ok(expr)
                }
            }
            // CAST(expr AS type)
            Token::Cast => {
                self.advance();
                self.expect(&Token::LeftParen)?;
                let expr = self.parse_expr()?;
                self.expect(&Token::As)?;
                let type_name = self.expect_identifier()?.to_ascii_uppercase();
                self.expect(&Token::RightParen)?;
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    type_name,
                })
            }
            // CASE [operand] WHEN ... THEN ... [ELSE ...] END
            Token::Case => {
                self.advance();
                let operand = if self.current() != &Token::When {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                let mut when_clauses = Vec::new();
                while self.current() == &Token::When {
                    self.advance();
                    let when_expr = self.parse_expr()?;
                    self.expect(&Token::Then)?;
                    let then_expr = self.parse_expr()?;
                    when_clauses.push((when_expr, then_expr));
                }
                let else_clause = if self.current() == &Token::Else {
                    self.advance();
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                self.expect(&Token::End)?;
                Ok(Expr::Case {
                    operand,
                    when_clauses,
                    else_clause,
                })
            }
            // EXISTS(subquery)
            Token::Exists => {
                self.advance();
                self.expect(&Token::LeftParen)?;
                let query = self.parse_select()?;
                self.expect(&Token::RightParen)?;
                Ok(Expr::Exists(Box::new(query)))
            }
            // Identifier — column reference or function call
            Token::Identifier(name) => {
                self.advance();
                // Function call: name(...)
                if self.current() == &Token::LeftParen {
                    self.advance();
                    let distinct = if self.current() == &Token::Distinct {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let args = if self.current() == &Token::RightParen {
                        vec![]
                    } else if self.current() == &Token::Star {
                        // e.g. COUNT(*)
                        self.advance();
                        vec![Expr::Column {
                            table: None,
                            name: "*".into(),
                        }]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.expect(&Token::RightParen)?;
                    return Ok(Expr::Function {
                        name: name.to_ascii_uppercase(),
                        args,
                        distinct,
                    });
                }
                // Qualified column: table.column
                if self.current() == &Token::Dot {
                    self.advance();
                    let col_name = self.expect_identifier()?;
                    return Ok(Expr::Column {
                        table: Some(name),
                        name: col_name,
                    });
                }
                // Simple column reference
                Ok(Expr::Column { table: None, name })
            }
            // Star — when used in expression context (e.g. COUNT(*) is handled
            // above, but if somehow we land here, treat it as a column ref).
            Token::Star => {
                self.advance();
                Ok(Expr::Column {
                    table: None,
                    name: "*".into(),
                })
            }
            _ => Err(self.error(format!(
                "unexpected token in expression: {:?}",
                self.current()
            ))),
        }
    }

    // =======================================================================
    // Helper: expression list
    // =======================================================================

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expr()?];
        while self.current() == &Token::Comma {
            self.advance();
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        let stmts = Parser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "expected exactly 1 statement");
        stmts.into_iter().next().unwrap()
    }

    // -- SELECT tests -------------------------------------------------------

    #[test]
    fn parse_simple_select() {
        let stmt = parse_one("SELECT * FROM users");
        if let Statement::Select(sel) = stmt {
            assert!(!sel.distinct);
            assert_eq!(sel.columns, vec![SelectColumn::AllColumns]);
            assert!(matches!(sel.from, Some(FromClause::Table { ref name, .. }) if name == "users"));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_with_where_and_or() {
        let stmt = parse_one("SELECT id FROM users WHERE age > 18 AND name = 'Alice' OR active = 1");
        if let Statement::Select(sel) = stmt {
            // The WHERE clause should be: ((age > 18) AND (name = 'Alice')) OR (active = 1)
            assert!(sel.where_clause.is_some());
            let w = sel.where_clause.unwrap();
            // Top-level should be OR
            if let Expr::BinaryOp { op, .. } = &w {
                assert_eq!(*op, BinaryOp::Or);
            } else {
                panic!("expected OR at top level");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_distinct_with_order_limit_offset() {
        let stmt = parse_one("SELECT DISTINCT name FROM users ORDER BY name ASC LIMIT 10 OFFSET 5");
        if let Statement::Select(sel) = stmt {
            assert!(sel.distinct);
            assert_eq!(sel.order_by.len(), 1);
            assert!(!sel.order_by[0].desc);
            assert_eq!(sel.limit, Some(Expr::Literal(LiteralValue::Integer(10))));
            assert_eq!(sel.offset, Some(Expr::Literal(LiteralValue::Integer(5))));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_with_join() {
        let stmt = parse_one(
            "SELECT u.id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
        );
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.columns.len(), 2);
            if let Some(FromClause::Join { join_type, on, .. }) = &sel.from {
                assert_eq!(*join_type, JoinType::Inner);
                assert!(on.is_some());
            } else {
                panic!("expected Join");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_with_group_by_having() {
        let stmt = parse_one(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        );
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.group_by.len(), 1);
            assert!(sel.having.is_some());
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_table_star() {
        let stmt = parse_one("SELECT u.* FROM users u");
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.columns, vec![SelectColumn::TableAllColumns("u".into())]);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn parse_select_with_alias() {
        let stmt = parse_one("SELECT id AS user_id FROM users");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { alias, .. } = &sel.columns[0] {
                assert_eq!(alias.as_deref(), Some("user_id"));
            } else {
                panic!("expected Expr column");
            }
        } else {
            panic!("expected Select");
        }
    }

    // -- INSERT tests -------------------------------------------------------

    #[test]
    fn parse_simple_insert() {
        let stmt = parse_one("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        if let Statement::Insert(ins) = stmt {
            assert_eq!(ins.table, "users");
            assert_eq!(ins.columns, Some(vec!["id".into(), "name".into()]));
            assert_eq!(ins.values.len(), 1);
            assert_eq!(ins.values[0].len(), 2);
            assert!(!ins.or_replace);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn parse_insert_multiple_rows() {
        let stmt = parse_one("INSERT INTO t VALUES (1, 2), (3, 4), (5, 6)");
        if let Statement::Insert(ins) = stmt {
            assert_eq!(ins.values.len(), 3);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn parse_insert_or_replace() {
        let stmt = parse_one("INSERT OR REPLACE INTO users (id) VALUES (1)");
        if let Statement::Insert(ins) = stmt {
            assert!(ins.or_replace);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn parse_replace_into() {
        let stmt = parse_one("REPLACE INTO users (id) VALUES (1)");
        if let Statement::Insert(ins) = stmt {
            assert!(ins.or_replace);
        } else {
            panic!("expected Insert");
        }
    }

    // -- UPDATE tests -------------------------------------------------------

    #[test]
    fn parse_simple_update() {
        let stmt = parse_one("UPDATE users SET name = 'Bob' WHERE id = 1");
        if let Statement::Update(upd) = stmt {
            assert_eq!(upd.table, "users");
            assert_eq!(upd.assignments.len(), 1);
            assert_eq!(upd.assignments[0].0, "name");
            assert!(upd.where_clause.is_some());
        } else {
            panic!("expected Update");
        }
    }

    #[test]
    fn parse_update_multiple_assignments() {
        let stmt = parse_one("UPDATE t SET a = 1, b = 2, c = 3");
        if let Statement::Update(upd) = stmt {
            assert_eq!(upd.assignments.len(), 3);
        } else {
            panic!("expected Update");
        }
    }

    // -- DELETE tests -------------------------------------------------------

    #[test]
    fn parse_simple_delete() {
        let stmt = parse_one("DELETE FROM users WHERE id = 1");
        if let Statement::Delete(del) = stmt {
            assert_eq!(del.table, "users");
            assert!(del.where_clause.is_some());
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn parse_delete_no_where() {
        let stmt = parse_one("DELETE FROM users");
        if let Statement::Delete(del) = stmt {
            assert_eq!(del.table, "users");
            assert!(del.where_clause.is_none());
        } else {
            panic!("expected Delete");
        }
    }

    // -- CREATE TABLE tests -------------------------------------------------

    #[test]
    fn parse_create_table() {
        let stmt = parse_one(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER DEFAULT 0
            )",
        );
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.name, "users");
            assert!(!ct.if_not_exists);
            assert_eq!(ct.columns.len(), 4);

            let id_col = &ct.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.type_name, Some("INTEGER".into()));
            assert!(id_col.primary_key);
            assert!(id_col.autoincrement);

            let name_col = &ct.columns[1];
            assert_eq!(name_col.name, "name");
            assert_eq!(name_col.type_name, Some("TEXT".into()));
            assert!(name_col.not_null);

            let email_col = &ct.columns[2];
            assert!(email_col.unique);

            let age_col = &ct.columns[3];
            assert_eq!(
                age_col.default,
                Some(Expr::Literal(LiteralValue::Integer(0)))
            );
        } else {
            panic!("expected CreateTable");
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let stmt = parse_one("CREATE TABLE IF NOT EXISTS t (id INTEGER)");
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.if_not_exists);
        } else {
            panic!("expected CreateTable");
        }
    }

    // -- DROP TABLE / INDEX tests -------------------------------------------

    #[test]
    fn parse_drop_table() {
        let stmt = parse_one("DROP TABLE users");
        if let Statement::DropTable(dt) = stmt {
            assert_eq!(dt.name, "users");
            assert!(!dt.if_exists);
        } else {
            panic!("expected DropTable");
        }
    }

    #[test]
    fn parse_drop_table_if_exists() {
        let stmt = parse_one("DROP TABLE IF EXISTS users");
        if let Statement::DropTable(dt) = stmt {
            assert!(dt.if_exists);
        } else {
            panic!("expected DropTable");
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse_one("DROP INDEX idx_users_email");
        if let Statement::DropIndex(di) = stmt {
            assert_eq!(di.name, "idx_users_email");
            assert!(!di.if_exists);
        } else {
            panic!("expected DropIndex");
        }
    }

    // -- CREATE INDEX tests -------------------------------------------------

    #[test]
    fn parse_create_index() {
        let stmt = parse_one("CREATE INDEX idx_name ON users (name)");
        if let Statement::CreateIndex(ci) = stmt {
            assert_eq!(ci.name, "idx_name");
            assert_eq!(ci.table, "users");
            assert!(!ci.unique);
            assert!(!ci.if_not_exists);
            assert_eq!(ci.columns.len(), 1);
        } else {
            panic!("expected CreateIndex");
        }
    }

    #[test]
    fn parse_create_unique_index() {
        let stmt = parse_one("CREATE UNIQUE INDEX IF NOT EXISTS idx_email ON users (email DESC)");
        if let Statement::CreateIndex(ci) = stmt {
            assert!(ci.unique);
            assert!(ci.if_not_exists);
            assert!(ci.columns[0].desc);
        } else {
            panic!("expected CreateIndex");
        }
    }

    // -- Transaction tests --------------------------------------------------

    #[test]
    fn parse_begin_commit_rollback() {
        let stmts = Parser::parse("BEGIN; COMMIT; ROLLBACK").unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], Statement::Begin);
        assert_eq!(stmts[1], Statement::Commit);
        assert_eq!(stmts[2], Statement::Rollback);
    }

    #[test]
    fn parse_begin_transaction() {
        let stmts = Parser::parse("BEGIN TRANSACTION").unwrap();
        assert_eq!(stmts, vec![Statement::Begin]);
    }

    // -- Multiple statements ------------------------------------------------

    #[test]
    fn parse_multiple_statements() {
        let stmts =
            Parser::parse("SELECT 1; SELECT 2; SELECT 3").unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn parse_trailing_semicolons() {
        let stmts = Parser::parse("SELECT 1;;;").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    // -- Expression tests ---------------------------------------------------

    #[test]
    fn parse_expression_precedence() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let stmt = parse_one("SELECT 1 + 2 * 3");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::BinaryOp { op, right, .. } = expr {
                    assert_eq!(*op, BinaryOp::Add);
                    if let Expr::BinaryOp { op: inner_op, .. } = right.as_ref() {
                        assert_eq!(*inner_op, BinaryOp::Mul);
                    } else {
                        panic!("expected Mul on right");
                    }
                } else {
                    panic!("expected BinaryOp Add");
                }
            }
        }
    }

    #[test]
    fn parse_unary_minus() {
        let stmt = parse_one("SELECT -42");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::UnaryOp { op, expr: inner } = expr {
                    assert_eq!(*op, UnaryOp::Neg);
                    assert_eq!(**inner, Expr::Literal(LiteralValue::Integer(42)));
                } else {
                    panic!("expected UnaryOp Neg");
                }
            }
        }
    }

    #[test]
    fn parse_not_expression() {
        let stmt = parse_one("SELECT * FROM t WHERE NOT active");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::UnaryOp { op, .. }) = &sel.where_clause {
                assert_eq!(*op, UnaryOp::Not);
            } else {
                panic!("expected UnaryOp Not");
            }
        }
    }

    #[test]
    fn parse_between_expression() {
        let stmt = parse_one("SELECT * FROM t WHERE age BETWEEN 18 AND 65");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::Between { negated, .. }) = &sel.where_clause {
                assert!(!negated);
            } else {
                panic!("expected Between");
            }
        }
    }

    #[test]
    fn parse_not_between() {
        let stmt = parse_one("SELECT * FROM t WHERE age NOT BETWEEN 18 AND 65");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::Between { negated, .. }) = &sel.where_clause {
                assert!(negated);
            } else {
                panic!("expected Between negated");
            }
        }
    }

    #[test]
    fn parse_in_list() {
        let stmt = parse_one("SELECT * FROM t WHERE id IN (1, 2, 3)");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::InList { list, negated, .. }) = &sel.where_clause {
                assert!(!negated);
                assert_eq!(list.len(), 3);
            } else {
                panic!("expected InList");
            }
        }
    }

    #[test]
    fn parse_not_in() {
        let stmt = parse_one("SELECT * FROM t WHERE id NOT IN (1, 2)");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::InList { negated, .. }) = &sel.where_clause {
                assert!(negated);
            } else {
                panic!("expected InList negated");
            }
        }
    }

    #[test]
    fn parse_like_expression() {
        let stmt = parse_one("SELECT * FROM t WHERE name LIKE '%alice%'");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::Like { negated, .. }) = &sel.where_clause {
                assert!(!negated);
            } else {
                panic!("expected Like");
            }
        }
    }

    #[test]
    fn parse_is_null_is_not_null() {
        let stmt = parse_one("SELECT * FROM t WHERE a IS NULL AND b IS NOT NULL");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::BinaryOp { left, right, .. }) = &sel.where_clause {
                if let Expr::IsNull { negated, .. } = left.as_ref() {
                    assert!(!negated);
                } else {
                    panic!("expected IsNull on left");
                }
                if let Expr::IsNull { negated, .. } = right.as_ref() {
                    assert!(negated);
                } else {
                    panic!("expected IsNull negated on right");
                }
            }
        }
    }

    #[test]
    fn parse_function_call() {
        let stmt = parse_one("SELECT COUNT(DISTINCT id) FROM t");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::Function {
                    name, distinct, args, ..
                } = expr
                {
                    assert_eq!(name, "COUNT");
                    assert!(distinct);
                    assert_eq!(args.len(), 1);
                } else {
                    panic!("expected Function");
                }
            }
        }
    }

    #[test]
    fn parse_cast_expression() {
        let stmt = parse_one("SELECT CAST(age AS integer)");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::Cast { type_name, .. } = expr {
                    assert_eq!(type_name, "INTEGER");
                } else {
                    panic!("expected Cast");
                }
            }
        }
    }

    #[test]
    fn parse_case_expression() {
        let stmt = parse_one(
            "SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END FROM t",
        );
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::Case {
                    operand,
                    when_clauses,
                    else_clause,
                } = expr
                {
                    assert!(operand.is_none());
                    assert_eq!(when_clauses.len(), 1);
                    assert!(else_clause.is_some());
                } else {
                    panic!("expected Case");
                }
            }
        }
    }

    #[test]
    fn parse_exists_subquery() {
        let stmt = parse_one("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::Exists(..)) = &sel.where_clause {
                // ok
            } else {
                panic!("expected Exists");
            }
        }
    }

    #[test]
    fn parse_qualified_column() {
        let stmt = parse_one("SELECT t.id FROM t");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::Column { table, name } = expr {
                    assert_eq!(table.as_deref(), Some("t"));
                    assert_eq!(name, "id");
                } else {
                    panic!("expected Column");
                }
            }
        }
    }

    #[test]
    fn parse_concat_operator() {
        let stmt = parse_one("SELECT 'a' || 'b'");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::BinaryOp { op, .. } = expr {
                    assert_eq!(*op, BinaryOp::Concat);
                } else {
                    panic!("expected Concat");
                }
            }
        }
    }

    #[test]
    fn parse_string_number_blob_literals() {
        let stmt = parse_one("SELECT 'hello', 42, 3.14, X'CAFE'");
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.columns.len(), 4);
        }
    }

    #[test]
    fn parse_placeholder() {
        let stmt = parse_one("SELECT * FROM t WHERE id = ?1");
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::BinaryOp { right, .. }) = &sel.where_clause {
                assert_eq!(**right, Expr::Placeholder(1));
            } else {
                panic!("expected BinaryOp with placeholder");
            }
        }
    }

    #[test]
    fn parse_left_join() {
        let stmt = parse_one("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id");
        if let Statement::Select(sel) = stmt {
            if let Some(FromClause::Join { join_type, .. }) = &sel.from {
                assert_eq!(*join_type, JoinType::Left);
            } else {
                panic!("expected Left Join");
            }
        }
    }

    #[test]
    fn parse_cross_join() {
        let stmt = parse_one("SELECT * FROM a CROSS JOIN b");
        if let Statement::Select(sel) = stmt {
            if let Some(FromClause::Join { join_type, on, .. }) = &sel.from {
                assert_eq!(*join_type, JoinType::Cross);
                assert!(on.is_none());
            } else {
                panic!("expected Cross Join");
            }
        }
    }

    #[test]
    fn error_on_invalid_sql() {
        let result = Parser::parse("FROBNICATE THE WIDGETS");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_input() {
        let stmts = Parser::parse("").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn parse_whitespace_only() {
        let stmts = Parser::parse("   \n\t  ").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn parse_subquery_in_from() {
        let stmt = parse_one("SELECT x FROM (SELECT 1 AS x) sub");
        if let Statement::Select(sel) = stmt {
            if let Some(FromClause::Subquery { alias, .. }) = &sel.from {
                assert_eq!(alias, "sub");
            } else {
                panic!("expected Subquery from");
            }
        }
    }

    #[test]
    fn parse_nested_parens_in_expr() {
        let stmt = parse_one("SELECT (1 + (2 * 3))");
        if let Statement::Select(sel) = stmt {
            if let SelectColumn::Expr { expr, .. } = &sel.columns[0] {
                if let Expr::BinaryOp { op, .. } = expr {
                    assert_eq!(*op, BinaryOp::Add);
                } else {
                    panic!("expected BinaryOp Add");
                }
            }
        }
    }
}
