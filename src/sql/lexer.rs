//! Hand-written SQL tokenizer for Horizon DB.
//!
//! The [`Lexer`] takes a raw SQL string and produces a `Vec<Token>`.
//! It is case-insensitive for keywords and handles all standard SQL literal
//! forms including strings, numbers, blobs, and placeholders.

use crate::error::{HorizonError, Result};

/// A single SQL token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // -----------------------------------------------------------------------
    // Keywords
    // -----------------------------------------------------------------------
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Table,
    Index,
    View,
    Trigger,
    If,
    Exists,
    Not,
    And,
    Or,
    Is,
    Null,
    In,
    Between,
    Like,
    As,
    On,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    Full,
    Natural,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Limit,
    Offset,
    Distinct,
    Union,
    Intersect,
    Except,
    All,
    Primary,
    Key,
    Unique,
    Check,
    Default,
    Foreign,
    References,
    Autoincrement,
    IntegerKw,
    TextKw,
    RealKw,
    BlobKw,
    NumericKw,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Savepoint,
    Release,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    With,
    Recursive,
    Alter,
    Add,
    Rename,
    Column,
    To,
    Explain,
    Query,
    Plan,
    Pragma,
    Vacuum,
    Attach,
    Detach,
    Database,
    Replace,
    Abort,
    Fail,
    Ignore,
    Conflict,
    Returning,
    Window,
    Over,
    Partition,
    Rows,
    Range,
    Unbounded,
    Preceding,
    Following,
    Current,
    Row,
    Collate,
    Glob,
    Escape,
    True,
    False,
    Before,
    After,
    Instead,
    For,
    Of,
    Generated,
    Always,
    Stored,
    Virtual,

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------
    IntegerLiteral(i64),
    RealLiteral(f64),
    StringLiteral(String),
    BlobLiteral(Vec<u8>),

    // -----------------------------------------------------------------------
    // Identifiers
    // -----------------------------------------------------------------------
    Identifier(String),

    // -----------------------------------------------------------------------
    // Operators & punctuation
    // -----------------------------------------------------------------------
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,
    Pipe,
    PipePipe,
    Ampersand,
    Tilde,
    ShiftLeft,
    ShiftRight,

    // -----------------------------------------------------------------------
    // Special
    // -----------------------------------------------------------------------
    /// Positional placeholder `?` or numbered `?1`, `?2`, etc.
    Placeholder(usize),
    /// End-of-file sentinel.
    Eof,
}

// ---------------------------------------------------------------------------
// Keyword lookup
// ---------------------------------------------------------------------------

fn keyword_token(word: &str) -> Option<Token> {
    // The input `word` is already uppercased by the caller.
    match word {
        "SELECT" => Some(Token::Select),
        "FROM" => Some(Token::From),
        "WHERE" => Some(Token::Where),
        "INSERT" => Some(Token::Insert),
        "INTO" => Some(Token::Into),
        "VALUES" => Some(Token::Values),
        "UPDATE" => Some(Token::Update),
        "SET" => Some(Token::Set),
        "DELETE" => Some(Token::Delete),
        "CREATE" => Some(Token::Create),
        "DROP" => Some(Token::Drop),
        "TABLE" => Some(Token::Table),
        "INDEX" => Some(Token::Index),
        "VIEW" => Some(Token::View),
        "TRIGGER" => Some(Token::Trigger),
        "IF" => Some(Token::If),
        "EXISTS" => Some(Token::Exists),
        "NOT" => Some(Token::Not),
        "AND" => Some(Token::And),
        "OR" => Some(Token::Or),
        "IS" => Some(Token::Is),
        "NULL" => Some(Token::Null),
        "IN" => Some(Token::In),
        "BETWEEN" => Some(Token::Between),
        "LIKE" => Some(Token::Like),
        "AS" => Some(Token::As),
        "ON" => Some(Token::On),
        "JOIN" => Some(Token::Join),
        "INNER" => Some(Token::Inner),
        "LEFT" => Some(Token::Left),
        "RIGHT" => Some(Token::Right),
        "OUTER" => Some(Token::Outer),
        "CROSS" => Some(Token::Cross),
        "FULL" => Some(Token::Full),
        "NATURAL" => Some(Token::Natural),
        "ORDER" => Some(Token::Order),
        "BY" => Some(Token::By),
        "ASC" => Some(Token::Asc),
        "DESC" => Some(Token::Desc),
        "GROUP" => Some(Token::Group),
        "HAVING" => Some(Token::Having),
        "LIMIT" => Some(Token::Limit),
        "OFFSET" => Some(Token::Offset),
        "DISTINCT" => Some(Token::Distinct),
        "UNION" => Some(Token::Union),
        "INTERSECT" => Some(Token::Intersect),
        "EXCEPT" => Some(Token::Except),
        "ALL" => Some(Token::All),
        "PRIMARY" => Some(Token::Primary),
        "KEY" => Some(Token::Key),
        "UNIQUE" => Some(Token::Unique),
        "CHECK" => Some(Token::Check),
        "DEFAULT" => Some(Token::Default),
        "FOREIGN" => Some(Token::Foreign),
        "REFERENCES" => Some(Token::References),
        "AUTOINCREMENT" => Some(Token::Autoincrement),
        "INTEGER" => Some(Token::IntegerKw),
        "TEXT" => Some(Token::TextKw),
        "REAL" => Some(Token::RealKw),
        "BLOB" => Some(Token::BlobKw),
        "NUMERIC" => Some(Token::NumericKw),
        "BEGIN" => Some(Token::Begin),
        "COMMIT" => Some(Token::Commit),
        "ROLLBACK" => Some(Token::Rollback),
        "TRANSACTION" => Some(Token::Transaction),
        "SAVEPOINT" => Some(Token::Savepoint),
        "RELEASE" => Some(Token::Release),
        "CASE" => Some(Token::Case),
        "WHEN" => Some(Token::When),
        "THEN" => Some(Token::Then),
        "ELSE" => Some(Token::Else),
        "END" => Some(Token::End),
        "CAST" => Some(Token::Cast),
        "WITH" => Some(Token::With),
        "RECURSIVE" => Some(Token::Recursive),
        "ALTER" => Some(Token::Alter),
        "ADD" => Some(Token::Add),
        "RENAME" => Some(Token::Rename),
        "COLUMN" => Some(Token::Column),
        "TO" => Some(Token::To),
        "EXPLAIN" => Some(Token::Explain),
        "QUERY" => Some(Token::Query),
        "PLAN" => Some(Token::Plan),
        "PRAGMA" => Some(Token::Pragma),
        "VACUUM" => Some(Token::Vacuum),
        "ATTACH" => Some(Token::Attach),
        "DETACH" => Some(Token::Detach),
        "DATABASE" => Some(Token::Database),
        "REPLACE" => Some(Token::Replace),
        "ABORT" => Some(Token::Abort),
        "FAIL" => Some(Token::Fail),
        "IGNORE" => Some(Token::Ignore),
        "CONFLICT" => Some(Token::Conflict),
        "RETURNING" => Some(Token::Returning),
        "WINDOW" => Some(Token::Window),
        "OVER" => Some(Token::Over),
        "PARTITION" => Some(Token::Partition),
        "ROWS" => Some(Token::Rows),
        "RANGE" => Some(Token::Range),
        "UNBOUNDED" => Some(Token::Unbounded),
        "PRECEDING" => Some(Token::Preceding),
        "FOLLOWING" => Some(Token::Following),
        "CURRENT" => Some(Token::Current),
        "ROW" => Some(Token::Row),
        "COLLATE" => Some(Token::Collate),
        "GLOB" => Some(Token::Glob),
        "ESCAPE" => Some(Token::Escape),
        "TRUE" => Some(Token::True),
        "FALSE" => Some(Token::False),
        "BEFORE" => Some(Token::Before),
        "AFTER" => Some(Token::After),
        "INSTEAD" => Some(Token::Instead),
        "FOR" => Some(Token::For),
        "OF" => Some(Token::Of),
        "GENERATED" => Some(Token::Generated),
        "ALWAYS" => Some(Token::Always),
        "STORED" => Some(Token::Stored),
        "VIRTUAL" => Some(Token::Virtual),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

/// A hand-written SQL tokenizer.
///
/// Create one with [`Lexer::new`], then call [`Lexer::tokenize`] to obtain
/// the full token stream (terminated by [`Token::Eof`]).
pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
    /// Auto-incrementing counter for bare `?` placeholders.
    placeholder_counter: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer over the given SQL text.
    pub fn new(input: &'a str) -> Self {
        Lexer {
            input: input.as_bytes(),
            pos: 0,
            placeholder_counter: 0,
        }
    }

    /// Tokenize the entire input and return the token list.
    ///
    /// The returned vector always ends with [`Token::Eof`].
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = tok == Token::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    // -- helpers ------------------------------------------------------------

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.input.get(self.pos + offset).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let ch = self.input.get(self.pos).copied()?;
        self.pos += 1;
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Skip `-- line comments` and `/* block comments */`, returning `true`
    /// if a comment was actually consumed so the caller can loop.
    fn skip_comment(&mut self) -> Result<bool> {
        if self.peek() == Some(b'-') && self.peek_at(1) == Some(b'-') {
            // Line comment — consume until end of line or end of input.
            self.pos += 2;
            while let Some(ch) = self.peek() {
                self.pos += 1;
                if ch == b'\n' {
                    break;
                }
            }
            return Ok(true);
        }
        if self.peek() == Some(b'/') && self.peek_at(1) == Some(b'*') {
            // Block comment — consume until `*/`.
            self.pos += 2;
            loop {
                match self.peek() {
                    None => {
                        return Err(HorizonError::InvalidSql(
                            "unterminated block comment".into(),
                        ));
                    }
                    Some(b'*') if self.peek_at(1) == Some(b'/') => {
                        self.pos += 2;
                        break;
                    }
                    _ => {
                        self.pos += 1;
                    }
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<()> {
        loop {
            self.skip_whitespace();
            if !self.skip_comment()? {
                break;
            }
        }
        Ok(())
    }

    // -- main scanner -------------------------------------------------------

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments()?;

        let ch = match self.peek() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        // ----- string literal -----
        if ch == b'\'' {
            return self.read_string_literal();
        }

        // ----- blob literal X'...' -----
        if (ch == b'x' || ch == b'X') && self.peek_at(1) == Some(b'\'') {
            return self.read_blob_literal();
        }

        // ----- numeric literal -----
        if ch.is_ascii_digit() {
            return self.read_number();
        }

        // ----- dot — could be `.123` float or just a dot -----
        if ch == b'.' && self.peek_at(1).map_or(false, |c| c.is_ascii_digit()) {
            return self.read_number();
        }

        // ----- identifier / keyword -----
        if ch.is_ascii_alphabetic() || ch == b'_' {
            return self.read_identifier_or_keyword();
        }

        // ----- double-quoted identifier -----
        if ch == b'"' {
            return self.read_quoted_identifier();
        }

        // ----- placeholder ?N -----
        if ch == b'?' {
            return self.read_placeholder();
        }

        // ----- operators & punctuation -----
        self.read_operator()
    }

    // -- literal readers ----------------------------------------------------

    fn read_string_literal(&mut self) -> Result<Token> {
        self.advance(); // consume opening '
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(HorizonError::InvalidSql(
                        "unterminated string literal".into(),
                    ));
                }
                Some(b'\'') => {
                    // Check for escaped quote ('')
                    if self.peek() == Some(b'\'') {
                        self.advance();
                        s.push('\'');
                    } else {
                        break;
                    }
                }
                Some(c) => {
                    s.push(c as char);
                }
            }
        }
        Ok(Token::StringLiteral(s))
    }

    fn read_blob_literal(&mut self) -> Result<Token> {
        self.advance(); // consume 'X' or 'x'
        self.advance(); // consume opening '
        let mut hex = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(HorizonError::InvalidSql(
                        "unterminated blob literal".into(),
                    ));
                }
                Some(b'\'') => break,
                Some(c) if c.is_ascii_hexdigit() => hex.push(c as char),
                Some(c) => {
                    return Err(HorizonError::InvalidSql(format!(
                        "invalid character in blob literal: '{}'",
                        c as char
                    )));
                }
            }
        }
        if hex.len() % 2 != 0 {
            return Err(HorizonError::InvalidSql(
                "blob literal must have an even number of hex digits".into(),
            ));
        }
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        Ok(Token::BlobLiteral(bytes))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        let mut is_real = false;

        // Integer part
        while self.peek().map_or(false, |c| c.is_ascii_digit()) {
            self.advance();
        }

        // Fractional part
        if self.peek() == Some(b'.')
            && self.peek_at(1).map_or(false, |c| c.is_ascii_digit())
        {
            is_real = true;
            self.advance(); // consume '.'
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                self.advance();
            }
        } else if self.peek() == Some(b'.') && self.pos > start {
            // e.g. "123." — still a real
            // Only treat as real if we already consumed at least one digit
            is_real = true;
            self.advance();
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                self.advance();
            }
        }

        // Exponent part
        if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            is_real = true;
            self.advance();
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                self.advance();
            }
            if !self.peek().map_or(false, |c| c.is_ascii_digit()) {
                return Err(HorizonError::InvalidSql(
                    "invalid numeric literal: expected digit after exponent".into(),
                ));
            }
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                self.advance();
            }
        }

        let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();

        if is_real {
            let val: f64 = text.parse().map_err(|_| {
                HorizonError::InvalidSql(format!("invalid real literal: {text}"))
            })?;
            Ok(Token::RealLiteral(val))
        } else {
            let val: i64 = text.parse().map_err(|_| {
                HorizonError::InvalidSql(format!("invalid integer literal: {text}"))
            })?;
            Ok(Token::IntegerLiteral(val))
        }
    }

    fn read_identifier_or_keyword(&mut self) -> Result<Token> {
        let start = self.pos;
        while self
            .peek()
            .map_or(false, |c| c.is_ascii_alphanumeric() || c == b'_')
        {
            self.advance();
        }
        let word = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
        let upper = word.to_ascii_uppercase();

        if let Some(kw) = keyword_token(&upper) {
            Ok(kw)
        } else {
            Ok(Token::Identifier(word.to_string()))
        }
    }

    fn read_quoted_identifier(&mut self) -> Result<Token> {
        self.advance(); // consume opening "
        let mut name = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(HorizonError::InvalidSql(
                        "unterminated quoted identifier".into(),
                    ));
                }
                Some(b'"') => {
                    // Doubled quote escapes itself
                    if self.peek() == Some(b'"') {
                        self.advance();
                        name.push('"');
                    } else {
                        break;
                    }
                }
                Some(c) => name.push(c as char),
            }
        }
        Ok(Token::Identifier(name))
    }

    fn read_placeholder(&mut self) -> Result<Token> {
        self.advance(); // consume '?'
        let start = self.pos;
        while self.peek().map_or(false, |c| c.is_ascii_digit()) {
            self.advance();
        }
        if self.pos > start {
            let num: usize = std::str::from_utf8(&self.input[start..self.pos])
                .unwrap()
                .parse()
                .map_err(|_| {
                    HorizonError::InvalidSql("invalid placeholder number".into())
                })?;
            Ok(Token::Placeholder(num))
        } else {
            self.placeholder_counter += 1;
            Ok(Token::Placeholder(self.placeholder_counter))
        }
    }

    fn read_operator(&mut self) -> Result<Token> {
        let ch = self.advance().unwrap();
        match ch {
            b'+' => Ok(Token::Plus),
            b'*' => Ok(Token::Star),
            b'/' => Ok(Token::Slash),
            b'%' => Ok(Token::Percent),
            b'(' => Ok(Token::LeftParen),
            b')' => Ok(Token::RightParen),
            b',' => Ok(Token::Comma),
            b';' => Ok(Token::Semicolon),
            b'.' => Ok(Token::Dot),
            b'&' => Ok(Token::Ampersand),
            b'~' => Ok(Token::Tilde),
            b'-' => Ok(Token::Minus),
            b'=' => {
                if self.peek() == Some(b'=') {
                    self.advance();
                }
                Ok(Token::Eq)
            }
            b'!' => {
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::NotEq)
                } else {
                    Err(HorizonError::InvalidSql(
                        "expected '=' after '!'".into(),
                    ))
                }
            }
            b'<' => {
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::LtEq)
                } else if self.peek() == Some(b'>') {
                    self.advance();
                    Ok(Token::NotEq)
                } else if self.peek() == Some(b'<') {
                    self.advance();
                    Ok(Token::ShiftLeft)
                } else {
                    Ok(Token::Lt)
                }
            }
            b'>' => {
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::GtEq)
                } else if self.peek() == Some(b'>') {
                    self.advance();
                    Ok(Token::ShiftRight)
                } else {
                    Ok(Token::Gt)
                }
            }
            b'|' => {
                if self.peek() == Some(b'|') {
                    self.advance();
                    Ok(Token::PipePipe)
                } else {
                    Ok(Token::Pipe)
                }
            }
            _ => Err(HorizonError::InvalidSql(format!(
                "unexpected character: '{}'",
                ch as char
            ))),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input).tokenize().unwrap()
    }

    #[test]
    fn keywords_are_case_insensitive() {
        let tokens = lex("select FROM Where");
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::From);
        assert_eq!(tokens[2], Token::Where);
    }

    #[test]
    fn identifiers() {
        let tokens = lex("my_table \"My Column\"");
        assert_eq!(tokens[0], Token::Identifier("my_table".into()));
        assert_eq!(tokens[1], Token::Identifier("My Column".into()));
    }

    #[test]
    fn integer_and_real_literals() {
        let tokens = lex("42 3.14 .5 1e10 2.5E-3");
        assert_eq!(tokens[0], Token::IntegerLiteral(42));
        assert_eq!(tokens[1], Token::RealLiteral(3.14));
        assert_eq!(tokens[2], Token::RealLiteral(0.5));
        assert_eq!(tokens[3], Token::RealLiteral(1e10));
        assert_eq!(tokens[4], Token::RealLiteral(2.5e-3));
    }

    #[test]
    fn string_literal_with_escape() {
        let tokens = lex("'hello' 'it''s'");
        assert_eq!(tokens[0], Token::StringLiteral("hello".into()));
        assert_eq!(tokens[1], Token::StringLiteral("it's".into()));
    }

    #[test]
    fn blob_literal() {
        let tokens = lex("X'DEADBEEF'");
        assert_eq!(tokens[0], Token::BlobLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn operators() {
        let tokens = lex("+ - * / % = != <> < > <= >= || << >>");
        assert_eq!(tokens[0], Token::Plus);
        assert_eq!(tokens[1], Token::Minus);
        assert_eq!(tokens[2], Token::Star);
        assert_eq!(tokens[3], Token::Slash);
        assert_eq!(tokens[4], Token::Percent);
        assert_eq!(tokens[5], Token::Eq);
        assert_eq!(tokens[6], Token::NotEq);
        assert_eq!(tokens[7], Token::NotEq);
        assert_eq!(tokens[8], Token::Lt);
        assert_eq!(tokens[9], Token::Gt);
        assert_eq!(tokens[10], Token::LtEq);
        assert_eq!(tokens[11], Token::GtEq);
        assert_eq!(tokens[12], Token::PipePipe);
        assert_eq!(tokens[13], Token::ShiftLeft);
        assert_eq!(tokens[14], Token::ShiftRight);
    }

    #[test]
    fn punctuation() {
        let tokens = lex("( ) , ; .");
        assert_eq!(tokens[0], Token::LeftParen);
        assert_eq!(tokens[1], Token::RightParen);
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Semicolon);
        assert_eq!(tokens[4], Token::Dot);
    }

    #[test]
    fn placeholders() {
        let tokens = lex("? ?1 ?42 ?");
        assert_eq!(tokens[0], Token::Placeholder(1));
        assert_eq!(tokens[1], Token::Placeholder(1));
        assert_eq!(tokens[2], Token::Placeholder(42));
        assert_eq!(tokens[3], Token::Placeholder(2));
    }

    #[test]
    fn line_comments() {
        let tokens = lex("SELECT -- this is a comment\n42");
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::IntegerLiteral(42));
    }

    #[test]
    fn block_comments() {
        let tokens = lex("SELECT /* comment */ 42");
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::IntegerLiteral(42));
    }

    #[test]
    fn unterminated_string_is_error() {
        let result = Lexer::new("'hello").tokenize();
        assert!(result.is_err());
    }

    #[test]
    fn unterminated_block_comment_is_error() {
        let result = Lexer::new("/* oops").tokenize();
        assert!(result.is_err());
    }

    #[test]
    fn full_select_statement() {
        let tokens = lex("SELECT id, name FROM users WHERE age > 18;");
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Identifier("id".into()));
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Identifier("name".into()));
        assert_eq!(tokens[4], Token::From);
        assert_eq!(tokens[5], Token::Identifier("users".into()));
        assert_eq!(tokens[6], Token::Where);
        assert_eq!(tokens[7], Token::Identifier("age".into()));
        assert_eq!(tokens[8], Token::Gt);
        assert_eq!(tokens[9], Token::IntegerLiteral(18));
        assert_eq!(tokens[10], Token::Semicolon);
        assert_eq!(tokens[11], Token::Eof);
    }

    #[test]
    fn double_equals() {
        let tokens = lex("a == b");
        assert_eq!(tokens[0], Token::Identifier("a".into()));
        assert_eq!(tokens[1], Token::Eq);
        assert_eq!(tokens[2], Token::Identifier("b".into()));
    }

    #[test]
    fn empty_input() {
        let tokens = lex("");
        assert_eq!(tokens, vec![Token::Eof]);
    }

    #[test]
    fn blob_literal_lowercase() {
        let tokens = lex("x'0a1b'");
        assert_eq!(tokens[0], Token::BlobLiteral(vec![0x0A, 0x1B]));
    }
}
