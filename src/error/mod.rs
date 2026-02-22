//! Unified error handling for Horizon DB.
//!
//! This module defines [`HorizonError`], the single error type propagated
//! throughout every layer of the database engine — from the storage manager
//! and buffer pool, through the query executor, up to the public API surface.
//!
//! A convenience [`Result<T>`] type alias is re-exported so that callers can
//! write `Result<T>` instead of `std::result::Result<T, HorizonError>`.

use std::fmt;
use std::io;

/// The canonical error type for all Horizon DB operations.
///
/// Every fallible function in the codebase returns this type (via the
/// [`Result`] alias). Variants are organised by subsystem so that callers
/// can match on the error category without inspecting free-form strings.
#[derive(Debug)]
pub enum HorizonError {
    /// An I/O error originating from the filesystem or disk layer.
    Io(io::Error),

    /// The on-disk database file is corrupt or contains an unrecognised
    /// format (e.g. bad magic bytes, invalid page checksums, truncated
    /// pages).
    CorruptDatabase(String),

    /// The SQL text could not be parsed into a valid statement.
    InvalidSql(String),

    /// A type mismatch or invalid cast was encountered during expression
    /// evaluation (e.g. comparing an integer to a blob).
    TypeError(String),

    /// A constraint was violated. This covers `PRIMARY KEY`, `UNIQUE`,
    /// `NOT NULL`, `FOREIGN KEY`, and `CHECK` constraints.
    ConstraintViolation(String),

    /// A transaction-level error such as a commit or rollback failure,
    /// a serialization conflict under MVCC, or a deadlock.
    TransactionError(String),

    /// The referenced table does not exist in the catalog.
    TableNotFound(String),

    /// The referenced column does not exist in the target table.
    ColumnNotFound(String),

    /// The referenced index does not exist in the catalog.
    IndexNotFound(String),

    /// A table with the given name already exists.
    DuplicateTable(String),

    /// A column with the given name already exists within the same table.
    DuplicateColumn(String),

    /// An index with the given name already exists.
    DuplicateIndex(String),

    /// The buffer pool has no available page frames and all pages are
    /// pinned, so the requested page cannot be loaded.
    BufferPoolFull,

    /// The requested page number does not exist in the database file.
    PageNotFound(u32),

    /// An arithmetic overflow or a value that exceeds the maximum
    /// representable size for its type.
    Overflow(String),

    /// An internal invariant was violated. This usually indicates a bug
    /// in the database engine itself and should be reported.
    Internal(String),

    /// The requested feature has not been implemented yet.
    NotImplemented(String),

    /// A write operation was attempted against a database that was opened
    /// in read-only mode, or against a read-only transaction.
    ReadOnly(String),

    /// An error occurred during deserialization of a value or data structure
    /// from its binary representation.
    Deserialization(String),
}

impl fmt::Display for HorizonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HorizonError::Io(err) => write!(f, "I/O error: {err}"),
            HorizonError::CorruptDatabase(msg) => write!(f, "corrupt database: {msg}"),
            HorizonError::InvalidSql(msg) => write!(f, "invalid SQL: {msg}"),
            HorizonError::TypeError(msg) => write!(f, "type error: {msg}"),
            HorizonError::ConstraintViolation(msg) => {
                write!(f, "constraint violation: {msg}")
            }
            HorizonError::TransactionError(msg) => write!(f, "transaction error: {msg}"),
            HorizonError::TableNotFound(name) => write!(f, "table not found: {name}"),
            HorizonError::ColumnNotFound(name) => write!(f, "column not found: {name}"),
            HorizonError::IndexNotFound(name) => write!(f, "index not found: {name}"),
            HorizonError::DuplicateTable(name) => {
                write!(f, "table already exists: {name}")
            }
            HorizonError::DuplicateColumn(name) => {
                write!(f, "column already exists: {name}")
            }
            HorizonError::DuplicateIndex(name) => {
                write!(f, "index already exists: {name}")
            }
            HorizonError::BufferPoolFull => write!(f, "buffer pool full: no available pages"),
            HorizonError::PageNotFound(id) => write!(f, "page not found: {id}"),
            HorizonError::Overflow(msg) => write!(f, "overflow: {msg}"),
            HorizonError::Internal(msg) => write!(f, "internal error: {msg}"),
            HorizonError::NotImplemented(msg) => write!(f, "not implemented: {msg}"),
            HorizonError::ReadOnly(msg) => write!(f, "read-only: {msg}"),
            HorizonError::Deserialization(msg) => {
                write!(f, "deserialization error: {msg}")
            }
        }
    }
}

impl std::error::Error for HorizonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HorizonError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for HorizonError {
    fn from(err: io::Error) -> Self {
        HorizonError::Io(err)
    }
}

/// A specialised [`Result`] type for Horizon DB operations.
///
/// This is defined as a convenience so that every function in the codebase
/// can simply return `Result<T>` rather than spelling out the full
/// `std::result::Result<T, HorizonError>`.
pub type Result<T> = std::result::Result<T, HorizonError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts_via_from() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        let horizon_err: HorizonError = HorizonError::from(io_err);
        assert!(matches!(horizon_err, HorizonError::Io(_)));
        assert!(horizon_err.to_string().contains("file missing"));
    }

    #[test]
    fn io_error_converts_via_question_mark() {
        fn might_fail() -> Result<()> {
            let _f = std::fs::File::open("/non/existent/path/horizon_test")?;
            Ok(())
        }

        let err = might_fail().unwrap_err();
        assert!(matches!(err, HorizonError::Io(_)));
    }

    #[test]
    fn display_messages_are_human_readable() {
        let cases: Vec<(HorizonError, &str)> = vec![
            (
                HorizonError::CorruptDatabase("bad checksum".into()),
                "corrupt database: bad checksum",
            ),
            (
                HorizonError::InvalidSql("unexpected token".into()),
                "invalid SQL: unexpected token",
            ),
            (
                HorizonError::TypeError("cannot compare INT and TEXT".into()),
                "type error: cannot compare INT and TEXT",
            ),
            (
                HorizonError::ConstraintViolation("UNIQUE on col id".into()),
                "constraint violation: UNIQUE on col id",
            ),
            (
                HorizonError::TransactionError("serialization conflict".into()),
                "transaction error: serialization conflict",
            ),
            (
                HorizonError::TableNotFound("users".into()),
                "table not found: users",
            ),
            (
                HorizonError::ColumnNotFound("email".into()),
                "column not found: email",
            ),
            (
                HorizonError::IndexNotFound("idx_users_email".into()),
                "index not found: idx_users_email",
            ),
            (
                HorizonError::DuplicateTable("users".into()),
                "table already exists: users",
            ),
            (
                HorizonError::DuplicateColumn("email".into()),
                "column already exists: email",
            ),
            (
                HorizonError::DuplicateIndex("idx_pk".into()),
                "index already exists: idx_pk",
            ),
            (
                HorizonError::BufferPoolFull,
                "buffer pool full: no available pages",
            ),
            (HorizonError::PageNotFound(42), "page not found: 42"),
            (
                HorizonError::Overflow("value exceeds i64::MAX".into()),
                "overflow: value exceeds i64::MAX",
            ),
            (
                HorizonError::Internal("unexpected None".into()),
                "internal error: unexpected None",
            ),
            (
                HorizonError::NotImplemented("window functions".into()),
                "not implemented: window functions",
            ),
            (
                HorizonError::ReadOnly("cannot INSERT in read-only mode".into()),
                "read-only: cannot INSERT in read-only mode",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn error_source_chains_io_errors() {
        use std::error::Error;

        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let horizon_err = HorizonError::Io(io_err);
        assert!(horizon_err.source().is_some());

        let non_io = HorizonError::Internal("bug".into());
        assert!(non_io.source().is_none());
    }
}
