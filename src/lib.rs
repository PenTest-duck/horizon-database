//! # Horizon DB
//!
//! A highly-performant serverless embedded RDBMS written in Rust
//! with full SQLite feature parity.

pub mod error;
pub mod types;
pub mod pager;
pub mod wal;
pub mod buffer;
pub mod btree;
pub mod mvcc;
pub mod catalog;
pub mod sql;
pub mod planner;
pub mod execution;

pub use error::{HorizonError, Result};
pub use types::Value;
pub use pager::PageId;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A row returned from a query.
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Arc<Vec<String>>,
    pub values: Vec<Value>,
}

impl Row {
    /// Get a value by column name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .map(|i| &self.values[i])
    }

    /// Get a value by column index.
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// Query result set.
pub struct QueryResult {
    pub columns: Arc<Vec<String>>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// The main database handle. Thread-safe via internal locking.
pub struct Database {
    path: PathBuf,
    inner: Mutex<DatabaseInner>,
}

struct DatabaseInner {
    buffer_pool: buffer::BufferPool,
    catalog: catalog::Catalog,
    txn_manager: mvcc::TransactionManager,
}

impl Database {
    /// Open or create a database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let pager = pager::Pager::open(path, false)?;
        let wal_path = path.with_extension("hdb-wal");
        let wal = Some(wal::WalManager::open(&wal_path)?);
        let mut buffer_pool = buffer::BufferPool::new(pager, wal, 1024)?;

        // Initialize catalog from schema table
        let schema_root = buffer_pool.pager().schema_root();
        let catalog = if schema_root == 0 {
            // New database — create schema table
            let tree = btree::BTree::create(&mut buffer_pool)?;
            buffer_pool.pager_mut().set_schema_root(tree.root_page())?;
            catalog::Catalog::new()
        } else {
            catalog::Catalog::load(&mut buffer_pool, schema_root)?
        };

        let txn_manager = mvcc::TransactionManager::new();

        Ok(Database {
            path: path.to_path_buf(),
            inner: Mutex::new(DatabaseInner {
                buffer_pool,
                catalog,
                txn_manager,
            }),
        })
    }

    /// Execute a SQL statement that doesn't return rows.
    pub fn execute(&self, sql_text: &str) -> Result<usize> {
        let stmts = sql::parser::Parser::parse(sql_text)?;
        if stmts.is_empty() {
            return Ok(0);
        }

        let mut inner = self.inner.lock().map_err(|_| {
            HorizonError::Internal("mutex poisoned".into())
        })?;

        let mut total = 0;
        for stmt in stmts {
            let DatabaseInner { buffer_pool, catalog, txn_manager } = &mut *inner;
            total += execution::execute_statement(&stmt, buffer_pool, catalog, txn_manager)?;
        }
        Ok(total)
    }

    /// Execute a SQL query that returns rows.
    pub fn query(&self, sql_text: &str) -> Result<QueryResult> {
        let stmts = sql::parser::Parser::parse(sql_text)?;
        if stmts.is_empty() {
            return Ok(QueryResult {
                columns: Arc::new(vec![]),
                rows: vec![],
            });
        }

        let mut inner = self.inner.lock().map_err(|_| {
            HorizonError::Internal("mutex poisoned".into())
        })?;

        let DatabaseInner { buffer_pool, catalog, txn_manager } = &mut *inner;

        // Route SELECT, PRAGMA, EXPLAIN, and RETURNING through execute_query
        match &stmts[0] {
            sql::ast::Statement::Select(_)
            | sql::ast::Statement::Pragma(_)
            | sql::ast::Statement::Explain(_) => {
                execution::execute_query(&stmts[0], buffer_pool, catalog, txn_manager)
            }
            stmt if execution::has_returning(stmt) => {
                execution::execute_query(stmt, buffer_pool, catalog, txn_manager)
            }
            _ => Err(HorizonError::Internal(
                "query() requires a SELECT, PRAGMA, EXPLAIN, or RETURNING statement".into(),
            )),
        }
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Close the database, flushing all writes.
    pub fn close(self) -> Result<()> {
        let mut inner = self.inner.into_inner().map_err(|_| {
            HorizonError::Internal("mutex poisoned".into())
        })?;
        inner.buffer_pool.flush_all()?;
        Ok(())
    }
}
