//! Schema catalog for Horizon DB.
//!
//! The catalog is the central repository of metadata describing every table,
//! column, and index in the database. It is loaded from a dedicated "schema
//! B+Tree" on startup and updated whenever DDL statements (`CREATE TABLE`,
//! `DROP TABLE`, `CREATE INDEX`, etc.) are executed.
//!
//! # Persistence
//!
//! Table and index metadata is serialized into a simple text-based key/value
//! format and stored in the schema B+Tree (whose root page is tracked in the
//! database file header). Keys are prefixed with `"table:"` or `"index:"`
//! followed by the object name.
//!
//! # Key types
//!
//! - [`Catalog`]: The top-level container holding all table and index metadata.
//! - [`TableInfo`]: Schema description of a single table (columns, primary key,
//!   root page, next rowid).
//! - [`ColumnInfo`]: Schema description of a single column within a table.
//! - [`IndexInfo`]: Schema description of a secondary index.

use std::collections::HashMap;
use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::error::{HorizonError, Result};
use crate::pager::PageId;
use crate::types::{DataType, Value, determine_affinity};

/// Metadata for a single column in a table.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Declared type name from the `CREATE TABLE` statement (e.g.
    /// `"INTEGER"`, `"VARCHAR(255)"`).
    pub type_name: String,
    /// Resolved type affinity derived from [`type_name`](Self::type_name).
    pub affinity: DataType,
    /// Whether this column is (part of) the primary key.
    pub primary_key: bool,
    /// Whether this column auto-increments.
    pub autoincrement: bool,
    /// Whether the column has a `NOT NULL` constraint.
    pub not_null: bool,
    /// Whether the column has a `UNIQUE` constraint.
    pub unique: bool,
    /// Optional default value expression, pre-evaluated to a [`Value`].
    pub default_value: Option<Value>,
    /// Zero-based ordinal position within the table.
    pub position: usize,
}

/// Metadata for a table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Ordered list of columns.
    pub columns: Vec<ColumnInfo>,
    /// Root page of the table's B+Tree in the database file.
    pub root_page: PageId,
    /// The next rowid to assign for an `INSERT` without an explicit rowid.
    pub next_rowid: i64,
    /// Index of the primary key column within [`columns`](Self::columns),
    /// if any.
    pub pk_column: Option<usize>,
}

impl TableInfo {
    /// Look up a column by name (case-insensitive).
    pub fn find_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Return the zero-based index of a column by name (case-insensitive).
    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }
}

/// Metadata for an index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Name of the table this index belongs to.
    pub table_name: String,
    /// Ordered list of column names that form the index key.
    pub columns: Vec<String>,
    /// Whether this is a unique index.
    pub unique: bool,
    /// Root page of the index's B+Tree.
    pub root_page: PageId,
}

/// The schema catalog -- tracks all tables and indexes in the database.
pub struct Catalog {
    tables: HashMap<String, TableInfo>,
    indexes: HashMap<String, IndexInfo>,
}

impl Catalog {
    /// Create a new, empty catalog (used for a fresh database).
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Load the catalog from an existing schema B+Tree.
    ///
    /// Scans every entry in the tree; keys prefixed with `"table:"` are
    /// deserialized as [`TableInfo`] and those prefixed with `"index:"` as
    /// [`IndexInfo`].
    pub fn load(pool: &mut BufferPool, schema_root: PageId) -> Result<Self> {
        let tree = BTree::open(schema_root);
        let entries = tree.scan_all(pool)?;
        let mut catalog = Catalog::new();

        for entry in entries {
            let key = String::from_utf8(entry.key).map_err(|_| {
                HorizonError::CorruptDatabase("invalid schema key".into())
            })?;

            if key.starts_with("table:") {
                let table_info = Self::deserialize_table(&entry.value)?;
                catalog.tables.insert(table_info.name.clone(), table_info);
            } else if key.starts_with("index:") {
                let index_info = Self::deserialize_index(&entry.value)?;
                catalog.indexes.insert(index_info.name.clone(), index_info);
            }
        }

        Ok(catalog)
    }

    /// Add a table to the catalog and persist to the schema B+Tree.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::DuplicateTable`] if a table with the same
    /// name already exists.
    pub fn create_table(&mut self, pool: &mut BufferPool, table: TableInfo) -> Result<()> {
        if self.tables.contains_key(&table.name) {
            return Err(HorizonError::DuplicateTable(table.name.clone()));
        }

        // Persist to schema table
        let schema_root = pool.pager().schema_root();
        if schema_root != 0 {
            let mut tree = BTree::open(schema_root);
            let key = format!("table:{}", table.name);
            let value = Self::serialize_table(&table);
            tree.insert(pool, key.as_bytes(), &value)?;
            // Update root if it changed (due to splits)
            if tree.root_page() != schema_root {
                pool.pager_mut().set_schema_root(tree.root_page())?;
            }
        }

        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    /// Drop a table from the catalog and remove it from the schema B+Tree.
    ///
    /// Also removes all indexes that reference the dropped table.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::TableNotFound`] if the table does not exist.
    pub fn drop_table(&mut self, pool: &mut BufferPool, name: &str) -> Result<TableInfo> {
        let table = self.tables.remove(name)
            .ok_or_else(|| HorizonError::TableNotFound(name.into()))?;

        // Remove from schema B+Tree
        let schema_root = pool.pager().schema_root();
        if schema_root != 0 {
            let mut tree = BTree::open(schema_root);
            let key = format!("table:{}", name);
            tree.delete(pool, key.as_bytes())?;
            if tree.root_page() != schema_root {
                pool.pager_mut().set_schema_root(tree.root_page())?;
            }
        }

        // Also remove indexes for this table
        let index_names: Vec<String> = self.indexes.iter()
            .filter(|(_, idx)| idx.table_name == name)
            .map(|(name, _)| name.clone())
            .collect();
        for idx_name in index_names {
            self.indexes.remove(&idx_name);
        }

        Ok(table)
    }

    /// Get an immutable reference to a table's metadata.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::TableNotFound`] if the table does not exist.
    pub fn get_table(&self, name: &str) -> Result<&TableInfo> {
        self.tables.get(name)
            .ok_or_else(|| HorizonError::TableNotFound(name.into()))
    }

    /// Get a mutable reference to a table's metadata.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::TableNotFound`] if the table does not exist.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableInfo> {
        self.tables.get_mut(name)
            .ok_or_else(|| HorizonError::TableNotFound(name.into()))
    }

    /// Check whether a table with the given name exists.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Return a list of all table names currently in the catalog.
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Add an index to the catalog.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::DuplicateIndex`] if an index with the same
    /// name already exists.
    pub fn create_index(&mut self, _pool: &mut BufferPool, index: IndexInfo) -> Result<()> {
        if self.indexes.contains_key(&index.name) {
            return Err(HorizonError::DuplicateIndex(index.name.clone()));
        }
        self.indexes.insert(index.name.clone(), index);
        Ok(())
    }

    /// Get an immutable reference to an index's metadata.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::IndexNotFound`] if the index does not exist.
    pub fn get_index(&self, name: &str) -> Result<&IndexInfo> {
        self.indexes.get(name)
            .ok_or_else(|| HorizonError::IndexNotFound(name.into()))
    }

    /// Return all indexes associated with a given table.
    pub fn get_indexes_for_table(&self, table_name: &str) -> Vec<&IndexInfo> {
        self.indexes.values()
            .filter(|idx| idx.table_name == table_name)
            .collect()
    }

    /// Update the persisted metadata for a table (e.g. after advancing
    /// `next_rowid` during INSERT).
    pub fn update_table_meta(&mut self, pool: &mut BufferPool, name: &str, table: &TableInfo) -> Result<()> {
        let schema_root = pool.pager().schema_root();
        if schema_root != 0 {
            let mut tree = BTree::open(schema_root);
            let key = format!("table:{}", name);
            let value = Self::serialize_table(table);
            tree.insert(pool, key.as_bytes(), &value)?;
            if tree.root_page() != schema_root {
                pool.pager_mut().set_schema_root(tree.root_page())?;
            }
        }
        if let Some(existing) = self.tables.get_mut(name) {
            *existing = table.clone();
        }
        Ok(())
    }

    // =====================================================================
    // Serialization helpers
    // =====================================================================

    /// Serialize a [`TableInfo`] into a simple text-based format.
    ///
    /// Each line is a `key=value` pair. Columns are encoded as
    /// `colN=name:type=TYPE:pk:autoinc:notnull:unique`.
    fn serialize_table(table: &TableInfo) -> Vec<u8> {
        let mut parts = Vec::new();
        parts.push(format!("name={}", table.name));
        parts.push(format!("root_page={}", table.root_page));
        parts.push(format!("next_rowid={}", table.next_rowid));
        if let Some(pk) = table.pk_column {
            parts.push(format!("pk_column={}", pk));
        }
        for (i, col) in table.columns.iter().enumerate() {
            let mut col_str = format!("col{}={}", i, col.name);
            if !col.type_name.is_empty() {
                col_str.push_str(&format!(":type={}", col.type_name));
            }
            if col.primary_key { col_str.push_str(":pk"); }
            if col.autoincrement { col_str.push_str(":autoinc"); }
            if col.not_null { col_str.push_str(":notnull"); }
            if col.unique { col_str.push_str(":unique"); }
            parts.push(col_str);
        }
        parts.join("\n").into_bytes()
    }

    /// Deserialize a [`TableInfo`] from its text-based representation.
    fn deserialize_table(data: &[u8]) -> Result<TableInfo> {
        let text = String::from_utf8(data.to_vec()).map_err(|_| {
            HorizonError::CorruptDatabase("invalid table metadata".into())
        })?;

        let mut name = String::new();
        let mut root_page: PageId = 0;
        let mut next_rowid: i64 = 1;
        let mut pk_column: Option<usize> = None;
        let mut columns = Vec::new();

        for line in text.lines() {
            if let Some(val) = line.strip_prefix("name=") {
                name = val.to_string();
            } else if let Some(val) = line.strip_prefix("root_page=") {
                root_page = val.parse().unwrap_or(0);
            } else if let Some(val) = line.strip_prefix("next_rowid=") {
                next_rowid = val.parse().unwrap_or(1);
            } else if let Some(val) = line.strip_prefix("pk_column=") {
                pk_column = val.parse().ok();
            } else if line.starts_with("col") {
                // Parse column: colN=name:type=TYPE:pk:notnull:unique
                if let Some(eq_pos) = line.find('=') {
                    let col_data = &line[eq_pos + 1..];
                    let parts: Vec<&str> = col_data.split(':').collect();
                    let col_name = parts[0].to_string();
                    let mut type_name = String::new();
                    let mut primary_key = false;
                    let mut autoincrement = false;
                    let mut not_null = false;
                    let mut unique = false;

                    for part in &parts[1..] {
                        if let Some(tn) = part.strip_prefix("type=") {
                            type_name = tn.to_string();
                        } else if *part == "pk" {
                            primary_key = true;
                        } else if *part == "autoinc" {
                            autoincrement = true;
                        } else if *part == "notnull" {
                            not_null = true;
                        } else if *part == "unique" {
                            unique = true;
                        }
                    }

                    let affinity = determine_affinity(&type_name);
                    let position = columns.len();
                    columns.push(ColumnInfo {
                        name: col_name, type_name, affinity, primary_key,
                        autoincrement, not_null, unique, default_value: None, position,
                    });
                }
            }
        }

        Ok(TableInfo { name, columns, root_page, next_rowid, pk_column })
    }

    /// Serialize an [`IndexInfo`] into a simple text-based format.
    #[allow(dead_code)]
    fn serialize_index(index: &IndexInfo) -> Vec<u8> {
        let mut parts = Vec::new();
        parts.push(format!("name={}", index.name));
        parts.push(format!("table={}", index.table_name));
        parts.push(format!("root_page={}", index.root_page));
        parts.push(format!("unique={}", index.unique));
        parts.push(format!("columns={}", index.columns.join(",")));
        parts.join("\n").into_bytes()
    }

    /// Deserialize an [`IndexInfo`] from its text-based representation.
    fn deserialize_index(data: &[u8]) -> Result<IndexInfo> {
        let text = String::from_utf8(data.to_vec()).map_err(|_| {
            HorizonError::CorruptDatabase("invalid index metadata".into())
        })?;
        let mut name = String::new();
        let mut table_name = String::new();
        let mut root_page: PageId = 0;
        let mut unique = false;
        let mut columns = Vec::new();

        for line in text.lines() {
            if let Some(val) = line.strip_prefix("name=") { name = val.to_string(); }
            else if let Some(val) = line.strip_prefix("table=") { table_name = val.to_string(); }
            else if let Some(val) = line.strip_prefix("root_page=") { root_page = val.parse().unwrap_or(0); }
            else if let Some(val) = line.strip_prefix("unique=") { unique = val == "true"; }
            else if let Some(val) = line.strip_prefix("columns=") { columns = val.split(',').map(|s| s.to_string()).collect(); }
        }

        Ok(IndexInfo { name, table_name, columns, unique, root_page })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    // =====================================================================
    // Helper: build a sample TableInfo
    // =====================================================================

    fn sample_table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    type_name: "INTEGER".to_string(),
                    affinity: DataType::Integer,
                    primary_key: true,
                    autoincrement: true,
                    not_null: true,
                    unique: false,
                    default_value: None,
                    position: 0,
                },
                ColumnInfo {
                    name: "email".to_string(),
                    type_name: "TEXT".to_string(),
                    affinity: DataType::Text,
                    primary_key: false,
                    autoincrement: false,
                    not_null: true,
                    unique: true,
                    default_value: None,
                    position: 1,
                },
                ColumnInfo {
                    name: "score".to_string(),
                    type_name: "REAL".to_string(),
                    affinity: DataType::Real,
                    primary_key: false,
                    autoincrement: false,
                    not_null: false,
                    unique: false,
                    default_value: None,
                    position: 2,
                },
            ],
            root_page: 7,
            next_rowid: 42,
            pk_column: Some(0),
        }
    }

    fn sample_index() -> IndexInfo {
        IndexInfo {
            name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            root_page: 12,
        }
    }

    // =====================================================================
    // TableInfo tests
    // =====================================================================

    #[test]
    fn find_column_by_name() {
        let table = sample_table("users");
        let col = table.find_column("email").unwrap();
        assert_eq!(col.name, "email");
        assert_eq!(col.affinity, DataType::Text);
    }

    #[test]
    fn find_column_case_insensitive() {
        let table = sample_table("users");
        assert!(table.find_column("EMAIL").is_some());
        assert!(table.find_column("Id").is_some());
        assert!(table.find_column("SCORE").is_some());
    }

    #[test]
    fn find_column_not_found() {
        let table = sample_table("users");
        assert!(table.find_column("nonexistent").is_none());
    }

    #[test]
    fn find_column_index_by_name() {
        let table = sample_table("users");
        assert_eq!(table.find_column_index("id"), Some(0));
        assert_eq!(table.find_column_index("email"), Some(1));
        assert_eq!(table.find_column_index("score"), Some(2));
        assert_eq!(table.find_column_index("missing"), None);
    }

    #[test]
    fn find_column_index_case_insensitive() {
        let table = sample_table("users");
        assert_eq!(table.find_column_index("ID"), Some(0));
        assert_eq!(table.find_column_index("Email"), Some(1));
    }

    // =====================================================================
    // Table serialization round-trip tests
    // =====================================================================

    #[test]
    fn table_serialization_round_trip() {
        let table = sample_table("users");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.root_page, 7);
        assert_eq!(decoded.next_rowid, 42);
        assert_eq!(decoded.pk_column, Some(0));
        assert_eq!(decoded.columns.len(), 3);
    }

    #[test]
    fn table_serialization_preserves_column_names() {
        let table = sample_table("test");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        assert_eq!(decoded.columns[0].name, "id");
        assert_eq!(decoded.columns[1].name, "email");
        assert_eq!(decoded.columns[2].name, "score");
    }

    #[test]
    fn table_serialization_preserves_column_types() {
        let table = sample_table("test");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        assert_eq!(decoded.columns[0].type_name, "INTEGER");
        assert_eq!(decoded.columns[1].type_name, "TEXT");
        assert_eq!(decoded.columns[2].type_name, "REAL");
    }

    #[test]
    fn table_serialization_preserves_affinity() {
        let table = sample_table("test");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        assert_eq!(decoded.columns[0].affinity, DataType::Integer);
        assert_eq!(decoded.columns[1].affinity, DataType::Text);
        assert_eq!(decoded.columns[2].affinity, DataType::Real);
    }

    #[test]
    fn table_serialization_preserves_constraints() {
        let table = sample_table("test");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        // Column 0: pk, autoinc, notnull
        assert!(decoded.columns[0].primary_key);
        assert!(decoded.columns[0].autoincrement);
        assert!(decoded.columns[0].not_null);
        assert!(!decoded.columns[0].unique);

        // Column 1: notnull, unique
        assert!(!decoded.columns[1].primary_key);
        assert!(!decoded.columns[1].autoincrement);
        assert!(decoded.columns[1].not_null);
        assert!(decoded.columns[1].unique);

        // Column 2: no constraints
        assert!(!decoded.columns[2].primary_key);
        assert!(!decoded.columns[2].autoincrement);
        assert!(!decoded.columns[2].not_null);
        assert!(!decoded.columns[2].unique);
    }

    #[test]
    fn table_serialization_preserves_positions() {
        let table = sample_table("test");
        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();

        assert_eq!(decoded.columns[0].position, 0);
        assert_eq!(decoded.columns[1].position, 1);
        assert_eq!(decoded.columns[2].position, 2);
    }

    #[test]
    fn table_serialization_no_pk_column() {
        let table = TableInfo {
            name: "logs".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "message".to_string(),
                    type_name: "TEXT".to_string(),
                    affinity: DataType::Text,
                    primary_key: false,
                    autoincrement: false,
                    not_null: false,
                    unique: false,
                    default_value: None,
                    position: 0,
                },
            ],
            root_page: 3,
            next_rowid: 1,
            pk_column: None,
        };

        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();
        assert_eq!(decoded.pk_column, None);
        assert_eq!(decoded.name, "logs");
        assert_eq!(decoded.columns.len(), 1);
    }

    #[test]
    fn table_serialization_no_columns() {
        let table = TableInfo {
            name: "empty".to_string(),
            columns: vec![],
            root_page: 1,
            next_rowid: 1,
            pk_column: None,
        };

        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();
        assert_eq!(decoded.name, "empty");
        assert!(decoded.columns.is_empty());
    }

    #[test]
    fn table_serialization_column_with_empty_type() {
        let table = TableInfo {
            name: "dynamic".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "data".to_string(),
                    type_name: "".to_string(),
                    affinity: DataType::Blob,
                    primary_key: false,
                    autoincrement: false,
                    not_null: false,
                    unique: false,
                    default_value: None,
                    position: 0,
                },
            ],
            root_page: 5,
            next_rowid: 1,
            pk_column: None,
        };

        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();
        assert_eq!(decoded.columns[0].name, "data");
        assert_eq!(decoded.columns[0].type_name, "");
        // Empty type name → Blob affinity via determine_affinity("")
        assert_eq!(decoded.columns[0].affinity, DataType::Blob);
    }

    #[test]
    fn table_deserialization_rejects_invalid_utf8() {
        let data: Vec<u8> = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8
        assert!(Catalog::deserialize_table(&data).is_err());
    }

    // =====================================================================
    // Index serialization round-trip tests
    // =====================================================================

    #[test]
    fn index_serialization_round_trip() {
        let index = sample_index();
        let bytes = Catalog::serialize_index(&index);
        let decoded = Catalog::deserialize_index(&bytes).unwrap();

        assert_eq!(decoded.name, "idx_users_email");
        assert_eq!(decoded.table_name, "users");
        assert_eq!(decoded.columns, vec!["email".to_string()]);
        assert!(decoded.unique);
        assert_eq!(decoded.root_page, 12);
    }

    #[test]
    fn index_serialization_multi_column() {
        let index = IndexInfo {
            name: "idx_composite".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["user_id".to_string(), "created_at".to_string(), "status".to_string()],
            unique: false,
            root_page: 25,
        };

        let bytes = Catalog::serialize_index(&index);
        let decoded = Catalog::deserialize_index(&bytes).unwrap();

        assert_eq!(decoded.name, "idx_composite");
        assert_eq!(decoded.table_name, "orders");
        assert_eq!(decoded.columns.len(), 3);
        assert_eq!(decoded.columns[0], "user_id");
        assert_eq!(decoded.columns[1], "created_at");
        assert_eq!(decoded.columns[2], "status");
        assert!(!decoded.unique);
        assert_eq!(decoded.root_page, 25);
    }

    #[test]
    fn index_serialization_not_unique() {
        let index = IndexInfo {
            name: "idx_status".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["status".to_string()],
            unique: false,
            root_page: 10,
        };

        let bytes = Catalog::serialize_index(&index);
        let decoded = Catalog::deserialize_index(&bytes).unwrap();
        assert!(!decoded.unique);
    }

    #[test]
    fn index_deserialization_rejects_invalid_utf8() {
        let data: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        assert!(Catalog::deserialize_index(&data).is_err());
    }

    // =====================================================================
    // Catalog in-memory operations (no BufferPool)
    // =====================================================================

    #[test]
    fn new_catalog_is_empty() {
        let catalog = Catalog::new();
        assert!(catalog.list_tables().is_empty());
        assert!(!catalog.table_exists("anything"));
    }

    #[test]
    fn get_table_not_found() {
        let catalog = Catalog::new();
        let result = catalog.get_table("users");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HorizonError::TableNotFound(_)));
    }

    #[test]
    fn get_table_mut_not_found() {
        let mut catalog = Catalog::new();
        let result = catalog.get_table_mut("users");
        assert!(result.is_err());
    }

    #[test]
    fn get_index_not_found() {
        let catalog = Catalog::new();
        let result = catalog.get_index("idx_foo");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HorizonError::IndexNotFound(_)));
    }

    #[test]
    fn table_exists_returns_false_for_missing() {
        let catalog = Catalog::new();
        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn get_indexes_for_table_empty() {
        let catalog = Catalog::new();
        let indexes = catalog.get_indexes_for_table("users");
        assert!(indexes.is_empty());
    }

    // =====================================================================
    // Catalog with direct HashMap insertion (bypassing BufferPool)
    // =====================================================================

    #[test]
    fn catalog_direct_insert_and_get() {
        let mut catalog = Catalog::new();
        let table = sample_table("users");
        catalog.tables.insert("users".to_string(), table);

        assert!(catalog.table_exists("users"));
        let t = catalog.get_table("users").unwrap();
        assert_eq!(t.name, "users");
        assert_eq!(t.columns.len(), 3);
    }

    #[test]
    fn catalog_direct_insert_and_list() {
        let mut catalog = Catalog::new();
        catalog.tables.insert("users".to_string(), sample_table("users"));
        catalog.tables.insert("orders".to_string(), sample_table("orders"));

        let mut names = catalog.list_tables();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    #[test]
    fn catalog_direct_remove_table() {
        let mut catalog = Catalog::new();
        catalog.tables.insert("users".to_string(), sample_table("users"));
        assert!(catalog.table_exists("users"));

        catalog.tables.remove("users");
        assert!(!catalog.table_exists("users"));
    }

    #[test]
    fn catalog_direct_insert_index_and_get() {
        let mut catalog = Catalog::new();
        let index = sample_index();
        catalog.indexes.insert(index.name.clone(), index);

        let idx = catalog.get_index("idx_users_email").unwrap();
        assert_eq!(idx.table_name, "users");
        assert!(idx.unique);
    }

    #[test]
    fn catalog_get_indexes_for_table_filters_correctly() {
        let mut catalog = Catalog::new();

        catalog.indexes.insert("idx_users_email".to_string(), IndexInfo {
            name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            root_page: 10,
        });

        catalog.indexes.insert("idx_users_name".to_string(), IndexInfo {
            name: "idx_users_name".to_string(),
            table_name: "users".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
            root_page: 11,
        });

        catalog.indexes.insert("idx_orders_date".to_string(), IndexInfo {
            name: "idx_orders_date".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["date".to_string()],
            unique: false,
            root_page: 12,
        });

        let user_indexes = catalog.get_indexes_for_table("users");
        assert_eq!(user_indexes.len(), 2);

        let order_indexes = catalog.get_indexes_for_table("orders");
        assert_eq!(order_indexes.len(), 1);
        assert_eq!(order_indexes[0].name, "idx_orders_date");

        let empty = catalog.get_indexes_for_table("products");
        assert!(empty.is_empty());
    }

    #[test]
    fn catalog_get_table_mut_allows_modification() {
        let mut catalog = Catalog::new();
        catalog.tables.insert("users".to_string(), sample_table("users"));

        {
            let t = catalog.get_table_mut("users").unwrap();
            t.next_rowid = 100;
        }

        let t = catalog.get_table("users").unwrap();
        assert_eq!(t.next_rowid, 100);
    }

    #[test]
    fn catalog_drop_table_removes_associated_indexes() {
        let mut catalog = Catalog::new();
        catalog.tables.insert("users".to_string(), sample_table("users"));

        catalog.indexes.insert("idx_users_email".to_string(), IndexInfo {
            name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            root_page: 10,
        });

        catalog.indexes.insert("idx_orders_date".to_string(), IndexInfo {
            name: "idx_orders_date".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["date".to_string()],
            unique: false,
            root_page: 11,
        });

        // Manually remove the table and its indexes (simulating drop_table
        // without a BufferPool).
        catalog.tables.remove("users");
        let index_names: Vec<String> = catalog.indexes.iter()
            .filter(|(_, idx)| idx.table_name == "users")
            .map(|(name, _)| name.clone())
            .collect();
        for name in index_names {
            catalog.indexes.remove(&name);
        }

        // The users index should be gone but the orders index should remain.
        assert!(catalog.get_index("idx_users_email").is_err());
        assert!(catalog.get_index("idx_orders_date").is_ok());
    }

    // =====================================================================
    // ColumnInfo affinity derived from type_name
    // =====================================================================

    #[test]
    fn column_affinity_matches_type_name() {
        let col = ColumnInfo {
            name: "value".to_string(),
            type_name: "VARCHAR(255)".to_string(),
            affinity: determine_affinity("VARCHAR(255)"),
            primary_key: false,
            autoincrement: false,
            not_null: false,
            unique: false,
            default_value: None,
            position: 0,
        };
        assert_eq!(col.affinity, DataType::Text);
    }

    #[test]
    fn column_info_debug_format() {
        let col = ColumnInfo {
            name: "id".to_string(),
            type_name: "INTEGER".to_string(),
            affinity: DataType::Integer,
            primary_key: true,
            autoincrement: false,
            not_null: true,
            unique: false,
            default_value: None,
            position: 0,
        };
        let debug = format!("{:?}", col);
        assert!(debug.contains("id"));
        assert!(debug.contains("INTEGER"));
    }

    // =====================================================================
    // Edge cases in serialization
    // =====================================================================

    #[test]
    fn table_serialization_large_rowid() {
        let table = TableInfo {
            name: "big".to_string(),
            columns: vec![],
            root_page: u32::MAX,
            next_rowid: i64::MAX,
            pk_column: None,
        };

        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();
        assert_eq!(decoded.root_page, u32::MAX);
        assert_eq!(decoded.next_rowid, i64::MAX);
    }

    #[test]
    fn table_serialization_negative_rowid() {
        let table = TableInfo {
            name: "neg".to_string(),
            columns: vec![],
            root_page: 1,
            next_rowid: -100,
            pk_column: None,
        };

        let bytes = Catalog::serialize_table(&table);
        let decoded = Catalog::deserialize_table(&bytes).unwrap();
        assert_eq!(decoded.next_rowid, -100);
    }

    #[test]
    fn index_serialization_single_column() {
        let index = IndexInfo {
            name: "idx_single".to_string(),
            table_name: "t".to_string(),
            columns: vec!["col1".to_string()],
            unique: true,
            root_page: 1,
        };

        let bytes = Catalog::serialize_index(&index);
        let decoded = Catalog::deserialize_index(&bytes).unwrap();
        assert_eq!(decoded.columns, vec!["col1".to_string()]);
    }

    // =====================================================================
    // Multiple table operations
    // =====================================================================

    #[test]
    fn catalog_multiple_tables() {
        let mut catalog = Catalog::new();

        for i in 0..10 {
            let name = format!("table_{}", i);
            catalog.tables.insert(name.clone(), TableInfo {
                name,
                columns: vec![],
                root_page: i as u32 + 1,
                next_rowid: 1,
                pk_column: None,
            });
        }

        assert_eq!(catalog.list_tables().len(), 10);
        assert!(catalog.table_exists("table_0"));
        assert!(catalog.table_exists("table_9"));
        assert!(!catalog.table_exists("table_10"));
    }
}
