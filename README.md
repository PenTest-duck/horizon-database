# Horizon DB

A highly-performant serverless embedded RDBMS written in Rust with full SQLite feature parity.

**Zero external dependencies** for the core library. Single `.hdb` file storage with WAL journaling.

## Features

### SQL Support
- **DDL**: CREATE/DROP TABLE, CREATE/DROP INDEX, ALTER TABLE (ADD COLUMN, RENAME TABLE/COLUMN, DROP COLUMN)
- **DML**: INSERT, SELECT, UPDATE, DELETE with full WHERE clause evaluation
- **Queries**: JOIN (INNER, LEFT, RIGHT, CROSS), subqueries (scalar, EXISTS, IN), UNION/INTERSECT/EXCEPT
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, TOTAL with GROUP BY/HAVING
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE with PARTITION BY, ORDER BY, and frame clauses
- **CTEs**: WITH...AS including recursive CTEs
- **Advanced**: DISTINCT, ORDER BY, LIMIT/OFFSET, CASE/WHEN, CAST, LIKE, BETWEEN, IN, IS NULL, COLLATE
- **Views**: CREATE VIEW with runtime expansion
- **Triggers**: CREATE TRIGGER (BEFORE/AFTER for INSERT/UPDATE/DELETE)
- **Transactions**: BEGIN/COMMIT/ROLLBACK with MVCC snapshot isolation
- **UPSERT**: INSERT OR REPLACE
- **RETURNING**: INSERT/UPDATE/DELETE...RETURNING clause

### Built-in Functions
- **String**: LENGTH, UPPER, LOWER, SUBSTR, REPLACE, TRIM, LTRIM, RTRIM, INSTR, HEX, QUOTE, UNICODE, CHAR, PRINTF, ZEROBLOB
- **Math**: ABS, ROUND, RANDOM, MIN, MAX, TOTAL
- **Type**: TYPEOF, CAST, COALESCE, NULLIF, IIF
- **JSON**: JSON, JSON_EXTRACT, JSON_ARRAY, JSON_OBJECT, JSON_TYPE, JSON_VALID, JSON_ARRAY_LENGTH, JSON_REMOVE, JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_GROUP_ARRAY, JSON_GROUP_OBJECT, JSON_EACH, JSON_PATCH
- **Date/Time**: DATE, TIME, DATETIME, STRFTIME, JULIANDAY with modifiers (+N days, start of month, etc.)
- **Aggregate**: COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, TOTAL

### Extensions
- **FTS5**: Full-text search with inverted index, BM25 ranking, `MATCH` operator, `highlight()`, `snippet()`, `bm25()` functions
- **R-tree**: Spatial indexing with `CREATE VIRTUAL TABLE...USING rtree()`, 1-5 dimension support, overlap queries
- **Generated Columns**: STORED (computed at INSERT) and VIRTUAL (computed at SELECT)
- **EXPLAIN / EXPLAIN QUERY PLAN**: Query plan inspection
- **PRAGMA**: table_info, table_list, index_list, index_info, database_list, page_count, page_size, journal_mode, encoding
- **ATTACH/DETACH DATABASE**
- **VACUUM**
- **Collation**: BINARY, NOCASE, RTRIM

### CLI
- Interactive REPL with line editing and history
- Dot-commands: `.tables`, `.schema`, `.import`, `.dump`, `.mode`, `.headers`, `.quit`
- Output modes: column, csv, json, line
- Batch mode: `horizon db.hdb < script.sql`
- CSV/SQL import and export

## Quick Start

### As a Library

```rust
use horizon::Database;

fn main() -> horizon::Result<()> {
    let db = Database::open("my.hdb")?;

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")?;

    let result = db.query("SELECT * FROM users WHERE name LIKE 'A%'")?;
    for row in result {
        println!("{}: {}",
            row.get("name").unwrap(),
            row.get("email").unwrap()
        );
    }

    db.close()?;
    Ok(())
}
```

### As a CLI

```bash
# Build the CLI
cargo build --release

# Interactive mode
./target/release/horizon my.hdb

# Batch mode
echo "SELECT * FROM users;" | ./target/release/horizon my.hdb

# Import CSV
./target/release/horizon my.hdb
horizon> .import data.csv users
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                  Public API                  │
│           Database::open/execute/query       │
├─────────────────────────────────────────────┤
│              SQL Frontend                    │
│  Lexer → Parser → AST → Planner → LogicalPlan│
├─────────────────────────────────────────────┤
│            Execution Engine                  │
│  Plan Executor │ FTS5 │ R-tree │ Views/Triggers│
├─────────────────────────────────────────────┤
│          Transaction Layer (MVCC)            │
│      Snapshot Isolation │ Undo Log           │
├─────────────────────────────────────────────┤
│             Catalog Manager                  │
│    Tables │ Indexes │ Views │ Triggers       │
├─────────────────────────────────────────────┤
│              B+Tree Engine                   │
│       Insert │ Search │ Delete │ Scan        │
├─────────────────────────────────────────────┤
│             Buffer Pool (LRU)               │
│          1024 page frames, pin/unpin         │
├─────────────────────────────────────────────┤
│         WAL Manager │ Pager (4KB pages)      │
│      Append │ Checkpoint │ Recovery          │
└─────────────────────────────────────────────┘
              ↕                ↕
         [data.hdb]      [data.hdb-wal]
```

### Module Overview

| Module | File | Description |
|--------|------|-------------|
| `pager` | `src/pager/mod.rs` | 4KB page I/O, file header, page allocation |
| `wal` | `src/wal/mod.rs` | Write-ahead log for crash recovery |
| `buffer` | `src/buffer/mod.rs` | LRU buffer pool with 1024 page frames |
| `btree` | `src/btree/mod.rs` | B+Tree for tables (rowid key) and indexes |
| `types` | `src/types/mod.rs` | Value types, type affinity, serialization |
| `mvcc` | `src/mvcc/mod.rs` | MVCC transaction manager, snapshot isolation |
| `catalog` | `src/catalog/mod.rs` | Schema metadata (tables, indexes, views, triggers) |
| `sql/lexer` | `src/sql/lexer.rs` | Hand-written SQL tokenizer |
| `sql/parser` | `src/sql/parser.rs` | Recursive-descent SQL parser |
| `sql/ast` | `src/sql/ast.rs` | Abstract syntax tree definitions |
| `planner` | `src/planner/mod.rs` | Rule-based query planner |
| `execution` | `src/execution/mod.rs` | Query execution engine |
| `execution/fts5` | `src/execution/fts5.rs` | FTS5 full-text search |
| `execution/rtree` | `src/execution/rtree.rs` | R-tree spatial indexing |
| `execution/json` | `src/execution/json.rs` | JSON function implementation |
| `error` | `src/error/mod.rs` | Error types and Result alias |

## API Reference

### `Database`

```rust
// Open or create a database
let db = Database::open("path/to/db.hdb")?;

// Execute DDL/DML (returns affected row count)
let affected = db.execute("INSERT INTO t VALUES (1, 'hello')")?;

// Query (returns rows)
let result = db.query("SELECT * FROM t WHERE id > 5")?;

// Access results
for row in &result.rows {
    let id: &Value = row.get("id").unwrap();
    let name: &Value = row.values[1];  // by index
}

// Close (flushes all writes)
db.close()?;
```

### `Value`

The five SQLite-compatible storage types:

```rust
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
```

### `Row`

```rust
row.get("column_name")   // -> Option<&Value>, case-insensitive
row.get_index(0)          // -> Option<&Value>, by position
row.values[0]             // direct access to value vector
```

## Building

```bash
# Debug build
cargo build

# Release build (optimized, LTO, stripped)
cargo build --release

# Run tests (645 tests)
cargo test

# Run benchmarks
cargo bench
```

## Benchmarks

Performance comparison against SQLite (1000-row tables, Criterion.rs, WAL mode).

### Highlights

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| Point lookup (`WHERE id = N`) | 24 us | 17 us | 1.4x |
| COUNT(*) | 5.4 us | 3.3 us | 1.7x |
| INSERT 1000 rows | 33 ms | 36 ms | **0.9x (faster)** |
| ORDER BY | 686 us | 354 us | 1.9x |
| JOIN 100x500 | 509 us | 73 us | 7.0x |
| UPDATE 100 rows | 6.1 ms | 1.0 ms | 6.2x |
| DELETE 100 rows | 6.1 ms | 1.1 ms | 5.4x |

### Full Results

| Category | Benchmark | Horizon DB | SQLite | Ratio |
|----------|-----------|-----------|--------|-------|
| **Insert** | 100 rows (individual) | 10.95 ms | 6.24 ms | 1.8x |
| | 1000 rows (individual) | 32.74 ms | 35.85 ms | **0.9x** |
| | 100 rows (multi-value) | 8.50 ms | 3.97 ms | 2.1x |
| | 1000 rows (transaction) | 27.13 ms | 2.97 ms | 9.1x |
| **Select** | SELECT * (1000 rows) | 3.54 ms | 1.64 ms | 2.2x |
| | WHERE id = 500 | 23.88 us | 16.62 us | 1.4x |
| | WHERE id range | 1.22 ms | 14.69 us | 83x |
| | WHERE LIKE | 872 us | 158 us | 5.5x |
| | Column projection | 517 us | 57 us | 9.1x |
| **Order/Distinct** | ORDER BY | 686 us | 354 us | 1.9x |
| | ORDER BY LIMIT 10 | 576 us | 351 us | 1.6x |
| | DISTINCT | 417 us | 126 us | 3.3x |
| **Aggregate** | COUNT(*) | 5.44 us | 3.29 us | 1.7x |
| | GROUP BY + COUNT + AVG | 2.21 ms | 586 us | 3.8x |
| | SUM + MIN + MAX | 8.83 ms | 448 us | 19.7x |
| **Join** | INNER JOIN 100x500 | 509 us | 73 us | 7.0x |
| | JOIN + WHERE | 541 us | 64 us | 8.5x |
| **Mutation** | UPDATE 100/1000 | 6.13 ms | 996 us | 6.2x |
| | DELETE 100/1000 | 6.07 ms | 1.13 ms | 5.4x |
| **Advanced** | CTE with filter | 808 us | 70 us | 11.5x |
| | Subquery IN | 1.26 ms | 136 us | 9.3x |
| | Window ROW_NUMBER | 1.16 ms | 833 us | 1.4x |

See [docs/benchmarks.md](docs/benchmarks.md) for methodology and optimization details.

```bash
cargo bench
```

## File Format

- **Data file** (`.hdb`): 4KB pages, B+Tree storage, binary serialized rows
- **WAL file** (`.hdb-wal`): Append-only write-ahead log for crash recovery
- **Single-file**: All tables, indexes, and metadata in one `.hdb` file

## Testing

```
645 tests total:
  346 unit tests   (storage, parsing, types, B+Tree, FTS5, R-tree)
  292 integration  (end-to-end SQL through Database API)
    7 doc tests    (Value type examples)
```

## License

MIT
