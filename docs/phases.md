# Horizon DB — Development Phases

## Phase 1: Foundation (Storage Layer)
- [x] Project structure, Cargo.toml, CI config
- [ ] Error types (`HorizonError`)
- [ ] Value types (NULL, INTEGER, REAL, TEXT, BLOB)
- [ ] Page format and Pager (read/write 4KB pages)
- [ ] WAL manager (append, checkpoint, recovery)
- [ ] Buffer pool with LRU eviction
- [ ] B+Tree implementation (insert, search, delete, scan)
- [ ] Serialization (values ↔ bytes)

## Phase 2: Core SQL Engine
- [ ] SQL Tokenizer/Lexer
- [ ] SQL Parser → AST (expressions, DDL, DML)
- [ ] Catalog/Schema manager (CREATE TABLE metadata)
- [ ] Basic execution engine (table scan)
- [ ] CREATE TABLE, DROP TABLE
- [ ] INSERT, SELECT (basic), UPDATE, DELETE
- [ ] WHERE clause evaluation
- [ ] PRIMARY KEY, NOT NULL, UNIQUE constraints
- [ ] Basic type affinity system

## Phase 3: Indexes & Transactions
- [ ] CREATE INDEX / DROP INDEX
- [ ] Index scan in query execution
- [ ] MVCC transaction manager (begin, commit, rollback)
- [ ] Snapshot isolation
- [ ] Deadlock detection
- [ ] AUTOCOMMIT mode

## Phase 4: Query Processing
- [ ] Query planner (cost-based)
- [ ] JOIN algorithms (nested loop, hash join)
- [ ] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] GROUP BY, HAVING
- [ ] ORDER BY (external sort)
- [ ] LIMIT, OFFSET
- [ ] Subqueries (scalar, EXISTS, IN)
- [ ] DISTINCT
- [ ] UNION / INTERSECT / EXCEPT

## Phase 5: Advanced SQL
- [ ] Views (CREATE VIEW)
- [ ] Triggers (CREATE TRIGGER)
- [ ] CTEs (WITH ... AS)
- [ ] Recursive CTEs
- [ ] Window functions (ROW_NUMBER, RANK, etc.)
- [ ] UPSERT (INSERT OR REPLACE / ON CONFLICT)
- [ ] RETURNING clause
- [ ] ALTER TABLE (ADD COLUMN, RENAME)
- [ ] CASE expressions
- [ ] CAST / type coercion
- [ ] Collation sequences

## Phase 6: Extensions
- [ ] Built-in scalar functions (string, math, date/time)
- [ ] JSON functions (json_extract, json_array, etc.)
- [ ] FTS5 (Full-Text Search)
- [ ] R-tree (spatial indexing)
- [ ] Generated columns (STORED, VIRTUAL)
- [ ] Virtual tables framework
- [ ] EXPLAIN / EXPLAIN QUERY PLAN
- [ ] ATTACH DATABASE
- [ ] VACUUM
- [ ] PRAGMA commands

## Phase 7: CLI & Polish
- [ ] Interactive REPL with line editing
- [ ] Dot-commands (.tables, .schema, .import, .dump)
- [ ] CSV import/export
- [ ] SQL dump import/export
- [ ] Performance benchmarks
- [ ] Comprehensive test suite (SQLite compatibility tests)
- [ ] Documentation
