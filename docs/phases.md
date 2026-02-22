# Horizon DB — Development Phases

## Phase 1: Foundation (Storage Layer)
- [x] Project structure, Cargo.toml, CI config
- [x] Error types (`HorizonError`)
- [x] Value types (NULL, INTEGER, REAL, TEXT, BLOB)
- [x] Page format and Pager (read/write 4KB pages)
- [x] WAL manager (append, checkpoint, recovery)
- [x] Buffer pool with LRU eviction
- [x] B+Tree implementation (insert, search, delete, scan)
- [x] Serialization (values ↔ bytes)

## Phase 2: Core SQL Engine
- [x] SQL Tokenizer/Lexer
- [x] SQL Parser → AST (expressions, DDL, DML)
- [x] Catalog/Schema manager (CREATE TABLE metadata)
- [x] Basic execution engine (table scan)
- [x] CREATE TABLE, DROP TABLE
- [x] INSERT, SELECT (basic), UPDATE, DELETE
- [x] WHERE clause evaluation
- [x] PRIMARY KEY, NOT NULL, UNIQUE constraints
- [x] Basic type affinity system

## Phase 3: Indexes & Transactions
- [x] CREATE INDEX / DROP INDEX
- [x] Index scan in query execution
- [x] MVCC transaction manager (begin, commit, rollback)
- [x] Snapshot isolation
- [x] Deadlock detection
- [x] AUTOCOMMIT mode

## Phase 4: Query Processing
- [x] Query planner (rule-based)
- [x] JOIN algorithms (nested loop)
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [x] GROUP BY, HAVING
- [x] ORDER BY (external sort)
- [x] LIMIT, OFFSET
- [x] Subqueries (scalar, EXISTS, IN)
- [x] DISTINCT
- [x] UNION / INTERSECT / EXCEPT

## Phase 5: Advanced SQL
- [x] Views (CREATE VIEW)
- [x] Triggers (CREATE TRIGGER)
- [x] CTEs (WITH ... AS)
- [x] Recursive CTEs
- [x] Window functions (ROW_NUMBER, RANK, etc.)
- [x] UPSERT (INSERT OR REPLACE / ON CONFLICT)
- [x] RETURNING clause
- [x] ALTER TABLE (ADD COLUMN, RENAME)
- [x] CASE expressions
- [x] CAST / type coercion
- [x] Collation sequences

## Phase 6: Extensions
- [x] Built-in scalar functions (string, math, date/time)
- [x] JSON functions (json_extract, json_array, etc.)
- [ ] FTS5 (Full-Text Search)
- [ ] R-tree (spatial indexing)
- [x] Generated columns (STORED, VIRTUAL)
- [ ] Virtual tables framework
- [ ] EXPLAIN / EXPLAIN QUERY PLAN
- [x] ATTACH DATABASE
- [x] VACUUM
- [ ] PRAGMA commands

## Phase 7: CLI & Polish
- [x] Interactive REPL with line editing
- [x] Dot-commands (.tables, .schema, .import, .dump)
- [x] CSV import/export
- [x] SQL dump import/export
- [ ] Performance benchmarks
- [x] Comprehensive test suite (594 tests)
- [ ] Documentation
