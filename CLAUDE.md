# Horizon DB — Developer Guide

A highly-performant serverless embedded RDBMS written in Rust with full SQLite feature parity.

## Quick Reference

- **Language**: Rust (edition 2021)
- **Build**: `cargo build` / `cargo build --release`
- **Test**: `cargo test` / `cargo test -- --nocapture`
- **Run CLI**: `cargo run -- <database.hdb>`
- **Bench**: `cargo bench`
- **Lint**: `cargo clippy -- -D warnings`
- **Format**: `cargo fmt --check`

## Architecture

```
┌─────────────────────────────────────────┐
│           CLI / REPL (horizon)          │
├─────────────────────────────────────────┤
│          Public Rust API (lib.rs)       │
├─────────────────────────────────────────┤
│          SQL Parser & Lexer             │
│          (src/sql/)                     │
├─────────────────────────────────────────┤
│       Query Planner / Optimizer         │
│          (src/planner/)                 │
├─────────────────────────────────────────┤
│         Execution Engine                │
│          (src/execution/)               │
├─────────────────────────────────────────┤
│       MVCC Transaction Manager          │
│          (src/mvcc/)                    │
├─────────────────────────────────────────┤
│       Catalog / Schema Manager          │
│          (src/catalog/)                 │
├─────────────────────────────────────────┤
│    B+Tree Index + Table Storage         │
│          (src/btree/)                   │
├─────────────────────────────────────────┤
│       Buffer Pool / Page Cache          │
│          (src/buffer/)                  │
├─────────────────────────────────────────┤
│        Pager / Page Manager             │
│          (src/pager/)                   │
├─────────────────────────────────────────┤
│      WAL (Write-Ahead Logging)          │
│          (src/wal/)                     │
├─────────────────────────────────────────┤
│          Type System                    │
│          (src/types/)                   │
├─────────────────────────────────────────┤
│          Error Handling                 │
│          (src/error/)                   │
└─────────────────────────────────────────┘
```

## Module Map

| Module | Path | Purpose |
|--------|------|---------|
| `error` | `src/error/` | Unified error types |
| `types` | `src/types/` | Value types (NULL, INTEGER, REAL, TEXT, BLOB) |
| `pager` | `src/pager/` | Low-level page I/O, file format |
| `wal` | `src/wal/` | Write-ahead logging for crash recovery |
| `buffer` | `src/buffer/` | Buffer pool with LRU eviction |
| `btree` | `src/btree/` | B+Tree for table/index storage |
| `mvcc` | `src/mvcc/` | MVCC transaction manager |
| `catalog` | `src/catalog/` | Schema, table, index metadata |
| `sql` | `src/sql/` | Tokenizer, parser, AST |
| `planner` | `src/planner/` | Query planning and optimization |
| `execution` | `src/execution/` | Volcano-model query execution |
| `cli` | `src/cli/` | Interactive REPL |

## Key Design Decisions

1. **MVCC Concurrency**: Readers never block writers. Each transaction sees a consistent snapshot.
2. **Hybrid Storage**: B+Tree for indexes/tables, append-only WAL for durability.
3. **Single File**: All data in one `.hdb` file (+ optional WAL sidecar during writes).
4. **Page-Based**: 4KB pages, buffer pool with LRU eviction.
5. **Hand-Written Parser**: No parser generator dependencies. Full SQL dialect support.
6. **Volcano Execution Model**: Iterator-based query execution for composability.

## Coding Conventions

- Use `Result<T, HorizonError>` for all fallible operations
- Prefer `&[u8]` over `Vec<u8>` for read paths
- All public APIs must have doc comments
- Use `#[cfg(test)]` modules within source files for unit tests
- Integration tests go in `tests/` directory
- No `unsafe` without clear justification and safety comments
- Minimize allocations on hot paths
- Use `thiserror`-style error types (implemented manually, no dependency)

## File Format (.hdb)

```
Page 0: File Header (magic, version, page size, page count, free list head, schema version)
Page 1: Schema table root (B+Tree storing table/index metadata)
Page 2+: Data pages (B+Tree internal/leaf nodes, overflow pages)
```

## Documentation

- [Architecture Deep Dive](docs/architecture.md)
- [File Format Specification](docs/file-format.md)
- [SQL Dialect Reference](docs/sql-reference.md)
- [MVCC Design](docs/mvcc.md)
- [Development Phases](docs/phases.md)
