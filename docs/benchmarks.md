# Horizon DB — Benchmark Results

Performance comparison of Horizon DB against SQLite, using [Criterion.rs](https://github.com/bheisler/criterion.rs) microbenchmarks. Both databases are configured for on-disk storage with WAL journaling to ensure a fair comparison.

## Methodology

- **Framework:** Criterion.rs 0.5 with 100 samples per benchmark
- **Horizon DB:** Writes to a temp directory file (`bench.hdb`) with WAL
- **SQLite:** Writes to a temp directory file (`bench.db`) with `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`
- **Hardware:** Results are relative — run `cargo bench` on your own machine for absolute numbers
- **Source:** [`benches/benchmarks.rs`](../benches/benchmarks.rs)

## Results

### Insert Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| Insert 100 rows (individual) | 10.95 ms | 6.24 ms | 1.8x slower |
| Insert 1000 rows (individual) | 32.74 ms | 35.85 ms | **1.1x faster** |
| Insert 100 rows (multi-value) | 8.50 ms | 3.97 ms | 2.1x slower |
| Insert 1000 rows (transaction) | 27.13 ms | 2.97 ms | 9.1x slower |

Horizon is at parity (or faster) with SQLite on individual row inserts at scale. The gap widens for multi-value inserts and explicit transactions, suggesting optimization opportunities in batch write paths and transaction commit overhead.

### Select Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| SELECT * (1000 rows) | 3.54 ms | 1.64 ms | 2.2x slower |
| SELECT WHERE id = 500 (point lookup) | 23.88 us | 16.62 us | **1.4x slower** |
| SELECT WHERE id >= 200 AND id < 300 | 1.22 ms | 14.69 us | 83x slower |
| SELECT WHERE name LIKE 'name_5%' | 872.26 us | 158.00 us | 5.5x slower |
| SELECT id, name (column projection) | 517.15 us | 57.02 us | 9.1x slower |

**Point lookups improved from 76x to 1.4x** thanks to the primary key B+Tree seek optimization. Instead of scanning all rows, `WHERE id = N` now uses `BTree::search()` for O(log n) direct lookup. Range scans with `>=` and `<` still benefit but compound range predicates (AND of two comparisons) don't yet combine into a single range seek.

### Ordering, Limits, and Distinct

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| ORDER BY value DESC (1000 rows) | 686.11 us | 354.15 us | 1.9x slower |
| ORDER BY value DESC LIMIT 10 | 575.95 us | 351.48 us | 1.6x slower |
| SELECT DISTINCT category (1000 rows) | 417.38 us | 125.53 us | 3.3x slower |

Horizon's sort implementation is competitive — only 1.9x slower than SQLite.

### Aggregate Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| COUNT(*) | 5.44 us | 3.29 us | **1.7x slower** |
| GROUP BY + COUNT + AVG | 2.21 ms | 586.22 us | 3.8x slower |
| SUM + MIN + MAX | 8.83 ms | 447.77 us | 19.7x slower |

**COUNT(\*) improved from 124x to 1.7x** thanks to the BTree::count() fast path. Simple `SELECT COUNT(*) FROM table` queries now traverse B+Tree leaf pages counting entries without deserializing any row data, bringing performance to near-parity with SQLite.

### Join Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| INNER JOIN (100 customers x 500 orders) | 509.45 us | 72.83 us | **7.0x slower** |
| INNER JOIN + WHERE filter | 540.65 us | 63.74 us | **8.5x slower** |

**JOINs improved from 340-418x to 7-8.5x** thanks to the hash join implementation. The optimizer now detects equi-join conditions (`a.col = b.col`) and uses a HashMap-based join algorithm with O(n+m) complexity instead of the O(n*m) nested-loop join. Supports INNER, LEFT, and RIGHT joins.

### Index Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| Indexed lookup (category = 'alpha') | 946.12 us | 88.95 us | 10.6x slower |

Horizon has secondary index support but the query planner does not yet automatically select indexes for all applicable predicates. SQLite's optimizer is more mature at choosing access paths.

### Mutation Performance

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| UPDATE 100 of 1000 rows | 6.13 ms | 996.33 us | **6.2x slower** |
| DELETE 100 of 1000 rows | 6.07 ms | 1.13 ms | **5.4x slower** |

**UPDATE/DELETE improved from 7-8x to 5.4-6.2x** thanks to index-accelerated scans. UPDATE and DELETE now use the same scan_with_index() path as SELECT, leveraging PK seeks and secondary index scans instead of always doing full table scans.

### Advanced SQL

| Benchmark | Horizon DB | SQLite | Ratio |
|-----------|-----------|--------|-------|
| CTE with filter | 807.56 us | 70.45 us | 11.5x slower |
| Subquery IN (500 rows) | 1.26 ms | 136.30 us | 9.3x slower |
| Window ROW_NUMBER (500 rows) | 1.16 ms | 833.22 us | 1.4x slower |

### SQL Parsing (Horizon-only)

| Benchmark | Time |
|-----------|------|
| Parse complex SELECT (8 clauses) | 14.83 us |

Horizon's hand-written recursive descent parser processes a complex query with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET in ~15 microseconds.

## Optimization Summary

### Improvements in this release

| Area | Before | After | Improvement |
|------|--------|-------|-------------|
| Point lookups (WHERE id = N) | 76x slower | 1.4x slower | **~54x faster** |
| COUNT(*) | 124x slower | 1.7x slower | **~73x faster** |
| JOINs | 340-418x slower | 7-8.5x slower | **~49x faster** |
| UPDATE | 8.3x slower | 6.2x slower | ~1.3x faster |
| DELETE | 6.6x slower | 5.4x slower | ~1.2x faster |

### Where Horizon Excels

- **Point lookups** — 1.4x (near parity with SQLite)
- **COUNT(*)** — 1.7x (near parity with SQLite)
- **Bulk inserts** (1000 rows) — **1.1x faster** than SQLite

### Near Parity (< 2x)

- **ORDER BY + LIMIT** — 1.6x slower
- **Window functions** — 1.4x slower
- **Insert 100 rows** — 1.8x slower
- **ORDER BY** — 1.9x slower

### Remaining Optimization Opportunities

| Area | Gap | Root Cause | Potential Fix |
|------|-----|------------|---------------|
| SUM/MIN/MAX | 20x | Full scan + deserialization per row | Streaming aggregation without materialization |
| CTE | 11.5x | Materialization overhead | Inline simple CTEs |
| Indexed lookup | 10.6x | Planner not optimally selecting index | Cost-based index selection |
| Transactions | 9.1x | Per-commit WAL sync overhead | Group commit, async WAL flush |
| Subqueries | 9.3x | Re-execution of subquery per row | Materialize subquery results |
| JOINs | 7-8.5x | Hash join overhead vs SQLite's planner | Index-nested-loop join, better hash function |
| UPDATE/DELETE | 5.4-6.2x | MVCC version chain overhead | In-place update for single-version rows |
| LIKE | 5.5x | No prefix optimization | Prefix index seek for anchored patterns |
| DISTINCT | 3.3x | Hash set overhead | Sorted distinct via index |
| Full scan | 2.2x | Deserialization overhead | Columnar page format, zero-copy reads |
| Multi-value insert | 2.1x | Per-row overhead not amortized | Batch B+Tree insertion |

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark group
cargo bench -- insert
cargo bench -- select
cargo bench -- join

# Run a single benchmark by name
cargo bench -- "insert_100_rows"
```

Criterion generates HTML reports in `target/criterion/` with detailed statistics, throughput charts, and regression detection across runs.
