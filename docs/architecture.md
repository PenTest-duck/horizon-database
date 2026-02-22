# Horizon DB — Architecture

## Overview

Horizon DB is an embedded RDBMS that stores all data in a single `.hdb` file. It uses page-based storage with a B+Tree index structure and MVCC for concurrency control.

## Layered Architecture

### Layer 1: Disk I/O (Pager)
The Pager manages raw page I/O. Each page is 4096 bytes. The Pager knows nothing about what's in the pages — it just reads and writes fixed-size blocks.

**Responsibilities:**
- Open/create database files
- Read page N from disk
- Write page N to disk
- Allocate new pages
- Manage file header (page 0)
- Track total page count

### Layer 2: Write-Ahead Log (WAL)
The WAL ensures durability and crash recovery. All modifications go to the WAL before the main database file.

**Responsibilities:**
- Append page images before modification
- Checkpoint: flush WAL to main database
- Recovery: replay WAL on startup
- WAL file management (`.hdb-wal`)

### Layer 3: Buffer Pool
A fixed-size cache of pages in memory. Uses LRU eviction. All page access goes through the buffer pool.

**Responsibilities:**
- Cache frequently accessed pages
- Pin/unpin pages for active use
- Dirty page tracking
- LRU eviction when pool is full
- Flush dirty pages through WAL

### Layer 4: B+Tree
The primary data structure for both table storage and indexes. Each table is a B+Tree keyed by rowid. Each index is a B+Tree keyed by the indexed column(s).

**Key properties:**
- Internal nodes: keys + child page pointers
- Leaf nodes: keys + values (for tables) or keys + rowids (for indexes)
- Leaf nodes linked for efficient range scans
- Variable-length keys and values with overflow pages

### Layer 5: MVCC Transaction Manager
Provides snapshot isolation using multi-version concurrency control.

**How it works:**
- Each row version has a creation txn_id and optional deletion txn_id
- Readers see a consistent snapshot based on their start timestamp
- Writers create new versions; old versions are kept for active readers
- Garbage collection removes versions no longer visible to any transaction

### Layer 6: Catalog
Stores metadata about tables, columns, indexes, views, and triggers. The catalog itself is stored in special B+Trees (the schema tables).

### Layer 7: SQL Engine
Hand-written recursive descent parser producing an AST. The AST is transformed by the planner into an execution plan.

### Layer 8: Execution Engine
Volcano-model (iterator-based) execution. Each operator implements `next()` returning one row at a time.

**Operators:**
- SeqScan, IndexScan
- Filter, Project
- NestedLoopJoin, HashJoin
- Sort, Aggregate
- Limit, Offset

## Data Flow

```
SQL Text
  → Tokenizer → Token stream
  → Parser → AST
  → Planner → Logical Plan → Physical Plan
  → Executor → Row Iterator
  → B+Tree operations (via Buffer Pool → WAL → Pager → Disk)
```

## Page Layout

All pages are 4096 bytes with an 8-byte header:

```
[0..1]  Page type (1 = internal, 2 = leaf, 3 = overflow, 4 = freelist)
[1..2]  Flags
[2..4]  Cell count
[4..8]  Right-most child pointer (internal) / next leaf pointer (leaf)
[8..]   Cell pointer array + cell data (grows from both ends)
```

## Transaction IDs

- 64-bit monotonically increasing integers
- Stored in file header
- Each transaction gets a unique ID at BEGIN time
- Commit writes the txn_id to a commit log
