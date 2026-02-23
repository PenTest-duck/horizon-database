use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use horizon::Database;
use rusqlite::Connection;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Horizon Helpers
// ---------------------------------------------------------------------------

fn setup_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path().join("bench.hdb")).unwrap();
    (dir, db)
}

fn setup_db_with_data(rows: usize) -> (TempDir, Database) {
    let (dir, db) = setup_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL, category TEXT)")
        .unwrap();
    for i in 0..rows {
        let cat = match i % 5 {
            0 => "alpha",
            1 => "beta",
            2 => "gamma",
            3 => "delta",
            _ => "epsilon",
        };
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'name_{}', {}.5, '{}')",
            i, i, i, cat
        ))
        .unwrap();
    }
    (dir, db)
}

// ---------------------------------------------------------------------------
// SQLite Helpers
// ---------------------------------------------------------------------------

fn setup_sqlite() -> (TempDir, Connection) {
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("bench.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    (dir, conn)
}

fn setup_sqlite_with_data(rows: usize) -> (TempDir, Connection) {
    let (_dir, conn) = setup_sqlite();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL, category TEXT)",
        [],
    )
    .unwrap();
    for i in 0..rows {
        let cat = match i % 5 {
            0 => "alpha",
            1 => "beta",
            2 => "gamma",
            3 => "delta",
            _ => "epsilon",
        };
        conn.execute(
            &format!(
                "INSERT INTO t VALUES ({}, 'name_{}', {}.5, '{}')",
                i, i, i, cat
            ),
            [],
        )
        .unwrap();
    }
    (_dir, conn)
}

/// Execute a SELECT and return the number of rows, consuming all results.
fn sqlite_row_count(conn: &Connection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

// ===========================================================================
// Insert benchmarks
// ===========================================================================

fn bench_insert_100(c: &mut Criterion) {
    c.bench_function("insert_100_rows", |b| {
        b.iter_batched(
            setup_db,
            |(_dir, db)| {
                db.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                )
                .unwrap();
                for i in 0..100 {
                    db.execute(&format!(
                        "INSERT INTO t VALUES ({}, 'name_{}', {}.5)",
                        i, i, i
                    ))
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_100_sqlite(c: &mut Criterion) {
    c.bench_function("insert_100_rows_sqlite", |b| {
        b.iter_batched(
            setup_sqlite,
            |(_dir, conn)| {
                conn.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                    [],
                )
                .unwrap();
                for i in 0..100 {
                    conn.execute(
                        &format!("INSERT INTO t VALUES ({}, 'name_{}', {}.5)", i, i, i),
                        [],
                    )
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_1000(c: &mut Criterion) {
    c.bench_function("insert_1000_rows", |b| {
        b.iter_batched(
            setup_db,
            |(_dir, db)| {
                db.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                )
                .unwrap();
                for i in 0..1000 {
                    db.execute(&format!(
                        "INSERT INTO t VALUES ({}, 'name_{}', {}.5)",
                        i, i, i
                    ))
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_1000_sqlite(c: &mut Criterion) {
    c.bench_function("insert_1000_rows_sqlite", |b| {
        b.iter_batched(
            setup_sqlite,
            |(_dir, conn)| {
                conn.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                    [],
                )
                .unwrap();
                for i in 0..1000 {
                    conn.execute(
                        &format!("INSERT INTO t VALUES ({}, 'name_{}', {}.5)", i, i, i),
                        [],
                    )
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_multi_value(c: &mut Criterion) {
    c.bench_function("insert_100_rows_multi_value", |b| {
        b.iter_batched(
            setup_db,
            |(_dir, db)| {
                db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
                    .unwrap();
                // Build a multi-row INSERT
                let mut sql = String::from("INSERT INTO t VALUES ");
                for i in 0..100 {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'name_{}')", i, i));
                }
                db.execute(&sql).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_multi_value_sqlite(c: &mut Criterion) {
    c.bench_function("insert_100_rows_multi_value_sqlite", |b| {
        b.iter_batched(
            setup_sqlite,
            |(_dir, conn)| {
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", [])
                    .unwrap();
                let mut sql = String::from("INSERT INTO t VALUES ");
                for i in 0..100 {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({}, 'name_{}')", i, i));
                }
                conn.execute(&sql, []).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

// ===========================================================================
// Select benchmarks
// ===========================================================================

fn bench_select_all(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("select_all_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM t").unwrap();
            assert_eq!(result.len(), 1000);
        });
    });
}

fn bench_select_all_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("select_all_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT * FROM t");
            assert_eq!(count, 1000);
        });
    });
}

fn bench_select_where_eq(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("select_where_eq_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM t WHERE id = 500").unwrap();
            assert_eq!(result.len(), 1);
        });
    });
}

fn bench_select_where_eq_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("select_where_eq_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT * FROM t WHERE id = 500");
            assert_eq!(count, 1);
        });
    });
}

fn bench_select_where_range(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("select_where_range_1000_rows", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT * FROM t WHERE id >= 200 AND id < 300")
                .unwrap();
            assert_eq!(result.len(), 100);
        });
    });
}

fn bench_select_where_range_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("select_where_range_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count =
                sqlite_row_count(&conn, "SELECT * FROM t WHERE id >= 200 AND id < 300");
            assert_eq!(count, 100);
        });
    });
}

fn bench_select_where_like(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("select_where_like_1000_rows", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT * FROM t WHERE name LIKE 'name_5%'")
                .unwrap();
            assert!(result.len() > 0);
        });
    });
}

fn bench_select_where_like_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("select_where_like_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count =
                sqlite_row_count(&conn, "SELECT * FROM t WHERE name LIKE 'name_5%'");
            assert!(count > 0);
        });
    });
}

fn bench_select_columns(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("select_two_columns_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, name FROM t").unwrap();
            assert_eq!(result.len(), 1000);
        });
    });
}

fn bench_select_columns_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("select_two_columns_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT id, name FROM t");
            assert_eq!(count, 1000);
        });
    });
}

// ===========================================================================
// Order / Limit / Distinct
// ===========================================================================

fn bench_order_by(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("order_by_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM t ORDER BY value DESC").unwrap();
            assert_eq!(result.len(), 1000);
        });
    });
}

fn bench_order_by_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("order_by_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT * FROM t ORDER BY value DESC");
            assert_eq!(count, 1000);
        });
    });
}

fn bench_order_by_with_limit(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("order_by_limit_10_of_1000", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT * FROM t ORDER BY value DESC LIMIT 10")
                .unwrap();
            assert_eq!(result.len(), 10);
        });
    });
}

fn bench_order_by_with_limit_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("order_by_limit_10_of_1000_sqlite", |b| {
        b.iter(|| {
            let count =
                sqlite_row_count(&conn, "SELECT * FROM t ORDER BY value DESC LIMIT 10");
            assert_eq!(count, 10);
        });
    });
}

fn bench_distinct(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("distinct_category_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT DISTINCT category FROM t").unwrap();
            assert_eq!(result.len(), 5);
        });
    });
}

fn bench_distinct_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("distinct_category_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT DISTINCT category FROM t");
            assert_eq!(count, 5);
        });
    });
}

// ===========================================================================
// Aggregate benchmarks
// ===========================================================================

fn bench_count(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("count_star_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT COUNT(*) FROM t").unwrap();
            assert_eq!(result.len(), 1);
        });
    });
}

fn bench_count_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("count_star_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(&conn, "SELECT COUNT(*) FROM t");
            assert_eq!(count, 1);
        });
    });
}

fn bench_group_by(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("group_by_count_1000_rows", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT category, COUNT(*), AVG(value) FROM t GROUP BY category")
                .unwrap();
            assert_eq!(result.len(), 5);
        });
    });
}

fn bench_group_by_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("group_by_count_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT category, COUNT(*), AVG(value) FROM t GROUP BY category",
            );
            assert_eq!(count, 5);
        });
    });
}

fn bench_aggregate_sum(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("sum_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT SUM(value), MIN(value), MAX(value) FROM t").unwrap();
            assert_eq!(result.len(), 1);
        });
    });
}

fn bench_aggregate_sum_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("sum_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT SUM(value), MIN(value), MAX(value) FROM t",
            );
            assert_eq!(count, 1);
        });
    });
}

// ===========================================================================
// Join benchmarks
// ===========================================================================

fn setup_sqlite_join_data() -> (TempDir, Connection) {
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("bench_join.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
        [],
    )
    .unwrap();
    for i in 0..100 {
        conn.execute(
            &format!("INSERT INTO customers VALUES ({}, 'customer_{}')", i, i),
            [],
        )
        .unwrap();
    }
    for i in 0..500 {
        conn.execute(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, {}.99)",
                i,
                i % 100,
                i
            ),
            [],
        )
        .unwrap();
    }
    (dir, conn)
}

fn bench_join_small(c: &mut Criterion) {
    let (_dir, db) = setup_db();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
        .unwrap();
    db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO customers VALUES ({}, 'customer_{}')", i, i))
            .unwrap();
    }
    for i in 0..500 {
        db.execute(&format!(
            "INSERT INTO orders VALUES ({}, {}, {}.99)",
            i,
            i % 100,
            i
        ))
        .unwrap();
    }

    c.bench_function("join_100x500", |b| {
        b.iter(|| {
            let result = db
                .query(
                    "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
                )
                .unwrap();
            assert_eq!(result.len(), 500);
        });
    });
}

fn bench_join_small_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_join_data();
    c.bench_function("join_100x500_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
            );
            assert_eq!(count, 500);
        });
    });
}

fn bench_join_with_where(c: &mut Criterion) {
    let (_dir, db) = setup_db();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
        .unwrap();
    db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO customers VALUES ({}, 'customer_{}')", i, i))
            .unwrap();
    }
    for i in 0..500 {
        db.execute(&format!(
            "INSERT INTO orders VALUES ({}, {}, {}.99)",
            i,
            i % 100,
            i
        ))
        .unwrap();
    }

    c.bench_function("join_100x500_with_where", |b| {
        b.iter(|| {
            let result = db
                .query(
                    "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 250.0",
                )
                .unwrap();
            assert!(result.len() > 0);
        });
    });
}

fn bench_join_with_where_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_join_data();
    c.bench_function("join_100x500_with_where_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > 250.0",
            );
            assert!(count > 0);
        });
    });
}

// ===========================================================================
// Index benchmarks
// ===========================================================================

fn bench_indexed_lookup(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    db.execute("CREATE INDEX idx_category ON t(category)").unwrap();
    c.bench_function("indexed_lookup_1000_rows", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT * FROM t WHERE category = 'alpha'")
                .unwrap();
            assert_eq!(result.len(), 200);
        });
    });
}

fn bench_indexed_lookup_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    conn.execute("CREATE INDEX idx_category ON t(category)", [])
        .unwrap();
    c.bench_function("indexed_lookup_1000_rows_sqlite", |b| {
        b.iter(|| {
            let count =
                sqlite_row_count(&conn, "SELECT * FROM t WHERE category = 'alpha'");
            assert_eq!(count, 200);
        });
    });
}

// ===========================================================================
// Update / Delete benchmarks
// ===========================================================================

fn bench_update(c: &mut Criterion) {
    c.bench_function("update_100_of_1000_rows", |b| {
        b.iter_batched(
            || setup_db_with_data(1000),
            |(_dir, db)| {
                let affected = db
                    .execute("UPDATE t SET value = value + 1 WHERE id < 100")
                    .unwrap();
                assert_eq!(affected, 100);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_update_sqlite(c: &mut Criterion) {
    c.bench_function("update_100_of_1000_rows_sqlite", |b| {
        b.iter_batched(
            || setup_sqlite_with_data(1000),
            |(_dir, conn)| {
                let affected = conn
                    .execute("UPDATE t SET value = value + 1 WHERE id < 100", [])
                    .unwrap();
                assert_eq!(affected, 100);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_delete(c: &mut Criterion) {
    c.bench_function("delete_100_of_1000_rows", |b| {
        b.iter_batched(
            || setup_db_with_data(1000),
            |(_dir, db)| {
                let affected = db.execute("DELETE FROM t WHERE id < 100").unwrap();
                assert_eq!(affected, 100);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_delete_sqlite(c: &mut Criterion) {
    c.bench_function("delete_100_of_1000_rows_sqlite", |b| {
        b.iter_batched(
            || setup_sqlite_with_data(1000),
            |(_dir, conn)| {
                let affected = conn
                    .execute("DELETE FROM t WHERE id < 100", [])
                    .unwrap();
                assert_eq!(affected, 100);
            },
            BatchSize::SmallInput,
        );
    });
}

// ===========================================================================
// Transaction benchmarks
// ===========================================================================

fn bench_transaction_insert(c: &mut Criterion) {
    c.bench_function("transaction_insert_1000_rows", |b| {
        b.iter_batched(
            setup_db,
            |(_dir, db)| {
                db.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                )
                .unwrap();
                db.execute("BEGIN").unwrap();
                for i in 0..1000 {
                    db.execute(&format!(
                        "INSERT INTO t VALUES ({}, 'name_{}', {}.5)",
                        i, i, i
                    ))
                    .unwrap();
                }
                db.execute("COMMIT").unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_transaction_insert_sqlite(c: &mut Criterion) {
    c.bench_function("transaction_insert_1000_rows_sqlite", |b| {
        b.iter_batched(
            setup_sqlite,
            |(_dir, conn)| {
                conn.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                    [],
                )
                .unwrap();
                conn.execute_batch("BEGIN").unwrap();
                for i in 0..1000 {
                    conn.execute(
                        &format!("INSERT INTO t VALUES ({}, 'name_{}', {}.5)", i, i, i),
                        [],
                    )
                    .unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

// ===========================================================================
// CTE / Subquery / Window benchmarks
// ===========================================================================

fn bench_cte(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(1000);
    c.bench_function("cte_with_filter", |b| {
        b.iter(|| {
            let result = db
                .query(
                    "WITH top_items AS (SELECT * FROM t WHERE value > 500.0) \
                     SELECT COUNT(*) FROM top_items",
                )
                .unwrap();
            assert_eq!(result.len(), 1);
        });
    });
}

fn bench_cte_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(1000);
    c.bench_function("cte_with_filter_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "WITH top_items AS (SELECT * FROM t WHERE value > 500.0) \
                 SELECT COUNT(*) FROM top_items",
            );
            assert_eq!(count, 1);
        });
    });
}

fn bench_subquery_in(c: &mut Criterion) {
    let (_dir, db) = setup_db();
    db.execute("CREATE TABLE categories (name TEXT)").unwrap();
    db.execute("INSERT INTO categories VALUES ('alpha')").unwrap();
    db.execute("INSERT INTO categories VALUES ('beta')").unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, cat TEXT, val REAL)")
        .unwrap();
    for i in 0..500 {
        let cat = if i % 2 == 0 { "alpha" } else { "gamma" };
        db.execute(&format!("INSERT INTO items VALUES ({}, '{}', {}.0)", i, cat, i))
            .unwrap();
    }

    c.bench_function("subquery_in_500_rows", |b| {
        b.iter(|| {
            let result = db
                .query("SELECT * FROM items WHERE cat IN (SELECT name FROM categories)")
                .unwrap();
            assert_eq!(result.len(), 250);
        });
    });
}

fn bench_subquery_in_sqlite(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let conn = Connection::open(dir.path().join("bench_subquery.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn.execute("CREATE TABLE categories (name TEXT)", []).unwrap();
    conn.execute("INSERT INTO categories VALUES ('alpha')", [])
        .unwrap();
    conn.execute("INSERT INTO categories VALUES ('beta')", [])
        .unwrap();
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, cat TEXT, val REAL)",
        [],
    )
    .unwrap();
    for i in 0..500 {
        let cat = if i % 2 == 0 { "alpha" } else { "gamma" };
        conn.execute(
            &format!("INSERT INTO items VALUES ({}, '{}', {}.0)", i, cat, i),
            [],
        )
        .unwrap();
    }
    let _dir = dir;

    c.bench_function("subquery_in_500_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT * FROM items WHERE cat IN (SELECT name FROM categories)",
            );
            assert_eq!(count, 250);
        });
    });
}

fn bench_window_function(c: &mut Criterion) {
    let (_dir, db) = setup_db_with_data(500);
    c.bench_function("window_row_number_500_rows", |b| {
        b.iter(|| {
            let result = db
                .query(
                    "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM t",
                )
                .unwrap();
            assert_eq!(result.len(), 500);
        });
    });
}

fn bench_window_function_sqlite(c: &mut Criterion) {
    let (_dir, conn) = setup_sqlite_with_data(500);
    c.bench_function("window_row_number_500_rows_sqlite", |b| {
        b.iter(|| {
            let count = sqlite_row_count(
                &conn,
                "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM t",
            );
            assert_eq!(count, 500);
        });
    });
}

// ===========================================================================
// SQL Parsing benchmark (Horizon-only, no SQLite equivalent)
// ===========================================================================

fn bench_parse_complex_sql(c: &mut Criterion) {
    c.bench_function("parse_complex_select", |b| {
        b.iter(|| {
            let sql = "SELECT u.id, u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total \
                        FROM users u \
                        LEFT JOIN orders o ON u.id = o.user_id \
                        WHERE u.active = 1 AND o.created > '2024-01-01' \
                        GROUP BY u.id, u.name \
                        HAVING COUNT(o.id) > 5 \
                        ORDER BY total DESC \
                        LIMIT 10 OFFSET 20";
            let _stmts = horizon::sql::parser::Parser::parse(sql).unwrap();
        });
    });
}

// ===========================================================================
// Groups — Horizon and SQLite side-by-side
// ===========================================================================

criterion_group!(
    insert_benches,
    bench_insert_100,
    bench_insert_100_sqlite,
    bench_insert_1000,
    bench_insert_1000_sqlite,
    bench_insert_multi_value,
    bench_insert_multi_value_sqlite,
);

criterion_group!(
    select_benches,
    bench_select_all,
    bench_select_all_sqlite,
    bench_select_where_eq,
    bench_select_where_eq_sqlite,
    bench_select_where_range,
    bench_select_where_range_sqlite,
    bench_select_where_like,
    bench_select_where_like_sqlite,
    bench_select_columns,
    bench_select_columns_sqlite,
);

criterion_group!(
    sort_limit_benches,
    bench_order_by,
    bench_order_by_sqlite,
    bench_order_by_with_limit,
    bench_order_by_with_limit_sqlite,
    bench_distinct,
    bench_distinct_sqlite,
);

criterion_group!(
    aggregate_benches,
    bench_count,
    bench_count_sqlite,
    bench_group_by,
    bench_group_by_sqlite,
    bench_aggregate_sum,
    bench_aggregate_sum_sqlite,
);

criterion_group!(
    join_benches,
    bench_join_small,
    bench_join_small_sqlite,
    bench_join_with_where,
    bench_join_with_where_sqlite,
);

criterion_group!(
    index_benches,
    bench_indexed_lookup,
    bench_indexed_lookup_sqlite,
);

criterion_group!(
    mutation_benches,
    bench_update,
    bench_update_sqlite,
    bench_delete,
    bench_delete_sqlite,
    bench_transaction_insert,
    bench_transaction_insert_sqlite,
);

criterion_group!(
    advanced_benches,
    bench_cte,
    bench_cte_sqlite,
    bench_subquery_in,
    bench_subquery_in_sqlite,
    bench_window_function,
    bench_window_function_sqlite,
);

criterion_group!(parse_benches, bench_parse_complex_sql,);

criterion_main!(
    insert_benches,
    select_benches,
    sort_limit_benches,
    aggregate_benches,
    join_benches,
    index_benches,
    mutation_benches,
    advanced_benches,
    parse_benches,
);
