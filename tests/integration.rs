use horizon::{Database, HorizonError, Value};
use tempfile::TempDir;

fn open_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path().join("test.hdb")).unwrap();
    (dir, db)
}

#[test]
fn create_table_and_insert() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
    let affected = db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    assert_eq!(affected, 1);
}

#[test]
fn insert_and_select() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "id");
    assert_eq!(result.columns[1], "name");
    assert_eq!(result.columns[2], "age");
}

#[test]
fn select_with_where() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)").unwrap();
    db.execute("INSERT INTO items VALUES (1, 'Apple', 1.50)").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'Banana', 0.75)").unwrap();
    db.execute("INSERT INTO items VALUES (3, 'Cherry', 3.00)").unwrap();

    let result = db.query("SELECT * FROM items WHERE price > 1.0").unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn select_specific_columns() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();

    let result = db.query("SELECT name, age FROM users").unwrap();
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "name");
    assert_eq!(result.columns[1], "age");
    assert_eq!(result.len(), 1);

    let row = &result.rows[0];
    assert_eq!(row.get("name"), Some(&Value::Text("Alice".to_string())));
    assert_eq!(row.get("age"), Some(&Value::Integer(30)));
}

#[test]
fn update_rows() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();

    let affected = db.execute("UPDATE users SET age = 31 WHERE id = 1").unwrap();
    assert_eq!(affected, 1);

    let result = db.query("SELECT age FROM users WHERE id = 1").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("age"), Some(&Value::Integer(31)));
}

#[test]
fn delete_rows() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    let affected = db.execute("DELETE FROM users WHERE id = 2").unwrap();
    assert_eq!(affected, 1);

    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn order_by() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO nums VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO nums VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO nums VALUES (3, 20)").unwrap();

    let result = db.query("SELECT * FROM nums ORDER BY val").unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Integer(10)));
    assert_eq!(result.rows[1].get("val"), Some(&Value::Integer(20)));
    assert_eq!(result.rows[2].get("val"), Some(&Value::Integer(30)));
}

#[test]
fn limit_and_offset() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO nums VALUES ({}, {})", i, i * 10)).unwrap();
    }

    let result = db.query("SELECT * FROM nums LIMIT 3").unwrap();
    assert_eq!(result.len(), 3);

    let result = db.query("SELECT * FROM nums LIMIT 3 OFFSET 5").unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn multiple_tables() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT)").unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, b TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'hello')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 'world')").unwrap();

    let r1 = db.query("SELECT * FROM t1").unwrap();
    let r2 = db.query("SELECT * FROM t2").unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r1.rows[0].get("a"), Some(&Value::Text("hello".to_string())));
    assert_eq!(r2.rows[0].get("b"), Some(&Value::Text("world".to_string())));
}

#[test]
fn drop_table() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE temp (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO temp VALUES (1)").unwrap();
    db.execute("DROP TABLE temp").unwrap();

    // Table should no longer exist
    assert!(db.query("SELECT * FROM temp").is_err());
}

#[test]
fn null_handling() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'hello')").unwrap();

    let result = db.query("SELECT * FROM t WHERE val IS NULL").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(1)));

    let result = db.query("SELECT * FROM t WHERE val IS NOT NULL").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(2)));
}

#[test]
fn expressions_in_select() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)").unwrap();
    db.execute("INSERT INTO nums VALUES (1, 10, 20)").unwrap();

    let result = db.query("SELECT a + b FROM nums").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(30));
}

#[test]
fn reopen_database() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("persist.hdb");

    // Create and populate
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO data VALUES (1, 'persisted')").unwrap();
        db.close().unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("SELECT * FROM data").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows[0].get("val"), Some(&Value::Text("persisted".to_string())));
    }
}

#[test]
fn large_insert_and_scan() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, name TEXT, value REAL)").unwrap();

    for i in 0..500 {
        db.execute(&format!(
            "INSERT INTO big VALUES ({}, 'row_{}', {}.5)",
            i, i, i
        )).unwrap();
    }

    let result = db.query("SELECT * FROM big").unwrap();
    assert_eq!(result.len(), 500);
}

// ---- CREATE INDEX / DROP INDEX / Index Scan Tests ----

#[test]
fn create_index_on_existing_data() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)").unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Apple', 1.50)").unwrap();
    db.execute("INSERT INTO products VALUES (2, 'Banana', 0.75)").unwrap();
    db.execute("INSERT INTO products VALUES (3, 'Cherry', 3.00)").unwrap();
    db.execute("INSERT INTO products VALUES (4, 'Date', 5.00)").unwrap();
    db.execute("INSERT INTO products VALUES (5, 'Elderberry', 8.00)").unwrap();

    // Create an index on the name column after data is inserted
    db.execute("CREATE INDEX idx_products_name ON products (name)").unwrap();

    // Queries should still return correct results after index creation
    let result = db.query("SELECT * FROM products").unwrap();
    assert_eq!(result.len(), 5);

    // Equality query on indexed column should work
    let result = db.query("SELECT * FROM products WHERE name = 'Cherry'").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(3)));
    assert_eq!(result.rows[0].get("price"), Some(&Value::Real(3.0)));
}

#[test]
fn create_index_if_not_exists() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE INDEX idx_t_val ON t (val)").unwrap();

    // Creating again without IF NOT EXISTS should fail
    assert!(db.execute("CREATE INDEX idx_t_val ON t (val)").is_err());

    // Creating with IF NOT EXISTS should succeed silently
    db.execute("CREATE INDEX IF NOT EXISTS idx_t_val ON t (val)").unwrap();
}

#[test]
fn drop_index_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    db.execute("CREATE INDEX idx_t_val ON t (val)").unwrap();

    // Drop the index
    db.execute("DROP INDEX idx_t_val").unwrap();

    // Queries should still work after dropping the index (full table scan)
    let result = db.query("SELECT * FROM t WHERE val = 'hello'").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(1)));
}

#[test]
fn drop_index_if_exists() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // Drop non-existent index without IF EXISTS should fail
    assert!(db.execute("DROP INDEX idx_nonexistent").is_err());

    // Drop non-existent index with IF EXISTS should succeed silently
    db.execute("DROP INDEX IF EXISTS idx_nonexistent").unwrap();
}

#[test]
fn index_query_equality_integer() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, score INTEGER)").unwrap();
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO scores VALUES ({}, 'student_{}', {})", i, i, i * 5)).unwrap();
    }

    // Create index on the score column
    db.execute("CREATE INDEX idx_scores_score ON scores (score)").unwrap();

    // Equality query on indexed integer column
    let result = db.query("SELECT * FROM scores WHERE score = 50").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("student"), Some(&Value::Text("student_10".to_string())));
}

#[test]
fn index_query_equality_text() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, city TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'NYC')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'LA')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'NYC')").unwrap();
    db.execute("INSERT INTO users VALUES (4, 'Diana', 'Chicago')").unwrap();

    db.execute("CREATE INDEX idx_users_city ON users (city)").unwrap();

    // Query on indexed text column
    let result = db.query("SELECT name FROM users WHERE city = 'NYC'").unwrap();
    assert_eq!(result.len(), 2);
    // Collect names
    let mut names: Vec<String> = result.rows.iter()
        .map(|r| r.get("name").unwrap().as_text().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn index_maintained_on_insert() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, tag TEXT)").unwrap();
    db.execute("INSERT INTO items VALUES (1, 'red')").unwrap();

    // Create index after first insert
    db.execute("CREATE INDEX idx_items_tag ON items (tag)").unwrap();

    // Insert more data after index creation -- the index should be maintained
    db.execute("INSERT INTO items VALUES (2, 'blue')").unwrap();
    db.execute("INSERT INTO items VALUES (3, 'red')").unwrap();
    db.execute("INSERT INTO items VALUES (4, 'green')").unwrap();

    // Query using the index should find all matching rows
    let result = db.query("SELECT * FROM items WHERE tag = 'red'").unwrap();
    assert_eq!(result.len(), 2);

    let result = db.query("SELECT * FROM items WHERE tag = 'blue'").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(2)));
}

#[test]
fn index_correct_results_after_creation() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)").unwrap();

    // Insert a variety of data
    db.execute("INSERT INTO data VALUES (1, 'A', 100)").unwrap();
    db.execute("INSERT INTO data VALUES (2, 'B', 200)").unwrap();
    db.execute("INSERT INTO data VALUES (3, 'A', 300)").unwrap();
    db.execute("INSERT INTO data VALUES (4, 'C', 400)").unwrap();
    db.execute("INSERT INTO data VALUES (5, 'B', 500)").unwrap();

    // Query without index
    let without_index = db.query("SELECT * FROM data WHERE category = 'B'").unwrap();
    assert_eq!(without_index.len(), 2);

    // Create index
    db.execute("CREATE INDEX idx_data_category ON data (category)").unwrap();

    // Query with index should return identical results
    let with_index = db.query("SELECT * FROM data WHERE category = 'B'").unwrap();
    assert_eq!(with_index.len(), 2);

    // Verify exact same rows
    for i in 0..without_index.len() {
        assert_eq!(without_index.rows[i].values, with_index.rows[i].values);
    }
}

#[test]
fn index_range_query_gt() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO nums VALUES ({}, {})", i, i * 10)).unwrap();
    }

    db.execute("CREATE INDEX idx_nums_val ON nums (val)").unwrap();

    // Greater than query
    let result = db.query("SELECT * FROM nums WHERE val > 70").unwrap();
    assert_eq!(result.len(), 3); // 80, 90, 100
}

#[test]
fn index_range_query_lt() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO nums VALUES ({}, {})", i, i * 10)).unwrap();
    }

    db.execute("CREATE INDEX idx_nums_val ON nums (val)").unwrap();

    // Less than query
    let result = db.query("SELECT * FROM nums WHERE val < 30").unwrap();
    assert_eq!(result.len(), 2); // 10, 20
}

#[test]
fn index_with_no_matching_results() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    db.execute("CREATE INDEX idx_t_name ON t (name)").unwrap();

    let result = db.query("SELECT * FROM t WHERE name = 'Charlie'").unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn multiple_indexes_on_same_table() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)").unwrap();
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'Eng', 100)").unwrap();
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'Sales', 80)").unwrap();
    db.execute("INSERT INTO emp VALUES (3, 'Charlie', 'Eng', 120)").unwrap();
    db.execute("INSERT INTO emp VALUES (4, 'Diana', 'Sales', 90)").unwrap();

    db.execute("CREATE INDEX idx_emp_dept ON emp (dept)").unwrap();
    db.execute("CREATE INDEX idx_emp_name ON emp (name)").unwrap();

    // Query using dept index
    let result = db.query("SELECT * FROM emp WHERE dept = 'Eng'").unwrap();
    assert_eq!(result.len(), 2);

    // Query using name index
    let result = db.query("SELECT * FROM emp WHERE name = 'Bob'").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("salary"), Some(&Value::Integer(80)));
}

#[test]
fn create_index_on_nonexistent_column_fails() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // Should fail because 'nonexistent' column does not exist
    assert!(db.execute("CREATE INDEX idx_bad ON t (nonexistent)").is_err());
}

#[test]
fn unique_index_creation() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'ABC')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'DEF')").unwrap();

    // Creating a unique index should succeed
    db.execute("CREATE UNIQUE INDEX idx_t_code ON t (code)").unwrap();

    // Querying should still work
    let result = db.query("SELECT * FROM t WHERE code = 'ABC'").unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn select_without_where_unaffected_by_index() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

    db.execute("CREATE INDEX idx_t_val ON t (val)").unwrap();

    // SELECT * without WHERE should still return all rows
    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 3);
}

// ---- Transaction Tests (BEGIN / COMMIT / ROLLBACK) ----

#[test]
fn begin_insert_commit_persists_data() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();
    db.execute("COMMIT").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("alpha".to_string())));
    assert_eq!(result.rows[1].get("val"), Some(&Value::Text("beta".to_string())));
}

#[test]
fn begin_insert_rollback_removes_data() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // Insert one row outside transaction to have baseline data
    db.execute("INSERT INTO t VALUES (1, 'existing')").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'will_vanish')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'also_gone')").unwrap();
    db.execute("ROLLBACK").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("existing".to_string())));
}

#[test]
fn rollback_undoes_update() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET val = 'changed' WHERE id = 1").unwrap();

    // Verify the update took effect within the transaction
    let result = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("changed".to_string())));

    db.execute("ROLLBACK").unwrap();

    // After rollback, the original value should be restored
    let result = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("original".to_string())));
}

#[test]
fn rollback_undoes_delete() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'keep_me')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'keep_me_too')").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Verify the delete took effect within the transaction
    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);

    db.execute("ROLLBACK").unwrap();

    // After rollback, the deleted row should be back
    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn commit_without_begin_fails() {
    let (_dir, db) = open_db();
    let err = db.execute("COMMIT").unwrap_err();
    assert!(matches!(err, HorizonError::TransactionError(_)));
}

#[test]
fn rollback_without_begin_fails() {
    let (_dir, db) = open_db();
    let err = db.execute("ROLLBACK").unwrap_err();
    assert!(matches!(err, HorizonError::TransactionError(_)));
}

#[test]
fn double_begin_fails() {
    let (_dir, db) = open_db();
    db.execute("BEGIN").unwrap();
    let err = db.execute("BEGIN").unwrap_err();
    assert!(matches!(err, HorizonError::TransactionError(_)));
}

#[test]
fn begin_transaction_keyword() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    // BEGIN TRANSACTION is valid syntax
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn rollback_mixed_operations() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    db.execute("UPDATE t SET val = 99 WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("ROLLBACK").unwrap();

    // All changes should be undone
    let result = db.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Integer(10)));
    assert_eq!(result.rows[1].get("val"), Some(&Value::Integer(20)));
    assert_eq!(result.rows[2].get("val"), Some(&Value::Integer(30)));
}

// ---- INSERT OR REPLACE Tests ----

#[test]
fn insert_or_replace_new_row() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // INSERT OR REPLACE on a non-existing key should insert normally
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'hello')").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("hello".to_string())));
}

#[test]
fn insert_or_replace_existing_row() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

    // INSERT OR REPLACE with same PK should replace the row
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'replaced')").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("replaced".to_string())));
}

#[test]
fn replace_into_syntax() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

    // REPLACE INTO is equivalent to INSERT OR REPLACE
    db.execute("REPLACE INTO t VALUES (1, 'new')").unwrap();

    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("new".to_string())));
}

#[test]
fn insert_or_replace_multiple_rows() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

    // Replace rows 1 and 3, insert row 4
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'A')").unwrap();
    db.execute("INSERT OR REPLACE INTO t VALUES (3, 'C')").unwrap();
    db.execute("INSERT OR REPLACE INTO t VALUES (4, 'D')").unwrap();

    let result = db.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Text("A".to_string())));
    assert_eq!(result.rows[1].get("val"), Some(&Value::Text("b".to_string())));
    assert_eq!(result.rows[2].get("val"), Some(&Value::Text("C".to_string())));
    assert_eq!(result.rows[3].get("val"), Some(&Value::Text("D".to_string())));
}

#[test]
fn insert_duplicate_pk_without_replace_fails() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();

    // Plain INSERT with duplicate PK should fail
    let err = db.execute("INSERT INTO t VALUES (1, 'second')");
    assert!(err.is_err());
}

// ---- BETWEEN Expression Tests ----

#[test]
fn between_expression_inclusive() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 15)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 18)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 25)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 65)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 70)").unwrap();

    // BETWEEN is inclusive on both ends
    let result = db.query("SELECT * FROM t WHERE age BETWEEN 18 AND 65").unwrap();
    assert_eq!(result.len(), 3);

    let ids: Vec<i64> = result.rows.iter()
        .map(|r| r.get("id").unwrap().as_integer().unwrap())
        .collect();
    assert!(ids.contains(&2)); // age 18
    assert!(ids.contains(&3)); // age 25
    assert!(ids.contains(&4)); // age 65
}

#[test]
fn not_between_expression() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 15)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 25)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 70)").unwrap();

    let result = db.query("SELECT * FROM t WHERE age NOT BETWEEN 18 AND 65").unwrap();
    assert_eq!(result.len(), 2);

    let ids: Vec<i64> = result.rows.iter()
        .map(|r| r.get("id").unwrap().as_integer().unwrap())
        .collect();
    assert!(ids.contains(&1)); // age 15
    assert!(ids.contains(&3)); // age 70
}

#[test]
fn between_with_real_values() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0.50)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 1.00)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 2.50)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 5.00)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 10.00)").unwrap();

    let result = db.query("SELECT * FROM t WHERE price BETWEEN 1.0 AND 5.0").unwrap();
    assert_eq!(result.len(), 3); // 1.00, 2.50, 5.00
}

// ---- IN (value_list) Expression Tests ----

#[test]
fn in_list_integers() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 50)").unwrap();

    let result = db.query("SELECT * FROM t WHERE val IN (10, 30, 50)").unwrap();
    assert_eq!(result.len(), 3);

    let ids: Vec<i64> = result.rows.iter()
        .map(|r| r.get("id").unwrap().as_integer().unwrap())
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&5));
}

#[test]
fn in_list_strings() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'Diana')").unwrap();

    let result = db.query("SELECT * FROM t WHERE name IN ('Alice', 'Charlie')").unwrap();
    assert_eq!(result.len(), 2);

    let ids: Vec<i64> = result.rows.iter()
        .map(|r| r.get("id").unwrap().as_integer().unwrap())
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
}

#[test]
fn not_in_list() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.query("SELECT * FROM t WHERE val NOT IN (10, 30)").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("val"), Some(&Value::Integer(20)));
}

#[test]
fn in_list_no_matches() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    let result = db.query("SELECT * FROM t WHERE val IN (99, 100)").unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn in_list_single_value() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    let result = db.query("SELECT * FROM t WHERE val IN (10)").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&Value::Integer(1)));
}

// ---- JOIN Tests ----

#[test]
fn inner_join_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)").unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 1, 'Gadget')").unwrap();
    db.execute("INSERT INTO orders VALUES (3, 2, 'Thingamajig')").unwrap();
    // Charlie has no orders

    let result = db.query(
        "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    assert_eq!(result.len(), 3);

    // Collect the name-product pairs
    let mut pairs: Vec<(String, String)> = result.rows.iter().map(|r| {
        let name = r.values.iter().find(|v| matches!(v, Value::Text(s) if s == "Alice" || s == "Bob" || s == "Charlie"))
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .unwrap_or_default();
        let product = r.values.iter().find(|v| matches!(v, Value::Text(s) if s == "Widget" || s == "Gadget" || s == "Thingamajig"))
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .unwrap_or_default();
        (name, product)
    }).collect();
    pairs.sort();
    assert_eq!(pairs, vec![
        ("Alice".to_string(), "Gadget".to_string()),
        ("Alice".to_string(), "Widget".to_string()),
        ("Bob".to_string(), "Thingamajig".to_string()),
    ]);
}

#[test]
fn inner_join_no_matches() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, info TEXT)").unwrap();

    db.execute("INSERT INTO a VALUES (1, 'x')").unwrap();
    db.execute("INSERT INTO b VALUES (1, 99, 'no match')").unwrap();

    let result = db.query(
        "SELECT * FROM a INNER JOIN b ON a.id = b.a_id"
    ).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn left_join_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)").unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 2, 'Gadget')").unwrap();
    // Charlie has no orders

    let result = db.query(
        "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    // Alice(Widget), Bob(Gadget), Charlie(NULL)
    assert_eq!(result.len(), 3);

    // Find Charlie's row -- product should be NULL
    let charlie_row = result.rows.iter().find(|r| {
        r.values.iter().any(|v| matches!(v, Value::Text(s) if s == "Charlie"))
    }).expect("Charlie should appear in LEFT JOIN");

    // The product column for Charlie should be NULL
    let product_val = charlie_row.values.iter().find(|v| {
        matches!(v, Value::Null) || matches!(v, Value::Text(s) if s == "Widget" || s == "Gadget")
    });
    assert_eq!(product_val, Some(&Value::Null));
}

#[test]
fn left_join_all_match() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, info TEXT)").unwrap();

    db.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t1 VALUES (2, 'b')").unwrap();

    db.execute("INSERT INTO t2 VALUES (1, 1, 'info1')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2, 2, 'info2')").unwrap();

    let result = db.query(
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id"
    ).unwrap();
    // All rows match, so should be same as inner join
    assert_eq!(result.len(), 2);
}

#[test]
fn cross_join_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE colors (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE sizes (id INTEGER PRIMARY KEY, label TEXT)").unwrap();

    db.execute("INSERT INTO colors VALUES (1, 'Red')").unwrap();
    db.execute("INSERT INTO colors VALUES (2, 'Blue')").unwrap();

    db.execute("INSERT INTO sizes VALUES (1, 'S')").unwrap();
    db.execute("INSERT INTO sizes VALUES (2, 'M')").unwrap();
    db.execute("INSERT INTO sizes VALUES (3, 'L')").unwrap();

    let result = db.query(
        "SELECT colors.name, sizes.label FROM colors CROSS JOIN sizes"
    ).unwrap();
    // Cartesian product: 2 * 3 = 6
    assert_eq!(result.len(), 6);
}

#[test]
fn inner_join_with_where_clause() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)").unwrap();

    db.execute("INSERT INTO dept VALUES (1, 'Engineering')").unwrap();
    db.execute("INSERT INTO dept VALUES (2, 'Sales')").unwrap();

    db.execute("INSERT INTO emp VALUES (1, 'Alice', 1, 100)").unwrap();
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 2, 80)").unwrap();
    db.execute("INSERT INTO emp VALUES (3, 'Charlie', 1, 120)").unwrap();
    db.execute("INSERT INTO emp VALUES (4, 'Diana', 2, 90)").unwrap();

    let result = db.query(
        "SELECT emp.name, dept.name FROM emp INNER JOIN dept ON emp.dept_id = dept.id WHERE emp.salary > 90"
    ).unwrap();
    // Alice(100) and Charlie(120) are in Engineering with salary > 90
    assert_eq!(result.len(), 2);
}

// ---- Aggregate Tests ----

#[test]
fn count_star_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

    let result = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(3));
}

#[test]
fn count_star_empty_table() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    let result = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(0));
}

#[test]
fn sum_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.query("SELECT SUM(val) FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(60));
}

#[test]
fn avg_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.query("SELECT AVG(val) FROM t").unwrap();
    assert_eq!(result.len(), 1);
    // AVG(10, 20, 30) = 20.0
    assert_eq!(result.rows[0].values[0], Value::Real(20.0));
}

#[test]
fn min_max_basic() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 50)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 90)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 30)").unwrap();

    let result = db.query("SELECT MIN(val), MAX(val) FROM t").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(10));
    assert_eq!(result.rows[0].values[1], Value::Integer(90));
}

#[test]
fn group_by_count() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 'A', 100)").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 'B', 200)").unwrap();
    db.execute("INSERT INTO orders VALUES (3, 'A', 150)").unwrap();
    db.execute("INSERT INTO orders VALUES (4, 'B', 300)").unwrap();
    db.execute("INSERT INTO orders VALUES (5, 'A', 50)").unwrap();

    let result = db.query(
        "SELECT category, COUNT(*) FROM orders GROUP BY category"
    ).unwrap();
    assert_eq!(result.len(), 2);

    // Find group A and B
    let mut groups: Vec<(String, i64)> = result.rows.iter().map(|r| {
        let cat = r.values[0].as_text().unwrap().to_string();
        let count = r.values[1].as_integer().unwrap();
        (cat, count)
    }).collect();
    groups.sort();
    assert_eq!(groups, vec![
        ("A".to_string(), 3),
        ("B".to_string(), 2),
    ]);
}

#[test]
fn group_by_sum() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, revenue INTEGER)").unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'North', 100)").unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'South', 200)").unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'North', 300)").unwrap();
    db.execute("INSERT INTO sales VALUES (4, 'South', 400)").unwrap();

    let result = db.query(
        "SELECT region, SUM(revenue) FROM sales GROUP BY region"
    ).unwrap();
    assert_eq!(result.len(), 2);

    let mut groups: Vec<(String, i64)> = result.rows.iter().map(|r| {
        let region = r.values[0].as_text().unwrap().to_string();
        let sum = r.values[1].as_integer().unwrap();
        (region, sum)
    }).collect();
    groups.sort();
    assert_eq!(groups, vec![
        ("North".to_string(), 400),
        ("South".to_string(), 600),
    ]);
}

#[test]
fn group_by_with_having() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 'A', 100)").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 'B', 200)").unwrap();
    db.execute("INSERT INTO orders VALUES (3, 'A', 150)").unwrap();
    db.execute("INSERT INTO orders VALUES (4, 'B', 300)").unwrap();
    db.execute("INSERT INTO orders VALUES (5, 'C', 10)").unwrap();

    let result = db.query(
        "SELECT category, SUM(amount) FROM orders GROUP BY category HAVING SUM(amount) > 200"
    ).unwrap();
    // A: sum=250, B: sum=500, C: sum=10 -> only A and B pass HAVING
    assert_eq!(result.len(), 2);

    let mut groups: Vec<(String, i64)> = result.rows.iter().map(|r| {
        let cat = r.values[0].as_text().unwrap().to_string();
        let sum = r.values[1].as_integer().unwrap();
        (cat, sum)
    }).collect();
    groups.sort();
    assert_eq!(groups, vec![
        ("A".to_string(), 250),
        ("B".to_string(), 500),
    ]);
}

#[test]
fn group_by_having_filters_all() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'X', 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Y', 2)").unwrap();

    let result = db.query(
        "SELECT cat, SUM(val) FROM t GROUP BY cat HAVING SUM(val) > 100"
    ).unwrap();
    // Neither group passes HAVING
    assert_eq!(result.len(), 0);
}

#[test]
fn multiple_aggregates_without_group_by() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, score INTEGER)").unwrap();
    db.execute("INSERT INTO scores VALUES (1, 85)").unwrap();
    db.execute("INSERT INTO scores VALUES (2, 92)").unwrap();
    db.execute("INSERT INTO scores VALUES (3, 78)").unwrap();
    db.execute("INSERT INTO scores VALUES (4, 95)").unwrap();
    db.execute("INSERT INTO scores VALUES (5, 88)").unwrap();

    let result = db.query(
        "SELECT COUNT(*), SUM(score), MIN(score), MAX(score) FROM scores"
    ).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(5));    // COUNT
    assert_eq!(result.rows[0].values[1], Value::Integer(438));   // SUM
    assert_eq!(result.rows[0].values[2], Value::Integer(78));    // MIN
    assert_eq!(result.rows[0].values[3], Value::Integer(95));    // MAX
}

#[test]
fn group_by_multiple_columns() {
    let (_dir, db) = open_db();
    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, product TEXT, amount INTEGER)").unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'East', 'Widget', 100)").unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'East', 'Widget', 200)").unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'East', 'Gadget', 50)").unwrap();
    db.execute("INSERT INTO sales VALUES (4, 'West', 'Widget', 300)").unwrap();

    let result = db.query(
        "SELECT region, product, SUM(amount) FROM sales GROUP BY region, product"
    ).unwrap();
    assert_eq!(result.len(), 3); // (East, Widget), (East, Gadget), (West, Widget)

    let mut groups: Vec<(String, String, i64)> = result.rows.iter().map(|r| {
        let region = r.values[0].as_text().unwrap().to_string();
        let product = r.values[1].as_text().unwrap().to_string();
        let sum = r.values[2].as_integer().unwrap();
        (region, product, sum)
    }).collect();
    groups.sort();
    assert_eq!(groups, vec![
        ("East".to_string(), "Gadget".to_string(), 50),
        ("East".to_string(), "Widget".to_string(), 300),
        ("West".to_string(), "Widget".to_string(), 300),
    ]);
}
