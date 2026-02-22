use horizon::{Database, Value};
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
