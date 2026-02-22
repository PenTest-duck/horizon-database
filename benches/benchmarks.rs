use criterion::{criterion_group, criterion_main, Criterion};
use horizon::Database;
use tempfile::TempDir;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert_1000_rows", |b| {
        b.iter(|| {
            let dir = TempDir::new().unwrap();
            let db = Database::open(dir.path().join("bench.hdb")).unwrap();
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)").unwrap();
            for i in 0..1000 {
                db.execute(&format!(
                    "INSERT INTO t VALUES ({}, 'name_{}', {}.5)",
                    i, i, i
                )).unwrap();
            }
        });
    });
}

fn bench_select_all(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path().join("bench.hdb")).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL)").unwrap();
    for i in 0..1000 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'name_{}', {}.5)",
            i, i, i
        )).unwrap();
    }

    c.bench_function("select_all_1000_rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM t").unwrap();
            assert_eq!(result.len(), 1000);
        });
    });
}

criterion_group!(benches, bench_insert, bench_select_all);
criterion_main!(benches);
