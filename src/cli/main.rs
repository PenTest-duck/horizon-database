//! # Horizon DB CLI
//!
//! An interactive REPL for Horizon DB, similar to the `sqlite3` command-line shell.

use std::env;
use std::io::{self, BufRead, Write};

use horizon::Database;

fn main() {
    let args: Vec<String> = env::args().collect();

    let db_path = if args.len() > 1 {
        args[1].clone()
    } else {
        ":memory:".to_string() // TODO: implement in-memory mode
    };

    println!("Horizon DB v{}", env!("CARGO_PKG_VERSION"));
    println!("Enter \".help\" for usage hints.");
    println!("Connected to {}", db_path);

    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            std::process::exit(1);
        }
    };

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut sql_buffer = String::new();

    loop {
        // Print prompt
        let prompt = if sql_buffer.is_empty() {
            "horizon> "
        } else {
            "   ...> "
        };
        print!("{}", prompt);
        if stdout.flush().is_err() {
            break;
        }

        // Read a line
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(_) => break,
        }

        let trimmed = line.trim();

        // Handle empty lines
        if trimmed.is_empty() {
            continue;
        }

        // Handle dot-commands
        if sql_buffer.is_empty() && trimmed.starts_with('.') {
            handle_dot_command(trimmed, &db);
            continue;
        }

        // Accumulate SQL
        sql_buffer.push_str(&line);

        // Check if the statement is complete (ends with ;)
        let trimmed_buf = sql_buffer.trim();
        if !trimmed_buf.ends_with(';') {
            continue;
        }

        // Execute the SQL
        let sql = sql_buffer.trim().to_string();
        sql_buffer.clear();

        execute_sql(&db, &sql);
    }

    println!();
    if let Err(e) = db.close() {
        eprintln!("Error closing database: {}", e);
    }
}

fn execute_sql(db: &Database, sql: &str) {
    // Determine if it's a query (SELECT) or a statement
    let upper = sql.trim().to_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("EXPLAIN") || upper.starts_with("PRAGMA") || upper.starts_with("WITH") {
        match db.query(sql) {
            Ok(result) => {
                if result.is_empty() {
                    return;
                }

                // Print column headers
                let headers: Vec<&str> = result.columns.iter().map(|s| s.as_str()).collect();
                println!("{}", headers.join("|"));

                // Print rows
                for row in result {
                    let vals: Vec<String> = row.values.iter().map(|v| format!("{}", v)).collect();
                    println!("{}", vals.join("|"));
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    } else {
        match db.execute(sql) {
            Ok(affected) => {
                if affected > 0 {
                    println!("({} rows affected)", affected);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
}

fn handle_dot_command(cmd: &str, db: &Database) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let command = parts[0].to_lowercase();

    match command.as_str() {
        ".help" => {
            println!(".help              Show this help");
            println!(".tables            List all tables");
            println!(".schema [TABLE]    Show CREATE statements");
            println!(".quit              Exit this program");
            println!(".exit              Exit this program");
            println!(".dump              Dump database as SQL");
            println!(".mode MODE         Set output mode (column, csv, list)");
            println!(".headers on|off    Turn column headers on or off");
        }
        ".tables" => {
            match db.query("SELECT name FROM __schema_tables") {
                Ok(result) => {
                    let names: Vec<String> = result
                        .rows
                        .iter()
                        .filter_map(|r| r.values.first().and_then(|v| v.as_text().map(|s| s.to_string())))
                        .collect();
                    if !names.is_empty() {
                        println!("{}", names.join("  "));
                    }
                }
                Err(_) => {
                    // Fall back: list tables from the catalog directly
                    // This will be implemented properly later
                    println!("(no tables)");
                }
            }
        }
        ".schema" => {
            println!("(schema display not yet implemented)");
        }
        ".quit" | ".exit" => {
            std::process::exit(0);
        }
        ".dump" => {
            println!("(dump not yet implemented)");
        }
        _ => {
            eprintln!("Error: unknown command: {}", command);
            eprintln!("Use .help for a list of commands.");
        }
    }
}
