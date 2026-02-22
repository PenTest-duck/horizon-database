//! # Horizon DB CLI
//!
//! An interactive REPL for Horizon DB, similar to the `sqlite3` command-line shell.

use std::env;
use std::io::{self, BufRead, Write};
use std::time::Instant;

use horizon::Database;

/// Output formatting mode.
#[derive(Clone, Copy, PartialEq)]
enum OutputMode {
    Column,
    Csv,
    List,
    Table,
    Json,
    Line,
}

/// REPL configuration.
struct Config {
    mode: OutputMode,
    headers: bool,
    separator: String,
    null_display: String,
    timer: bool,
    width: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: OutputMode::List,
            headers: true,
            separator: "|".to_string(),
            null_display: String::new(),
            timer: false,
            width: 12,
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut db_path = String::new();
    let mut init_sql: Vec<String> = Vec::new();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "-cmd" => {
                if i + 1 < args.len() {
                    init_sql.push(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: -cmd requires an argument");
                    std::process::exit(1);
                }
            }
            "-csv" => {
                // Will be handled after config creation
                init_sql.push(".mode csv".into());
                i += 1;
            }
            "-column" => {
                init_sql.push(".mode column".into());
                i += 1;
            }
            "-json" => {
                init_sql.push(".mode json".into());
                i += 1;
            }
            "-line" => {
                init_sql.push(".mode line".into());
                i += 1;
            }
            "-header" | "-headers" => {
                init_sql.push(".headers on".into());
                i += 1;
            }
            "-noheader" | "-noheaders" => {
                init_sql.push(".headers off".into());
                i += 1;
            }
            "-version" => {
                println!("Horizon DB v{}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "-help" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            arg if arg.starts_with('-') => {
                eprintln!("Error: unknown option: {}", arg);
                std::process::exit(1);
            }
            _ => {
                if db_path.is_empty() {
                    db_path = args[i].clone();
                } else {
                    // Additional args are SQL to execute
                    init_sql.push(args[i].clone());
                }
                i += 1;
            }
        }
    }

    if db_path.is_empty() {
        db_path = ":memory:".to_string();
    }

    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            std::process::exit(1);
        }
    };

    let mut config = Config::default();

    // If SQL was provided on command line, execute and exit (non-interactive mode)
    let _has_init_sql = init_sql.iter().any(|s| !s.starts_with('.') || s.starts_with(".mode") || s.starts_with(".headers"));
    let batch_mode = init_sql.iter().any(|s| !s.starts_with('.'));

    for sql in &init_sql {
        let trimmed = sql.trim();
        if trimmed.starts_with('.') {
            handle_dot_command(trimmed, &db, &mut config);
        } else {
            execute_sql(&db, trimmed, &config);
        }
    }

    if batch_mode {
        // Check if stdin is a pipe with more SQL
        if atty_is_not_tty() {
            run_pipe_mode(&db, &mut config);
        }
        if let Err(e) = db.close() {
            eprintln!("Error closing database: {}", e);
        }
        return;
    }

    // Interactive mode
    let is_tty = !atty_is_not_tty();
    if is_tty {
        println!("Horizon DB v{}", env!("CARGO_PKG_VERSION"));
        println!("Enter \".help\" for usage hints.");
        if db_path == ":memory:" {
            println!("Connected to a transient in-memory database.");
        } else {
            println!("Connected to {}", db_path);
        }
    }

    if is_tty {
        run_interactive(&db, &mut config);
    } else {
        run_pipe_mode(&db, &mut config);
    }

    if let Err(e) = db.close() {
        eprintln!("Error closing database: {}", e);
    }
}

fn run_interactive(db: &Database, config: &mut Config) {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut sql_buffer = String::new();

    loop {
        let prompt = if sql_buffer.is_empty() {
            "horizon> "
        } else {
            "   ...> "
        };
        print!("{}", prompt);
        if stdout.flush().is_err() {
            break;
        }

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if sql_buffer.is_empty() && trimmed.starts_with('.') {
            handle_dot_command(trimmed, db, config);
            continue;
        }

        sql_buffer.push_str(&line);

        let trimmed_buf = sql_buffer.trim();
        if !trimmed_buf.ends_with(';') {
            continue;
        }

        let sql = sql_buffer.trim().to_string();
        sql_buffer.clear();
        execute_sql(db, &sql, config);
    }
    println!();
}

fn run_pipe_mode(db: &Database, config: &mut Config) {
    let stdin = io::stdin();
    let mut sql_buffer = String::new();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if sql_buffer.is_empty() && trimmed.starts_with('.') {
            handle_dot_command(trimmed, db, config);
            continue;
        }
        sql_buffer.push_str(trimmed);
        sql_buffer.push(' ');

        if sql_buffer.trim().ends_with(';') {
            let sql = sql_buffer.trim().to_string();
            sql_buffer.clear();
            execute_sql(db, &sql, config);
        }
    }

    if !sql_buffer.trim().is_empty() {
        execute_sql(db, sql_buffer.trim(), config);
    }
}

fn execute_sql(db: &Database, sql: &str, config: &Config) {
    let start = if config.timer {
        Some(Instant::now())
    } else {
        None
    };

    let upper = sql.trim_start().to_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("PRAGMA")
        || upper.starts_with("WITH")
    {
        match db.query(sql) {
            Ok(result) => {
                if result.is_empty() && result.columns.is_empty() {
                    // No results, no columns
                } else {
                    print_result(&result, config);
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    } else {
        match db.execute(sql) {
            Ok(affected) => {
                if affected > 0 {
                    println!("({} rows affected)", affected);
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    if let Some(start) = start {
        let elapsed = start.elapsed();
        eprintln!("Run Time: real {:.3}", elapsed.as_secs_f64());
    }
}

fn print_result(result: &horizon::QueryResult, config: &Config) {
    if result.is_empty() && !config.headers {
        return;
    }

    let col_names: Vec<&str> = result.columns.iter().map(|s| s.as_str()).collect();
    let ncols = col_names.len();

    // Format all cell values
    let rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| {
                    if v.is_null() {
                        config.null_display.clone()
                    } else {
                        format!("{}", v)
                    }
                })
                .collect()
        })
        .collect();

    match config.mode {
        OutputMode::List => {
            if config.headers {
                println!("{}", col_names.join(&config.separator));
            }
            for row in &rows {
                println!("{}", row.join(&config.separator));
            }
        }

        OutputMode::Csv => {
            if config.headers {
                println!("{}", col_names.iter().map(|n| csv_escape(n)).collect::<Vec<_>>().join(","));
            }
            for row in &rows {
                println!(
                    "{}",
                    row.iter().map(|v| csv_escape(v)).collect::<Vec<_>>().join(",")
                );
            }
        }

        OutputMode::Column | OutputMode::Table => {
            // Compute column widths
            let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
            for row in &rows {
                for (i, val) in row.iter().enumerate() {
                    if i < widths.len() {
                        widths[i] = widths[i].max(val.len());
                    }
                }
            }
            // Cap at reasonable max
            for w in &mut widths {
                *w = (*w).min(60).max(config.width);
            }

            if config.mode == OutputMode::Table {
                // Table mode with box drawing
                let border: String = widths
                    .iter()
                    .map(|w| "-".repeat(*w + 2))
                    .collect::<Vec<_>>()
                    .join("+");
                println!("+{}+", border);

                if config.headers {
                    let header: String = col_names
                        .iter()
                        .enumerate()
                        .map(|(i, n)| format!(" {:width$} ", n, width = widths[i]))
                        .collect::<Vec<_>>()
                        .join("|");
                    println!("|{}|", header);
                    println!("+{}+", border);
                }

                for row in &rows {
                    let line: String = row
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let w = if i < widths.len() { widths[i] } else { 12 };
                            format!(" {:width$} ", v, width = w)
                        })
                        .collect::<Vec<_>>()
                        .join("|");
                    println!("|{}|", line);
                }
                println!("+{}+", border);
            } else {
                // Column mode (aligned, no borders)
                if config.headers {
                    let header: String = col_names
                        .iter()
                        .enumerate()
                        .map(|(i, n)| format!("{:width$}", n, width = widths[i] + 2))
                        .collect::<Vec<_>>()
                        .join("");
                    println!("{}", header);
                    let dashes: String = widths
                        .iter()
                        .map(|w| format!("{:->width$}  ", "", width = w))
                        .collect::<Vec<_>>()
                        .join("");
                    println!("{}", dashes);
                }
                for row in &rows {
                    let line: String = row
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let w = if i < widths.len() { widths[i] } else { 12 };
                            format!("{:width$}  ", v, width = w)
                        })
                        .collect::<Vec<_>>()
                        .join("");
                    println!("{}", line);
                }
            }
        }

        OutputMode::Json => {
            println!("[");
            for (ri, row) in rows.iter().enumerate() {
                print!("  {{");
                for (ci, val) in row.iter().enumerate() {
                    if ci > 0 {
                        print!(", ");
                    }
                    let col = if ci < col_names.len() {
                        col_names[ci]
                    } else {
                        "?"
                    };
                    print!("\"{}\": ", json_escape(col));
                    let orig = &result.rows[ri].values[ci];
                    if orig.is_null() {
                        print!("null");
                    } else if let Some(n) = orig.as_integer() {
                        print!("{}", n);
                    } else if let Some(f) = orig.as_real() {
                        print!("{}", f);
                    } else {
                        print!("\"{}\"", json_escape(val));
                    }
                }
                if ri + 1 < rows.len() {
                    println!("}},"  );
                } else {
                    println!("}}");
                }
            }
            println!("]");
        }

        OutputMode::Line => {
            for (ri, row) in rows.iter().enumerate() {
                if ri > 0 {
                    println!();
                }
                let max_col_width = col_names.iter().map(|n| n.len()).max().unwrap_or(0);
                for (ci, val) in row.iter().enumerate() {
                    let col = if ci < ncols { col_names[ci] } else { "?" };
                    println!("{:>width$} = {}", col, val, width = max_col_width);
                }
            }
        }
    }
}

fn handle_dot_command(cmd: &str, db: &Database, config: &mut Config) {
    let parts: Vec<&str> = cmd.splitn(3, char::is_whitespace).collect();
    let command = parts[0].to_lowercase();
    let arg1 = parts.get(1).map(|s| s.trim()).unwrap_or("");

    match command.as_str() {
        ".help" => {
            println!(".dump              Dump database as SQL");
            println!(".exit              Exit this program");
            println!(".headers on|off    Turn column headers on or off");
            println!(".help              Show this help");
            println!(".import FILE TABLE Import CSV data into table");
            println!(".indices [TABLE]   List indexes");
            println!(".mode MODE         Set output mode (list, csv, column, table, json, line)");
            println!(".nullvalue STRING  Set string for NULL values");
            println!(".quit              Exit this program");
            println!(".read FILE         Execute SQL from file");
            println!(".schema [TABLE]    Show CREATE statements");
            println!(".separator STRING  Set column separator for list mode");
            println!(".tables            List all tables");
            println!(".timer on|off      Turn timer on or off");
            println!(".width NUM ...     Set column widths for column mode");
        }

        ".tables" => {
            match db.query("PRAGMA database_list") {
                Ok(_) => {
                    // Use PRAGMA table_info approach - list from catalog
                }
                Err(_) => {}
            }
            // Fallback: try to get tables from catalog
            // The cleanest way is to query all table names
            // We'll use the fact that PRAGMA table_info works per table
            // For now, enumerate via internal schema
            match db.query("SELECT name FROM __schema_tables") {
                Ok(result) => {
                    let names: Vec<String> = result
                        .rows
                        .iter()
                        .filter_map(|r| {
                            r.values
                                .first()
                                .and_then(|v| v.as_text().map(|s| s.to_string()))
                        })
                        .collect();
                    if !names.is_empty() {
                        println!("{}", names.join("  "));
                    }
                }
                Err(_) => {
                    println!("(no tables)");
                }
            }
        }

        ".schema" => {
            if !arg1.is_empty() {
                show_schema_for_table(db, arg1, config);
            } else {
                // Show all tables
                match db.query("SELECT name FROM __schema_tables") {
                    Ok(result) => {
                        for row in &result.rows {
                            if let Some(name) = row.values.first().and_then(|v| v.as_text()) {
                                show_schema_for_table(db, name, config);
                            }
                        }
                    }
                    Err(_) => println!("(no tables)"),
                }
            }
        }

        ".indices" | ".indexes" => {
            if !arg1.is_empty() {
                match db.query(&format!("PRAGMA index_list({})", arg1)) {
                    Ok(result) => {
                        for row in &result.rows {
                            if let Some(name) = row.get("name").and_then(|v| v.as_text()) {
                                println!("{}", name);
                            }
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            } else {
                println!("Usage: .indices TABLE_NAME");
            }
        }

        ".mode" => match arg1.to_lowercase().as_str() {
            "list" => config.mode = OutputMode::List,
            "csv" => {
                config.mode = OutputMode::Csv;
                config.separator = ",".into();
            }
            "column" => config.mode = OutputMode::Column,
            "table" => config.mode = OutputMode::Table,
            "json" => config.mode = OutputMode::Json,
            "line" => config.mode = OutputMode::Line,
            "" => {
                let name = match config.mode {
                    OutputMode::List => "list",
                    OutputMode::Csv => "csv",
                    OutputMode::Column => "column",
                    OutputMode::Table => "table",
                    OutputMode::Json => "json",
                    OutputMode::Line => "line",
                };
                println!("current output mode: {}", name);
            }
            other => eprintln!("Error: unknown mode \"{}\". Use list, csv, column, table, json, or line.", other),
        },

        ".headers" => match arg1.to_lowercase().as_str() {
            "on" | "yes" | "1" => config.headers = true,
            "off" | "no" | "0" => config.headers = false,
            _ => eprintln!("Usage: .headers on|off"),
        },

        ".separator" => {
            if !arg1.is_empty() {
                config.separator = unescape_str(arg1);
            } else {
                println!("\"{}\"", config.separator);
            }
        }

        ".nullvalue" => {
            config.null_display = arg1.to_string();
        }

        ".timer" => match arg1.to_lowercase().as_str() {
            "on" | "yes" | "1" => config.timer = true,
            "off" | "no" | "0" => config.timer = false,
            _ => eprintln!("Usage: .timer on|off"),
        },

        ".width" => {
            if !arg1.is_empty() {
                if let Ok(w) = arg1.parse::<usize>() {
                    config.width = w;
                }
            }
        }

        ".dump" => {
            dump_database(db);
        }

        ".read" => {
            if arg1.is_empty() {
                eprintln!("Usage: .read FILENAME");
            } else {
                read_sql_file(db, arg1, config);
            }
        }

        ".import" => {
            let parts: Vec<&str> = cmd.split_whitespace().collect();
            if parts.len() < 3 {
                eprintln!("Usage: .import FILE TABLE");
            } else {
                import_csv(db, parts[1], parts[2]);
            }
        }

        ".quit" | ".exit" => {
            std::process::exit(0);
        }

        _ => {
            eprintln!("Error: unknown command: {}", command);
            eprintln!("Use .help for a list of commands.");
        }
    }
}

fn show_schema_for_table(db: &Database, table: &str, _config: &Config) {
    match db.query(&format!("PRAGMA table_info({})", table)) {
        Ok(result) => {
            if result.is_empty() {
                eprintln!("Error: no such table: {}", table);
                return;
            }

            print!("CREATE TABLE {} (", table);
            let mut first = true;
            for row in &result.rows {
                if !first {
                    print!(", ");
                }
                first = false;

                let name = row.get("name").and_then(|v| v.as_text()).unwrap_or("?");
                let type_name = row.get("type").and_then(|v| v.as_text()).unwrap_or("");
                let notnull = row.get("notnull").and_then(|v| v.as_integer()).unwrap_or(0);
                let pk = row.get("pk").and_then(|v| v.as_integer()).unwrap_or(0);

                print!("{}", name);
                if !type_name.is_empty() {
                    print!(" {}", type_name);
                }
                if pk == 1 {
                    print!(" PRIMARY KEY");
                }
                if notnull == 1 {
                    print!(" NOT NULL");
                }
            }
            println!(");");
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn dump_database(db: &Database) {
    println!("BEGIN TRANSACTION;");

    match db.query("SELECT name FROM __schema_tables") {
        Ok(tables) => {
            for trow in &tables.rows {
                let table_name = match trow.values.first().and_then(|v| v.as_text()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                // Get schema
                match db.query(&format!("PRAGMA table_info({})", table_name)) {
                    Ok(info) => {
                        if info.is_empty() {
                            continue;
                        }

                        print!("CREATE TABLE {} (", table_name);
                        let mut cols = Vec::new();
                        let mut col_names = Vec::new();
                        for (i, row) in info.rows.iter().enumerate() {
                            let name =
                                row.get("name").and_then(|v| v.as_text()).unwrap_or("?");
                            let type_name =
                                row.get("type").and_then(|v| v.as_text()).unwrap_or("");
                            let notnull =
                                row.get("notnull").and_then(|v| v.as_integer()).unwrap_or(0);
                            let pk = row.get("pk").and_then(|v| v.as_integer()).unwrap_or(0);

                            let mut def = name.to_string();
                            if !type_name.is_empty() {
                                def.push(' ');
                                def.push_str(type_name);
                            }
                            if pk == 1 {
                                def.push_str(" PRIMARY KEY");
                            }
                            if notnull == 1 {
                                def.push_str(" NOT NULL");
                            }
                            if i > 0 {
                                print!(", ");
                            }
                            print!("{}", def);
                            cols.push(def);
                            col_names.push(name.to_string());
                        }
                        println!(");");

                        // Dump data
                        let col_list = col_names.join(", ");
                        match db.query(&format!("SELECT {} FROM {}", col_list, table_name)) {
                            Ok(data) => {
                                for row in &data.rows {
                                    let vals: Vec<String> = row
                                        .values
                                        .iter()
                                        .map(|v| sql_literal(v))
                                        .collect();
                                    println!(
                                        "INSERT INTO {} VALUES({});",
                                        table_name,
                                        vals.join(", ")
                                    );
                                }
                            }
                            Err(e) => eprintln!("-- Error dumping {}: {}", table_name, e),
                        }
                    }
                    Err(e) => eprintln!("-- Error getting schema for {}: {}", table_name, e),
                }
            }
        }
        Err(_) => {}
    }

    println!("COMMIT;");
}

fn read_sql_file(db: &Database, path: &str, config: &Config) {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: cannot read file \"{}\": {}", path, e);
            return;
        }
    };
    for line in contents.split(';') {
        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }
        let full = format!("{};", sql);
        execute_sql(db, &full, config);
    }
}

fn import_csv(db: &Database, file_path: &str, table_name: &str) {
    let contents = match std::fs::read_to_string(file_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: cannot read file \"{}\": {}", file_path, e);
            return;
        }
    };

    let mut lines = contents.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => {
            eprintln!("Error: empty CSV file");
            return;
        }
    };

    let columns: Vec<&str> = header.split(',').map(|s| s.trim().trim_matches('"')).collect();
    let col_list = columns.join(", ");

    let mut count = 0;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let values: Vec<String> = parse_csv_line(line)
            .iter()
            .map(|v| {
                if v.is_empty() {
                    "NULL".to_string()
                } else if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
                    v.to_string()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            })
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            table_name,
            col_list,
            values.join(", ")
        );
        match db.execute(&sql) {
            Ok(_) => count += 1,
            Err(e) => {
                eprintln!("Error on row {}: {}", count + 1, e);
                return;
            }
        }
    }
    println!("({} rows imported)", count);
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next(); // skip escaped quote
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == ',' {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields.iter().map(|f| f.trim().to_string()).collect()
}

// --- Utility functions ---

fn sql_literal(v: &horizon::Value) -> String {
    if v.is_null() {
        "NULL".to_string()
    } else if let Some(n) = v.as_integer() {
        n.to_string()
    } else if let Some(f) = v.as_real() {
        format!("{}", f)
    } else if let Some(s) = v.as_text() {
        format!("'{}'", s.replace('\'', "''"))
    } else if let Some(b) = v.as_blob() {
        format!("X'{}'", hex_encode(b))
    } else {
        "NULL".to_string()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect()
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn unescape_str(s: &str) -> String {
    s.replace("\\t", "\t")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\\\", "\\")
}

fn atty_is_not_tty() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        extern "C" {
            fn isatty(fd: i32) -> i32;
        }
        unsafe { isatty(io::stdin().as_raw_fd()) == 0 }
    }
    #[cfg(not(unix))]
    {
        false // assume interactive on non-unix
    }
}

fn print_usage() {
    println!("Usage: horizon [OPTIONS] [DBFILE] [SQL]");
    println!();
    println!("Options:");
    println!("  -csv              Set output mode to CSV");
    println!("  -column           Set output mode to column");
    println!("  -json             Set output mode to JSON");
    println!("  -line             Set output mode to line");
    println!("  -header           Turn headers on");
    println!("  -noheader         Turn headers off");
    println!("  -cmd COMMAND      Run command before reading stdin");
    println!("  -version          Show version and exit");
    println!("  -help             Show this help");
}
