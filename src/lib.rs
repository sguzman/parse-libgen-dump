use log::{error, info, debug, warn};
use rayon::prelude::*;
use sqlparser::ast::{Statement, SetExpr};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub fn process_sql_file(input_file: &str, output_dir: &str) -> std::io::Result<()> {
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    info!("Total lines in file: {}", lines.len());

    // First pass: extract CREATE TABLE statements and column names
    let (tables, create_statements, table_columns) = extract_create_tables(&lines);
    
    info!("Found {} tables", tables.len());
    for table in &tables {
        info!("Table found: {}", table);
    }

    // Write CREATE TABLE statements to a file
    write_create_tables(&create_statements, output_dir)?;

    // Create CSV files for each table with headers
    for (table, columns) in &table_columns {
        let csv_path = Path::new(output_dir).join(format!("{}.csv", table));
        let mut writer = csv::Writer::from_path(&csv_path)?;
        let headers: Vec<String> = columns.iter().map(|c| unquote_column_name(c)).collect();
        info!("Inserting headers for table {}: {:?}", table, headers);
        writer.write_record(&headers)?;
        writer.flush()?;
        info!("Created CSV file with headers: {:?}", csv_path);
    }

    // Second pass: process INSERT statements in parallel
    let tables = Arc::new(tables);
    let output_dir = Arc::new(output_dir.to_string());
    let insert_count = Arc::new(Mutex::new(0));
    let error_count = Arc::new(Mutex::new(0));

    lines.par_iter().for_each(|line| {
        if line.trim_start().to_lowercase().starts_with("insert into") {
            match parse_insert(line) {
                Ok((table_name, value_rows)) => {
                    let unquoted_table_name = unquote_table_name(&table_name);
                    if tables.contains(&unquoted_table_name) {
                        let csv_path = Path::new(&*output_dir).join(format!("{}.csv", unquoted_table_name));
                        for values in value_rows {
                            match append_to_csv(&csv_path, &values) {
                                Ok(_) => {
                                    let mut count = insert_count.lock().unwrap();
                                    *count += 1;
                                    if *count % 1000 == 0 {
                                        info!("Processed {} INSERT rows", *count);
                                    }
                                },
                                Err(e) => {
                                    error!("Failed to write to CSV for table {}: {}", unquoted_table_name, e);
                                    let mut count = error_count.lock().unwrap();
                                    *count += 1;
                                }
                            }
                        }
                    } else {
                        warn!("Table {} not found in CREATE TABLE statements", unquoted_table_name);
                    }
                },
                Err(e) => {
                    error!("Failed to parse INSERT statement: {}", e);
                    let mut count = error_count.lock().unwrap();
                    *count += 1;
                }
            }
        }
    });

    info!("Processing complete. Check the output directory for results.");
    info!("Total INSERT rows processed: {}", *insert_count.lock().unwrap());
    info!("Total errors encountered: {}", *error_count.lock().unwrap());

    // Check file sizes after processing
    for table in &*tables {
        let csv_path = Path::new(&*output_dir).join(format!("{}.csv", table));
        match std::fs::metadata(&csv_path) {
            Ok(metadata) => info!("File size for {}.csv: {} bytes", table, metadata.len()),
            Err(e) => error!("Failed to get metadata for {}.csv: {}", table, e),
        }
    }

    Ok(())
}

fn parse_insert(sql: &str) -> Result<(String, Vec<Vec<String>>), String> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    if let Statement::Insert { table_name, source, .. } = &ast[0] {
        let table_name = table_name.to_string();
        if let SetExpr::Values(values) = source.body.as_ref() {
            let parsed_values: Vec<Vec<String>> = values.rows.iter().map(|row| {
                row.iter()
                    .map(|expr| match expr {
                        sqlparser::ast::Expr::Value(val) => match val {
                            sqlparser::ast::Value::Number(n, _) => n.to_string(),
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Null => "NULL".to_string(),
                            _ => format!("{:?}", val),
                        },
                        _ => format!("{:?}", expr),
                    })
                    .collect()
            }).collect();
            debug!("Parsed INSERT for table {} with {} rows", table_name, parsed_values.len());
            return Ok((table_name, parsed_values));
        }
    }
    Err(format!("Failed to parse INSERT statement: {}", sql))
}

fn extract_create_tables(lines: &[String]) -> (HashSet<String>, Vec<String>, HashMap<String, Vec<String>>) {
    let mut tables = HashSet::new();
    let mut create_statements = Vec::new();
    let mut table_columns = HashMap::new();
    let mut current_statement = Vec::new();
    let mut in_create_table = false;
    let mut current_table = String::new();

    for line in lines {
        let trimmed = line.trim();
        if trimmed.to_lowercase().starts_with("create table") {
            in_create_table = true;
            current_statement.clear();
            if let Some(table_name) = extract_table_name(trimmed) {
                current_table = unquote_table_name(&table_name);
                tables.insert(current_table.clone());
            }
        }

        if in_create_table {
            current_statement.push(line.clone());

            if trimmed.ends_with(';') {
                in_create_table = false;
                let full_statement = current_statement.join("\n");
                create_statements.push(full_statement.clone());
                if let Some(columns) = extract_column_names(&full_statement) {
                    table_columns.insert(current_table.clone(), columns);
                }
                current_statement.clear();
            }
        }
    }

    (tables, create_statements, table_columns)
}

fn write_create_tables(create_statements: &[String], output_dir: &str) -> std::io::Result<()> {
    let path = Path::new(output_dir).join("create_tables.sql");
    let mut file = File::create(path)?;
    for statement in create_statements {
        writeln!(file, "{}", statement)?;
    }
    Ok(())
}

fn extract_table_name(create_statement: &str) -> Option<String> {
    let parts: Vec<&str> = create_statement.split_whitespace().collect();
    if parts.len() >= 3 {
        Some(parts[2].to_string())
    } else {
        None
    }
}

fn unquote_table_name(table_name: &str) -> String {
    table_name.trim_matches('`').to_string()
}

fn unquote_column_name(column_name: &str) -> String {
    column_name.trim_matches('`').to_string()
}

fn extract_column_names(create_statement: &str) -> Option<Vec<String>> {
    let dialect = MySqlDialect {};
    match Parser::parse_sql(&dialect, create_statement) {
        Ok(ast) => {
            if let Statement::CreateTable { columns, .. } = &ast[0] {
                Some(columns.iter().map(|c| c.name.value.clone()).collect())
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

fn append_to_csv(path: &Path, values: &[String]) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;
    let mut writer = csv::Writer::from_writer(file);
    writer.write_record(values)?;
    writer.flush()?;
    Ok(())
}