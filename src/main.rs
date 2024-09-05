use env_logger::Env;
use log::{debug, error, info};
use std::env;

use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader};

use log::{debug, error, info, warn};
use rayon::prelude::*;
use regex::Regex;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub fn process_sql_file(input_file: &str, output_dir: &str) {
    let file = File::open(input_file).unwrap_or_else(|e| {
        error!("Failed to open input file: {}", e);
        std::process::exit(1);
    });
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader
        .lines()
        .collect::<Result<_, _>>()
        .unwrap_or_else(|e| {
            error!("Failed to read lines from input file: {}", e);
            std::process::exit(1);
        });

    info!("Total lines in file: {}", lines.len());

    // First pass: extract CREATE TABLE statements and column names
    let (tables, create_statements, table_columns) = extract_create_tables(&lines);

    info!("Found {} tables", tables.len());
    for table in &tables {
        info!("Table found: {}", table);
    }

    // Write CREATE TABLE statements to a file
    write_create_tables(&create_statements, output_dir);

    // TODO Create CSV files for each table with headers

    // TODO - Second pass: process INSERT statements in parallel
}

fn write_create_tables(create_statements: &[String], output_dir: &str) {
    let path = Path::new(output_dir).join("create_tables.sql");
    let mut file = File::create(&path).unwrap_or_else(|e| {
        error!("Failed to create create_tables.sql: {}", e);
        std::process::exit(1);
    });
    for statement in create_statements {
        writeln!(file, "{}", statement).unwrap_or_else(|e| {
            error!("Failed to write CREATE TABLE statement: {}", e);
            std::process::exit(1);
        });
    }
}

fn parse_insert(sql: &str) -> Result<(String, Vec<Vec<String>>), String> {
    let insert_regex = Regex::new(r"^INSERT\s+INTO\s+`([^`]+)`").unwrap();
    let table_name = match insert_regex.captures(sql) {
        Some(caps) => caps.get(1).unwrap().as_str().to_string(),
        None => return Err("Invalid INSERT statement format".to_string()),
    };

    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    if let Statement::Insert { source, .. } = &ast[0] {
        if let SetExpr::Values(values) = source.body.as_ref() {
            let parsed_values: Vec<Vec<String>> = values
                .rows
                .iter()
                .map(|row| {
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
                })
                .collect();
            debug!(
                "Parsed INSERT for table {} with {} rows",
                table_name,
                parsed_values.len()
            );
            return Ok((table_name, parsed_values));
        }
    }
    Err("Failed to parse INSERT statement values".to_string())
}

fn extract_create_tables(
    lines: &[String],
) -> (HashSet<String>, Vec<String>, HashMap<String, Vec<String>>) {
    let mut tables = HashSet::new();
    let mut create_statements = Vec::new();
    let mut table_columns = HashMap::new();
    let create_table_regex = Regex::new(r"^\s*CREATE\s+TABLE\s+`?(\w+)`?").unwrap();
    let end_statement_regex = Regex::new(r".*\)\s*;").unwrap();

    let mut current_statement = Vec::new();
    let mut current_table = String::new();
    let mut in_create_table = false;

    for line in lines {
        if !in_create_table {
            if let Some(captures) = create_table_regex.captures(line) {
                in_create_table = true;
                current_table = captures[1].to_string();
                current_statement.clear();
                current_statement.push(line.clone());
            }
        } else {
            current_statement.push(line.clone());
            if end_statement_regex.is_match(line) {
                in_create_table = false;
                let full_statement = current_statement.join("\n");
                create_statements.push(full_statement.clone());
                tables.insert(current_table.clone());

                // Parse the CREATE TABLE statement
                let dialect = MySqlDialect {};
                match Parser::parse_sql(&dialect, &full_statement) {
                    Ok(ast) => {
                        if let Statement::CreateTable { columns, .. } = &ast[0] {
                            let column_names: Vec<String> =
                                columns.iter().map(|col| col.name.value.clone()).collect();
                            table_columns.insert(current_table.clone(), column_names);
                        } else {
                            warn!("Failed to extract columns for table {}", current_table);
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse CREATE TABLE statement for {}: {}",
                            current_table, e
                        );
                    }
                }

                current_statement.clear();
            }
        }
    }

    (tables, create_statements, table_columns)
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
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);
    writer.write_record(values)?;
    writer.flush()?;
    Ok(())
}

fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Starting application");

    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        error!("Incorrect number of arguments");
        eprintln!("Usage: {} <input_sql_file> <output_directory>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    let output_dir = &args[2];

    // Ensure the output directory exists
    std::fs::create_dir_all(output_dir).unwrap_or_else(|e| {
        error!("Failed to create output directory: {}", e);
        std::process::exit(1);
    });

    process_sql_file(input_file, output_dir);

    info!("Application finished successfully");
    Ok(())
}
