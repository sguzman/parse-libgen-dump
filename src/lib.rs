use log::{error, info};
use rayon::prelude::*;
use sqlparser::ast::Statement;
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

    // First pass: extract CREATE TABLE statements
    let (tables, create_statements) = extract_create_tables(&lines);
    
    // Write CREATE TABLE statements to a file
    write_create_tables(&create_statements, output_dir)?;

    // Create CSV files for each table
    for table in &tables {
        let csv_path = Path::new(output_dir).join(format!("{}.csv", table));
        File::create(csv_path)?;
    }

    // Second pass: process INSERT statements in parallel
    let tables = Arc::new(tables);
    let output_dir = Arc::new(output_dir.to_string());

    lines.par_iter().for_each(|line| {
        if line.trim_start().to_lowercase().starts_with("insert into") {
            if let Ok((table_name, values)) = parse_insert(line) {
                if tables.contains(&table_name) {
                    let csv_path = Path::new(&*output_dir).join(format!("{}.csv", table_name));
                    if let Err(e) = append_to_csv(&csv_path, &values) {
                        error!("Failed to write to CSV for table {}: {}", table_name, e);
                    }
                }
            }
        }
    });

    info!("Processing complete. Check the output directory for results.");
    Ok(())
}

fn extract_create_tables(lines: &[String]) -> (HashSet<String>, Vec<String>) {
    let mut tables = HashSet::new();
    let mut create_statements = Vec::new();
    let mut current_statement = Vec::new();
    let mut in_create_table = false;

    for line in lines {
        let trimmed = line.trim();
        if trimmed.to_lowercase().starts_with("create table") {
            in_create_table = true;
            current_statement.clear();
            if let Some(table_name) = extract_table_name(trimmed) {
                tables.insert(table_name);
            }
        }

        if in_create_table {
            current_statement.push(line.clone());

            if trimmed.ends_with(';') {
                in_create_table = false;
                create_statements.push(current_statement.join("\n"));
                current_statement.clear();
            }
        }
    }

    (tables, create_statements)
}

fn extract_table_name(create_statement: &str) -> Option<String> {
    let parts: Vec<&str> = create_statement.split_whitespace().collect();
    if parts.len() >= 3 {
        Some(parts[2].trim_matches('`').to_string())
    } else {
        None
    }
}

fn write_create_tables(create_statements: &[String], output_dir: &str) -> std::io::Result<()> {
    let path = Path::new(output_dir).join("create_tables.sql");
    let mut file = File::create(path)?;
    for statement in create_statements {
        writeln!(file, "{}", statement)?;
    }
    Ok(())
}

fn parse_insert(sql: &str) -> Result<(String, Vec<String>), String> {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    if let Statement::Insert { table_name, source, .. } = &ast[0] {
        let table_name = table_name.to_string();
        if let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() {
            if let Some(row) = values.rows.first() {
                let parsed_values: Vec<String> = row.iter()
                    .map(|expr| match expr {
                        sqlparser::ast::Expr::Value(val) => match val {
                            sqlparser::ast::Value::Number(n, _) => n.to_string(),
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Null => "NULL".to_string(),
                            _ => format!("{:?}", val),
                        },
                        _ => format!("{:?}", expr),
                    })
                    .collect();
                return Ok((table_name, parsed_values));
            }
        }
    }
    Err("Failed to parse INSERT statement".to_string())
}

fn append_to_csv(path: &Path, values: &[String]) -> std::io::Result<()> {
    let file = OpenOptions::new()
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