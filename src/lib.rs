use log::{debug, error, info, warn};
use sqlparser::ast::{SetExpr, Statement, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

pub fn process_sql_file(input_file: &str) -> std::io::Result<()> {
    info!("Processing SQL file: {}", input_file);
    let file_name = Path::new(input_file).file_stem().unwrap().to_str().unwrap();
    let tables_file = format!("{}_tables.sql", file_name);
    let csv_file = format!("{}.csv", file_name);

    // Extract CREATE TABLE statements
    info!("Extracting CREATE TABLE statements to {}", tables_file);
    extract_create_tables(input_file, &tables_file)?;

    // Process INSERT statements
    if input_file.contains("scimag") {
        info!("Processing large file: {}", input_file);
        process_large_file(input_file, &csv_file)?;
    } else {
        info!("Processing small file: {}", input_file);
        process_small_file(input_file, &csv_file)?;
    }

    info!("Processing complete for {}", input_file);
    Ok(())
}

fn extract_create_tables(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let mut output = File::create(output_file)?;

    let mut in_create_table = false;
    let mut table_count = 0;
    for line in reader.lines() {
        let line = line?;
        if line.trim_start().to_lowercase().starts_with("create table") {
            in_create_table = true;
            table_count += 1;
            debug!("Found CREATE TABLE statement: {}", line);
        }
        if in_create_table {
            writeln!(output, "{}", line)?;
        }
        if line.trim().ends_with(";") {
            in_create_table = false;
        }
    }
    info!("Extracted {} CREATE TABLE statements", table_count);
    Ok(())
}

fn process_large_file(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let mut writer = csv::Writer::from_path(output_file)?;

    let mut headers_written = false;
    let mut insert_count = 0;
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim_start().to_lowercase().starts_with("insert into") {
            insert_count += 1;
            let values = parse_values(&line);
            if !headers_written {
                let headers = column_names(&line);
                writer.write_record(&headers)?;
                headers_written = true;
                info!("CSV headers written: {:?}", headers);
            }
            for row in values {
                writer.write_record(&row)?;
            }
            if insert_count % 1000 == 0 {
                info!("Processed {} INSERT statements", insert_count);
            }
        }
        if index % 100000 == 0 {
            debug!("Processed {} lines", index);
        }
    }
    writer.flush()?;
    info!("Total INSERT statements processed: {}", insert_count);
    Ok(())
}

fn process_small_file(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let contents = std::fs::read_to_string(input_file)?;
    let mut writer = csv::Writer::from_path(output_file)?;

    let mut headers_written = false;
    let mut insert_count = 0;
    for line in contents.lines() {
        if line.trim_start().to_lowercase().starts_with("insert into") {
            insert_count += 1;
            let values = parse_values(line);
            if !headers_written {
                let headers = column_names(line);
                writer.write_record(&headers)?;
                headers_written = true;
                info!("CSV headers written: {:?}", headers);
            }
            for row in values {
                writer.write_record(&row)?;
            }
        }
    }
    writer.flush()?;
    info!("Total INSERT statements processed: {}", insert_count);
    Ok(())
}

fn parse_sql(sql: &str) -> Statement {
    let dialect = MySqlDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => ast[0].clone(),
        Err(e) => {
            error!("Failed to parse SQL: {}", e);
            panic!("SQL parsing error");
        }
    }
}

fn column_names(sql: &str) -> Vec<String> {
    let insert = parse_sql(sql);
    match insert {
        Statement::Insert { columns, .. } => {
            let names: Vec<String> = columns.into_iter().map(|c| c.to_string()).collect();
            debug!("Extracted column names: {:?}", names);
            names
        }
        _ => {
            warn!("Unexpected statement type when extracting column names");
            Vec::new()
        }
    }
}

fn parse_values(sql: &str) -> Vec<Vec<String>> {
    let insert = parse_sql(sql);
    match insert {
        Statement::Insert { source, .. } => {
            if let SetExpr::Values(values) = source.body.as_ref() {
                let parsed_values: Vec<Vec<String>> = values
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| match v {
                                sqlparser::ast::Expr::Value(val) => match val {
                                    Value::Number(n, _) => n.to_string(),
                                    Value::SingleQuotedString(s) => s.clone(),
                                    Value::Null => String::new(),
                                    _ => {
                                        warn!("Unexpected value type: {:?}", val);
                                        String::new()
                                    }
                                },
                                _ => {
                                    warn!("Unexpected expression type: {:?}", v);
                                    String::new()
                                }
                            })
                            .collect()
                    })
                    .collect();
                debug!("Parsed {} rows of values", parsed_values.len());
                parsed_values
            } else {
                warn!("Unexpected SetExpr type in INSERT statement");
                Vec::new()
            }
        }
        _ => {
            warn!("Unexpected statement type when parsing values");
            Vec::new()
        }
    }
}
