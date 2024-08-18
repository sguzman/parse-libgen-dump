use sqlparser::ast::{SetExpr, Statement, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

pub fn process_sql_file(input_file: &str) -> std::io::Result<()> {
    let file_name = Path::new(input_file).file_stem().unwrap().to_str().unwrap();
    let tables_file = format!("{}_tables.sql", file_name);
    let csv_file = format!("{}.csv", file_name);

    // Extract CREATE TABLE statements
    extract_create_tables(input_file, &tables_file)?;

    // Process INSERT statements
    if input_file.contains("scimag") {
        process_large_file(input_file, &csv_file)?;
    } else {
        process_small_file(input_file, &csv_file)?;
    }

    Ok(())
}

fn extract_create_tables(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let mut output = File::create(output_file)?;

    let mut in_create_table = false;
    for line in reader.lines() {
        let line = line?;
        if line.trim_start().to_lowercase().starts_with("create table") {
            in_create_table = true;
        }
        if in_create_table {
            writeln!(output, "{}", line)?;
        }
        if line.trim().ends_with(";") {
            in_create_table = false;
        }
    }
    Ok(())
}

fn process_large_file(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let mut writer = csv::Writer::from_path(output_file)?;

    let mut headers_written = false;
    for line in reader.lines() {
        let line = line?;
        if line.trim_start().to_lowercase().starts_with("insert into") {
            let values = parse_values(&line);
            if !headers_written {
                let headers = column_names(&line);
                writer.write_record(&headers)?;
                headers_written = true;
            }
            for row in values {
                writer.write_record(&row)?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}

fn process_small_file(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let contents = std::fs::read_to_string(input_file)?;
    let mut writer = csv::Writer::from_path(output_file)?;

    let mut headers_written = false;
    for line in contents.lines() {
        if line.trim_start().to_lowercase().starts_with("insert into") {
            let values = parse_values(line);
            if !headers_written {
                let headers = column_names(line);
                writer.write_record(&headers)?;
                headers_written = true;
            }
            for row in values {
                writer.write_record(&row)?;
            }
        }
    }
    writer.flush()?;
    Ok(())
}

fn parse_sql(sql: &str) -> Statement {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    ast[0].clone()
}

fn column_names(sql: &str) -> Vec<String> {
    let insert = parse_sql(sql);
    match insert {
        Statement::Insert { columns, .. } => columns.into_iter().map(|c| c.to_string()).collect(),
        _ => Vec::new(),
    }
}

fn parse_values(sql: &str) -> Vec<Vec<String>> {
    let insert = parse_sql(sql);
    match insert {
        Statement::Insert { source, .. } => {
            if let SetExpr::Values(values) = source.body.as_ref() {
                values
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| match v {
                                sqlparser::ast::Expr::Value(val) => match val {
                                    Value::Number(n, _) => n.to_string(),
                                    Value::SingleQuotedString(s) => s.clone(),
                                    Value::Null => String::new(),
                                    _ => String::new(),
                                },
                                _ => String::new(),
                            })
                            .collect()
                    })
                    .collect()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}
