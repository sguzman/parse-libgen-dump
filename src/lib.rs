use log::{debug, error, info, warn};
use rayon::prelude::*;
use sqlparser::ast::{SetExpr, Statement, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

pub fn process_sql_file_parallel(input_file: &str, output_file: &str) -> std::io::Result<()> {
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(output_file)?));

    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    let headers = Arc::new(Mutex::new(None));
    let insert_count = Arc::new(Mutex::new(0));

    lines.par_iter().for_each(|line| {
        if line.trim_start().to_lowercase().starts_with("insert into") {
            let values = parse_values(line);

            // Write headers if not written yet
            {
                let mut headers_guard = headers.lock().unwrap();
                if headers_guard.is_none() {
                    let column_names = column_names(line);
                    *headers_guard = Some(column_names.clone());
                    let mut writer = writer.lock().unwrap();
                    writer.write_record(&column_names).unwrap();
                    info!("CSV headers written: {:?}", column_names);
                }
            }

            // Write values
            {
                let mut writer = writer.lock().unwrap();
                for row in values {
                    writer.write_record(&row).unwrap();
                }
            }

            // Increment insert count
            {
                let mut count = insert_count.lock().unwrap();
                *count += 1;
                if *count % 1000 == 0 {
                    info!("Processed {} INSERT statements", *count);
                }
            }
        }
    });

    let final_count = *insert_count.lock().unwrap();
    info!("Total INSERT statements processed: {}", final_count);
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
