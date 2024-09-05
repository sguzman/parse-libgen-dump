use env_logger::Env;
use log::{debug, error, info, warn};
use std::env;

use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

use rayon::prelude::*;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};

use std::path::Path;
use std::sync::{Arc, Mutex};

pub fn process_sql_file(input_file: &str, output_dir: &str) {
    let file = File::open(input_file).unwrap_or_else(|e| {
        error!("Failed to open input file: {}", e);
        std::process::exit(1);
    });
    let reader = BufReader::new(file);
    // Iterate over lines in the file in parallel and filter out non-CREATE TABLE statements
    let create_table_lines: Vec<String> = reader
        .lines()
        .par_bridge()
        .filter_map(|line_result| match line_result {
            Ok(line) => {
                let trimmed_line = line.trim().to_uppercase();
                if trimmed_line.starts_with("CREATE TABLE") {
                    Some(line)
                } else {
                    None
                }
            }
            Err(e) => {
                warn!("Error reading line: {}", e);
                None
            }
        })
        .collect();

    info!("Found {} CREATE TABLE statements", create_table_lines.len());

    // TODO - First pass: extract CREATE TABLE statements and column names

    // TODO - Write CREATE TABLE statements to a file

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
    Err("Not implemented".to_string())
}

fn extract_create_tables(
    lines: &[String],
) -> (HashSet<String>, Vec<String>, HashMap<String, Vec<String>>) {
    // TODO
    (HashSet::new(), Vec::new(), HashMap::new())
}

fn unquote_table_name(table_name: &str) -> String {
    table_name.trim_matches('`').to_string()
}

fn unquote_column_name(column_name: &str) -> String {
    column_name.trim_matches('`').to_string()
}

fn extract_column_names(create_statement: &str) -> Option<Vec<String>> {
    // TODO
    None
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
    // get number of cores
    let num_cores = num_cpus::get();
    // Initialize rayon to use all available CPU cores
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cores)
        .build_global()
        .unwrap();

    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Starting application");

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        error!("Incorrect number of arguments");
        eprintln!("Usage: {} <input_sql_file>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    let output_dir = ".";

    // Ensure the output directory exists
    std::fs::create_dir_all(output_dir).unwrap_or_else(|e| {
        error!("Failed to create output directory: {}", e);
        std::process::exit(1);
    });

    process_sql_file(input_file, output_dir);

    info!("Application finished successfully");
    Ok(())
}
