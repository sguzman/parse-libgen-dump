use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::collections::HashMap;
use regex::Regex;
use csv::Writer;
use log::{info, warn, error};
use sqlparser::ast::{Statement, SetExpr, Values};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

// ... existing imports and setup ...

fn extract_column_names(create_statement: &str) -> Vec<String> {
    // ... implementation ...
}

fn process_create_statements(input_file: &Path) -> HashMap<String, Vec<String>> {
    // ... implementation ...
}

fn parse_insert_values(string: &str) -> Vec<Vec<Option<String>>> {
    // ... implementation using sqlparser ...
}

fn process_insert_statements(input_file: &Path, table_columns: &HashMap<String, Vec<String>>) {
    // ... implementation ...
}

fn main() {
    // ... setup logging ...

    let script_dir = std::env::current_dir().unwrap();
    let resources_dir = script_dir.join("resources");
    let input_file = resources_dir.join("data.sql");

    if !input_file.exists() {
        error!("Input file not found: {:?}", input_file);
        return;
    }

    info!("Starting to process {:?}", input_file);

    let table_columns = process_create_statements(&input_file);
    process_insert_statements(&input_file, &table_columns);

    info!("Processing complete");
}