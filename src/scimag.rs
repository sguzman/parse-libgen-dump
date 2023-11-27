// Import libgen_compact.rs
extern crate csv;
extern crate env_logger;
extern crate rayon;

use chrono::Local;
use env_logger::Builder;
use log::{debug, error, info, LevelFilter};
use rayon::prelude::*;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::io::{BufRead, Write};

const CORES: usize = 1;
const WORK: usize = 2;

const TOTAL: usize = CORES * WORK;

// Files to parse
const INPUT: &str = "scimag.sql";

// Tables to parse
const SCIMAGS: &str = "scimag";
const MAGS: &str = "magazines";
const PUBS: &str = "publishers";
const REPORTS: &str = "error_report";

// Struct to hold the number of lines for each file
struct Data {
    scimag: Vec<Vec<String>>,
    magazines: Vec<Vec<String>>,
    publishers: Vec<Vec<String>>,
    error_report: Vec<Vec<String>>,
}

fn predicate(line: &String, table: &str) -> bool {
    line.starts_with(format!("INSERT INTO `{}`", table).as_str())
}

// Write a single row into an open file handle
pub fn write_row(writer: &mut csv::Writer<std::fs::File>, row: Vec<String>) {
    writer.write_record(row).unwrap();
}

// Return string of name of file to write to
fn get_line(line: String) -> Option<&'static str> {
    let out = if predicate(&line, SCIMAGS) {
        SCIMAGS
    } else if predicate(&line, MAGS) {
        MAGS
    } else if predicate(&line, PUBS) {
        PUBS
    } else if predicate(&line, REPORTS) {
        REPORTS
    } else {
        ""
    };

    if out == "" {
        None
    } else {
        Some(out)
    }
}

fn process_lines() {
    info!("Reading lines from {}", INPUT);
    use parse_libgen::my_reader;

    let mut reader = my_reader::BufReader::open(INPUT).unwrap();
    let mut buffer = String::new();
    let mut line_work = Vec::new();

    // Ensure the output file exists
    ensure_file(&format!("{}.csv", SCIMAGS));
    ensure_file(&format!("{}.csv", MAGS));
    ensure_file(&format!("{}.csv", PUBS));
    ensure_file(&format!("{}.csv", REPORTS));

    // Create writer for each file
    let mut scimag = csv::Writer::from_path(format!("{}.csv", SCIMAGS)).unwrap();
    let mut magazines = csv::Writer::from_path(format!("{}.csv", MAGS)).unwrap();
    let mut publishers = csv::Writer::from_path(format!("{}.csv", PUBS)).unwrap();
    let mut error_report = csv::Writer::from_path(format!("{}.csv", REPORTS)).unwrap();

    // First time through, write the column names
    let mut scimag_first = true;
    let mut magazines_first = true;
    let mut publishers_first = true;
    let mut error_report_first = true;

    // Iterate over lines
    while let Some(line) = reader.read_line(&mut buffer) {
        if let Ok(line) = line {
            let line = line.trim().to_string();

            if line_work.len() == TOTAL {
                // Use Rayon to process each line in line_work in parallel
                let items = line_work.par_iter().map(|line: &String| {
                    // Get the name of the file to write to
                    let name = get_line(line.clone());

                    // If the name is None, then we don't care about this line
                    if let Some(name) = name {
                        // Get the values from the line
                        let values = parse_values(line);

                        // Get the column names from the line
                        let columns = column_names(line);
                    }
                });
            } else {
                line_work.push(line.clone());
            }
        }
    }
}

fn parse_sql(sql: &String) -> Statement {
    let dialect = MySqlDialect {};
    let sql = sql.as_str();
    debug!("Parsing SQL: {}", sql);
    // Parse SQL
    let ast = Parser::parse_sql(&dialect, sql);

    match ast {
        Ok(ast) => {
            // Get first statement
            let insert = ast.first().unwrap().clone();

            insert
        }
        Err(e) => {
            error!("Error parsing SQL: {}", e);
            // Print sql
            panic!("{}", sql);
        }
    }
}

// Get column names from SQL
fn column_names(sql: &String) -> Vec<String> {
    let insert = parse_sql(sql);

    match insert {
        Statement::Insert { columns, .. } => columns
            .iter()
            .map(|c| c.to_string())
            .map(|s| s.replace("`", ""))
            .collect(),
        _ => Vec::new(),
    }
}

// Get query object from statement
fn query(insert: Statement) -> Query {
    let src = match insert {
        Statement::Insert { source, .. } => Some(source),
        _ => None,
    };

    let src = src.unwrap();
    let src = src.clone();
    // Deference
    let src = *src;

    src
}

// Get values object from query
fn values(query: Query) -> sqlparser::ast::Values {
    let values = match query {
        Query { body, .. } => Some(body),
        _ => None,
    };

    let values = values.unwrap();

    let values = values.clone();
    let values = *values;

    // Iterate over values
    let val = match values {
        Values(values) => Some(values),
        _ => None,
    };

    let val = val.unwrap();

    val
}

// Get rows from values
fn rows(val: sqlparser::ast::Values) -> Vec<Vec<String>> {
    let mut rows = Vec::<Vec<String>>::new();
    val.rows.iter().for_each(|row| {
        // iterate over columns
        let mut single_row = Vec::new();
        row.iter().for_each(|col| {
            // Match for number and SingleQuotedString
            let string = match col {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(num, _)) => num,
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => s,
                // Handle null as empty string
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Null) => "",
                _ => {
                    error!("Unknown type: {:?}", col);
                    panic!("Unknown type");
                }
            };

            single_row.push(string.to_string());
        });

        rows.push(single_row);
    });

    debug!("Rows: {}", rows.len());
    rows
}

// From a string, parse out the insert statement values and return them as a vector
fn parse_values(sql: &String) -> Vec<Vec<String>> {
    let insert = parse_sql(sql);

    // Get query values
    let src = query(insert);

    // Get values
    let val = values(src);

    // iterate over rows
    let rs = rows(val);

    rs
}

// Given a string, assume its a filename and ensure it exists
fn ensure_file(file: &String) {
    let file = std::path::Path::new(&file);
    debug!("Creating file");
    let mut file = std::fs::File::create(file).unwrap();
    file.write_all(b"").unwrap();
    file.sync_all().unwrap();
    file.flush().unwrap();
}

// FUnction to write to file on subsequent calls
fn next_write(name: &'static str, mut csv: csv::Writer<std::fs::File>, data: Vec<Vec<String>>) {
    info!("Writing row to {}", name);

    for row in data {
        write_row(&mut csv, row);
    }
}

// Function to write to file on initial call
fn init_write(name: &'static str, mut csv: csv::Writer<std::fs::File>, columns: Vec<String>) {
    info!("Columns: {:?} for file {}", columns, name);
    write_row(&mut csv, columns);
}

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(CORES)
        .build_global()
        .unwrap();

    // Initialize logger
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
    info!("Starting");

    process_lines();
}
