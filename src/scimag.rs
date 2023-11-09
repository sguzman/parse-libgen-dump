// Import libgen_compact.rs
extern crate csv;
extern crate env_logger;
extern crate tokio;

use std::io::{BufRead, Write};

use chrono::Local;
use env_logger::Builder;
use log::{debug, error, info, LevelFilter};
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;
use tokio::task;

// Files to parse
const SCIMAG: &str = "scimag.sql";

// Tables to parse
const SCIMAGS: &str = "scimag";
const MAGS: &str = "magazines";
const PUBS: &str = "publishers";
const REPORTS: &str = "error_report";

fn predicate(line: &String, table: &str) -> bool {
    line.starts_with(format!("INSERT INTO `{}`", table).as_str())
}

// Write a single row into an open file handle
pub fn write_row(writer: &mut csv::Writer<std::fs::File>, row: Vec<String>) {
    writer.write_record(row).unwrap();
}

// Produce lines from a file
async fn get_line(
    line: String,
    tx1: mpsc::Sender<String>,
    tx2: mpsc::Sender<String>,
    tx3: mpsc::Sender<String>,
    tx4: mpsc::Sender<String>,
) {
    if predicate(&line, SCIMAGS) {
        tx1.send(line).await.unwrap();
    } else if predicate(&line, MAGS) {
        tx2.send(line).await.unwrap();
    } else if predicate(&line, PUBS) {
        tx3.send(line).await.unwrap();
    } else if predicate(&line, REPORTS) {
        tx4.send(line).await.unwrap();
    } else {
        debug!("Ignoring line");
    }
}

async fn produce(
    file_path: String,
    tx1: mpsc::Sender<String>,
    tx2: mpsc::Sender<String>,
    tx3: mpsc::Sender<String>,
    tx4: mpsc::Sender<String>,
) -> Result<(), std::io::Error> {
    info!("Reading lines from {}", file_path);

    let file = std::fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);

    // Iterate over lines
    reader.lines().for_each(|line| {
        if let Ok(line) = line {
            // Use let to capture variables for this closure
            let tx1 = tx1.clone();
            let tx2 = tx2.clone();
            let tx3 = tx3.clone();
            let tx4 = tx4.clone();

            // Spawn a task to process the line
            task::spawn(async move {
                get_line(line, tx1, tx2, tx3, tx4).await;
            });
        }
    });

    Ok(())
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
    if file.exists() {
        debug!("File exists already");
    } else {
        debug!("Creating file");
        let mut file = std::fs::File::create(file).unwrap();
        file.write_all(b"").unwrap();
        file.sync_all().unwrap();
        file.flush().unwrap();
    }
}

async fn consume(mut rx: mpsc::Receiver<String>, output_file: &str) {
    info!("Writing to {}", output_file);
    let csv = format!("{}.csv", output_file);
    ensure_file(&csv);
    let mut writer = csv::Writer::from_path(csv).unwrap();

    // First line is column names
    let line = rx.recv().await.unwrap();
    let columns = column_names(&line);
    info!("Columns: {:?} for file {}", columns, output_file);
    write_row(&mut writer, columns);

    while let Some(line) = rx.recv().await {
        let data = parse_values(&line);
        for row in data {
            write_row(&mut writer, row);
        }
    }

    info!("Flushing writer for {}", output_file);
    writer.flush().unwrap();
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
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

    // Make 5 channels. One for the producer and four for the consumers
    let (tx1, rx1) = mpsc::channel(100);
    let (tx2, rx2) = mpsc::channel(100);
    let (tx3, rx3) = mpsc::channel(100);
    let (tx4, rx4) = mpsc::channel(100);

    let producer = task::spawn(produce(SCIMAG.to_string(), tx1, tx2, tx3, tx4));
    let consumer1 = task::spawn(consume(rx1, SCIMAGS));
    let consumer2 = task::spawn(consume(rx2, MAGS));
    let consumer3 = task::spawn(consume(rx3, PUBS));
    let consumer4 = task::spawn(consume(rx4, REPORTS));

    // Await_all for producer and two consumers
    let _ = tokio::join!(producer, consumer1, consumer2, consumer3, consumer4);

    Ok(())
}
