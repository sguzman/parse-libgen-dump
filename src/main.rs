extern crate rayon;
extern crate sqlparser;

use sqlparser::ast::SetExpr;
use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use rayon::prelude::*;
use sqlparser::ast::Query;
use std::fs::read_to_string;

// Get first cmd line arg
fn get_arg() -> String {
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(0).unwrap();

    arg.to_string()
}

fn read_lines(filename: &str) -> Vec<String> {
    let mut result = Vec::new();

    for line in read_to_string(filename).unwrap().lines() {
        result.push(line.to_string())
    }

    result
}

fn parse_sql(sql: &String) -> Vec<Statement> {
    let dialect = MySqlDialect {};
    let sql = sql.as_str();
    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    ast
}

// Get column names from SQL
fn column_names(sql: &String) -> Vec<String> {
    let ast = parse_sql(sql);
    let insert = ast.first().unwrap();

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
fn query(insert: &Statement) -> Query {
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
                _ => {
                    panic!("Unknown type");
                }
            };

            single_row.push(string.to_string());
        });

        rows.push(single_row);
    });

    rows
}

// Use rayon to parallelize map
fn rayonize(contents: Vec<&String>) -> Vec<Vec<String>> {
    contents
        .into_par_iter()
        .map(|line| {
            let ast = parse_sql(line);
            let insert = ast.first().unwrap();

            // Get query values
            let src = query(insert);

            // Get values
            let val = values(src);

            // iterate over rows
            let rows = rows(val);

            rows
        })
        .flatten()
        .collect()
}

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build_global()
        .unwrap();

    // Read libgen.sql
    const filename: &str = "libgen.sql";

    // Get file contents, by lines
    let contents = read_lines(filename);

    // Filter down to only first 100 lines
    let contents = contents.iter().take(2).collect::<Vec<&String>>();

    // Using rayon to parallelize map
    let rows = rayonize(contents);
    println!("{:?}", rows);

    println!("Done");
}
