extern crate sqlparser;

use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::fs::read_to_string;

fn read_lines(filename: &str) -> Vec<String> {
    let mut result = Vec::new();

    for line in read_to_string(filename).unwrap().lines() {
        result.push(line.to_string())
    }

    result
}

fn parse_sql(sql: String)  {
    let dialect = MySqlDialect {};
    let sql = sql.as_str();
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    
    println!("AST: {:?}", ast);
}
fn main() {
    // Read libgen.sql
    const filename: &str = "libgen.sql";
    
    // Get file contents, by lines
    let contents = read_lines(filename);

    // Print length of contents
    println!("Lines: {}", contents.len());

    // Parse the first line
    parse_sql(contents[0].clone());

    println!("Done");
}
