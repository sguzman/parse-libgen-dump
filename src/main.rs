use parse_libgen::process_sql_file;
use std::env;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <sql_file>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    process_sql_file(input_file)?;

    println!("Processing complete.");
    Ok(())
}
