use env_logger::Env;
use log::{error, info};
use parse_libgen::process_sql_file_parallel;
use std::env;

fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Starting application");

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        error!("Incorrect number of arguments");
        eprintln!("Usage: {} <sql_file>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    if let Err(e) = process_sql_file_parallel(input_file) {
        error!("Error processing file: {}", e);
        std::process::exit(1);
    }

    info!("Application finished successfully");
    Ok(())
}