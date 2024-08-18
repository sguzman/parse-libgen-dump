use env_logger::Env;
use log::{error, info, debug};
use parse_libgen::process_sql_file;
use std::env;

fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Starting application");

    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        error!("Incorrect number of arguments");
        eprintln!("Usage: {} <input_sql_file> <output_directory>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    let output_dir = &args[2];

    // Ensure the output directory exists
    std::fs::create_dir_all(output_dir).unwrap_or_else(|e| {
        error!("Failed to create output directory: {}", e);
        std::process::exit(1);
    });

    process_sql_file(input_file, output_dir);

    info!("Application finished successfully");
    Ok(())
}