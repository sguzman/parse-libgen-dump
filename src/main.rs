use env_logger::Env;
use log::{error, info};
use parse_libgen::process_sql_file_parallel;
use rayon::ThreadPoolBuilder;
use std::env;
use std::path::Path;

fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Starting application");

    // Initialize Rayon thread pool
    let num_cores = 4;
    ThreadPoolBuilder::new()
        .num_threads(num_cores)
        .build_global()
        .unwrap();
    info!("Initialized Rayon with {} cores", num_cores);

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        error!("Incorrect number of arguments");
        eprintln!("Usage: {} <input_sql_file>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];
    let output_file = generate_output_filename(input_file);

    if let Err(e) = process_sql_file_parallel(input_file, &output_file) {
        error!("Error processing file: {}", e);
        std::process::exit(1);
    }

    info!("Application finished successfully");
    Ok(())
}

fn generate_output_filename(input_file: &str) -> String {
    let path = Path::new(input_file);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("csv");

    format!("{}_table.{}", stem, extension)
}
