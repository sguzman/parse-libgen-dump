use env_logger::Env;
use log::info;
use std::path::Path;

const INPUT_FILE: &str = "resources/data.sql";

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let input_file = Path::new(INPUT_FILE);
    info!("Processing file: {}", input_file.display());

    info!("SQL file processing completed successfully");
    Ok(())
}
