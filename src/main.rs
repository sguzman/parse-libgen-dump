use cached::proc_macro::io_cached;
use cached::DiskCache;
use env_logger::Env;
use log::info;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, PartialEq, Clone, Error, Serialize, Deserialize)]
enum CacheError {
    #[error("error with disk cache `{0}`")]
    DiskError(String),
}

const INPUT_FILE: &str = "resources/data.sql";

// Compose two arbitrary functions
fn compose<F, G, A, B, C>(f: F, g: G) -> impl Fn(A) -> Result<cached::Return<C>, CacheError>
where
    F: Fn(A) -> Result<B, CacheError>,
    G: Fn(B) -> Result<C, CacheError> + Clone,
{
    move |x: A| f(x).and_then(|y: B| g(y).map(cached::Return::new))
}

// Type of f1
type F1 = fn(&str) -> Result<Vec<Vec<String>>, CacheError>;

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let input_file: &Path = Path::new(INPUT_FILE);
    info!("Processing file: {}", input_file.display());

    info!("SQL file processing completed successfully");
    Ok(())
}
