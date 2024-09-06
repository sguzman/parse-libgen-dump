use cached::proc_macro::io_cached;
use cached::DiskCache;
use env_logger::Env;
use log::info;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, PartialEq, Clone, Error)]
enum CacheError {
    #[error("error with disk cache `{0}`")]
    DiskError(String),
}

/// Cache the results of a function on disk.
/// Cache files will be stored under the system cache dir
/// unless otherwise specified with `disk_dir` or the `create` argument.
/// A `map_error` closure must be specified to convert any
/// disk cache errors into the same type of error returned
/// by your function. All `io_cached` functions must return `Result`s.
#[io_cached(
    map_error = r##"|e| CacheError::DiskError(format!("{:?}", e))"##,
    disk = true,
    sync_to_disk_on_cache_change = true,
    disk_dir = ".cache"
)]
fn cached_sleep_secs(secs: u64) -> Result<String, CacheError> {
    std::thread::sleep(std::time::Duration::from_secs(secs));
    Ok(secs.to_string())
}

const INPUT_FILE: &str = "resources/data.sql";

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let input_file = Path::new(INPUT_FILE);
    info!("Processing file: {}", input_file.display());

    let result = cached_sleep_secs(5);
    info!("Result: {:?}", result);

    info!("SQL file processing completed successfully");
    Ok(())
}
