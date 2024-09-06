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

type InputFile = &'static str;
type IndexType = u64;

type CreateStmtLineNum = IndexType;
type CreateStmtLineNumPlus = IndexType;
type CreateStmtLineNums = Vec<CreateStmtLineNum>;
type CreateStmtRawRange = (CreateStmtLineNum, CreateStmtLineNumPlus);
type CreateStmtLines = Vec<String>;
type CreateStmtString = String;
type CreateStmtStrings = Vec<CreateStmtString>;

type InsertStmtLineNum = IndexType;
type InsertStmtLineNumPlus = IndexType;
type InsertStmtLineNums = Vec<InsertStmtLineNum>;
type InsertStmtLineString = String;
type InsertStmtLineStrings = Vec<InsertStmtLineString>;

// Compose two arbitrary functions
fn compose<F, G, A, B, C>(f: F, g: G) -> impl Fn(A) -> Result<cached::Return<C>, CacheError>
where
    F: Fn(A) -> Result<B, CacheError>,
    G: Fn(B) -> Result<C, CacheError> + Clone,
{
    move |x: A| f(x).and_then(|y: B| g(y).map(cached::Return::new))
}

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let input_file: &Path = Path::new(INPUT_FILE);
    info!("Processing file: {}", input_file.display());

    info!("SQL file processing completed successfully");
    Ok(())
}
