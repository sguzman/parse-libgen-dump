// Import libgen_compact.rs
extern crate env_logger;

use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;

use parse_libgen::logic;

// Files to parse
const LIBGEN_COMPACT: &str = "libgen_compact.sql";
const FICTION: &str = "fiction.sql";
const SCIMAG: &str = "scimag.sql";

// Tables to parse
const UPDATED: &str = "updated";
const FICTION_TABLE: &str = "fiction";
const FIC_DESCR: &str = "fiction_description";
const SCIMAG_TABLE: &str = "scimag";

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build_global()
        .unwrap();

    // Initialize logger
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
    log::info!("Starting");

    logic(LIBGEN_COMPACT, UPDATED);
    logic(FICTION, FICTION_TABLE);
    logic(FICTION, FIC_DESCR);
    logic(SCIMAG, SCIMAG_TABLE);
}
