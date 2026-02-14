use polars::prelude::*;
use polars::prelude::cloud::CloudOptions;
use anyhow::{anyhow, Result};

pub fn scan_raw_data(path: &str, options: CloudOptions) -> Result<LazyFrame> {
    log::info!("📡 Initiating discovery on S3 path: {}", path);
    
    let lf = LazyFrame::scan_parquet(
        path,
        ScanArgsParquet {
            cloud_options: Some(options),
            ..Default::default()
        }
    ).map_err(|e| anyhow!("Ingestion failed to initialize: {}", e))?;

    // REAL STUFF: Detailed Analysis
    // 1. Force a schema fetch to verify connectivity and file existence
    let schema = lf.clone().schema()
        .map_err(|e| anyhow!("Inference failed. Path may be empty, invalid, or inaccessible: {}. Error: {}", path, e))?;
    
    // 2. Log discovered columns for detailed visibility (Separation of Concerns: Analysis)
    log::info!("📊 Discovered Schema with {} columns:", schema.len());
    for (name, dtype) in schema.iter() {
        log::info!("  - {}: {:?}", name, dtype);
    }

    // 3. Early check for required columns
    let required = ["user_id", "timestamp", "amount"];
    for col_name in required {
        if !schema.contains(col_name) {
            return Err(anyhow!("Missing required column: {}", col_name));
        }
    }

    Ok(lf)
}
