use polars::prelude::*;
use anyhow::{anyhow, Result};
use std::fs;

pub fn write_to_parquet(mut df: DataFrame, path: &str) -> Result<()> {
    log::info!("💾 Writing {} rows to Parquet: {}", df.height(), path);
    
    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    
    let mut file = fs::File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut df)
        .map_err(|e| anyhow!("Parquet write failed: {}", e))?;
        
    log::info!("✅ Parquet file written successfully.");
    Ok(())
}
