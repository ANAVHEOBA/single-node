use anyhow::{anyhow, Result};

pub fn validate_s3_path(path: &str) -> Result<()> {
    if !path.starts_with("s3://") {
        return Err(anyhow!("Invalid protocol: path must start with s3://"));
    }
    if !path.contains('*') && !path.ends_with(".parquet") {
        return Err(anyhow!("Invalid file format: path must point to parquet files"));
    }
    Ok(())
}
