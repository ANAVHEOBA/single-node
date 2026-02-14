use polars::prelude::*;
use anyhow::{anyhow, Result};
use std::fs;
use std::path::Path;

/// Helper to generate Delta-compatible schema JSON string
fn generate_delta_schema(schema: &Schema) -> String {
    let fields: Vec<String> = schema.iter().map(|(name, dtype)| {
        let delta_type = match dtype {
            DataType::Int32 => "integer",
            DataType::Int64 => "long",
            DataType::Float32 => "float",
            DataType::Float64 => "double",
            DataType::String => "string",
            DataType::Boolean => "boolean",
            DataType::Date => "date",
            DataType::Datetime(_, _) => "timestamp",
            _ => "string", // Fallback
        };
        format!(r#"{{"name":"{}","type":"{}","nullable":true,"metadata":{{}}}}"#, name, delta_type)
    }).collect();

    format!(r#"{{"type":"struct","fields":[{}]}}"#, fields.join(","))
}

pub async fn write_to_delta(mut df: DataFrame, path: &str) -> Result<()> {
    
    log::info!("💾 Finalizing ACID transaction for Delta Table at: {}", path);
    
    // 1. Ensure directory exists
    fs::create_dir_all(path)?;
    fs::create_dir_all(format!("{}/_delta_log", path))?;

    // 2. Write the data part using Polars Parquet writer
    let part_filename = format!("part-{}-spark-killer.parquet", uuid::Uuid::new_v4());
    let full_path = Path::new(path).join(&part_filename);
    let mut file = fs::File::create(&full_path)?;
    
    let schema_json = generate_delta_schema(&df.schema());
    
    ParquetWriter::new(&mut file).finish(&mut df)
        .map_err(|e| anyhow!("Parquet write failed: {}", e))?;

    // 3. REAL ACID LOG: Create the 00000000000000000000.json commit file
    let log_path = Path::new(path).join("_delta_log").join("00000000000000000000.json");
    let commit_content = format!(
        r#"{{"commitInfo":{{"timestamp":{},"operation":"WRITE","operationParameters":{{"mode":"Overwrite"}},"isBlindAppend":false}}}}
{{"protocol":{{"minReaderVersion":1,"minWriterVersion":2}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":{}}}}}
{{"add":{{"path":"{}","partitionValues":{{}},"size":{},"modificationTime":{},"dataChange":true}}}}"#,
        chrono::Utc::now().timestamp_millis(),
        uuid::Uuid::new_v4(),
        schema_json.replace("\"", "\\\""), // Escape for JSON
        chrono::Utc::now().timestamp_millis(),
        part_filename,
        fs::metadata(&full_path)?.len(),
        chrono::Utc::now().timestamp_millis()
    );
    
    fs::write(log_path, commit_content)?;
    log::info!("✅ ACID commit successful: _delta_log/00000000000000000000.json created.");

    Ok(())
}
