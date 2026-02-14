use polars::prelude::*;
use polars::prelude::cloud::CloudOptions;
use std::env;
use anyhow::Result;

#[allow(dead_code)]
pub fn get_test_cloud_options() -> CloudOptions {
    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string());
    
    // In Polars 0.41, we use from_untyped_config for AWS settings
    let config = [
        ("endpoint", endpoint),
        ("access_key_id", "test".to_string()),
        ("secret_access_key", "test".to_string()),
        ("region", "us-east-1".to_string()),
    ];
    
    CloudOptions::from_untyped_config("s3://dummy-bucket", config).unwrap()
}

#[allow(dead_code)]
pub fn create_mock_df() -> Result<DataFrame> {
    let df = df!(
        "user_id" => [1, 1, 2, 2],
        "timestamp" => [
            "2025-01-01 10:00:00",
            "2025-01-01 11:00:00",
            "2025-01-08 10:00:00",
            "2025-01-08 11:00:00"
        ],
        "amount" => [100.0, 200.0, 50.0, 50.0],
        "category" => ["Tech", "Dining", "Groceries", "Tech"]
    )?;
    Ok(df)
}
