use std::env;

pub struct AppConfig {
    pub bucket: String,
    pub endpoint: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            bucket: env::var("S3_BUCKET").unwrap_or_else(|_| "spark-killer-bucket".to_string()),
            endpoint: env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:4566".to_string()),
        }
    }
}
