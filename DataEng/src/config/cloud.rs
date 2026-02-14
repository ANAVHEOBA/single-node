use polars::prelude::cloud::CloudOptions;
use super::app_config::AppConfig;

impl AppConfig {
    pub fn get_cloud_options(&self) -> CloudOptions {
        let endpoint = self.endpoint.clone();
        let path = format!("s3://{}", self.bucket);
        
        let config = [
            ("endpoint", endpoint),
            ("access_key_id", "test".to_string()),
            ("secret_access_key", "test".to_string()),
            ("region", "us-east-1".to_string()),
        ];
        
        CloudOptions::from_untyped_config(&path, config)
            .expect("Failed to create CloudOptions")
    }
}
