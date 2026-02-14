#[cfg(test)]
mod tests {
    use data_eng::ingestion::scan_raw_data;
    use data_eng::config::AppConfig;

    #[test]
    fn test_scan_happy_path() {
        let config = AppConfig {
            bucket: "spark-killer-bucket".into(),
            endpoint: "http://localhost:4566".into(),
        };
        let options = config.get_cloud_options();
        let path = format!("s3://{}/raw-data/*.parquet", config.bucket);

        // Polars schema() might use its own internal runtime, 
        // so we run this in a plain test to avoid nesting conflicts.
        let result = scan_raw_data(&path, options);
        assert!(result.is_ok(), "Should successfully initialize LazyFrame scan and infer schema");
    }

    #[test]
    fn test_scan_non_existent_bucket() {
        let config = AppConfig {
            bucket: "non-existent-bucket".into(),
            endpoint: "http://localhost:4566".into(),
        };
        let options = config.get_cloud_options();
        let path = format!("s3://{}/raw-data/*.parquet", config.bucket);

        let result = scan_raw_data(&path, options);
        assert!(result.is_err(), "Should return error for non-existent bucket");
    }

    #[test]
    fn test_scan_empty_path() {
        let config = AppConfig {
            bucket: "spark-killer-bucket".into(),
            endpoint: "http://localhost:4566".into(),
        };
        let options = config.get_cloud_options();
        let path = format!("s3://{}/empty-folder/*.parquet", config.bucket);

        let result = scan_raw_data(&path, options);
        assert!(result.is_err(), "Should return error if no files found");
    }
}
