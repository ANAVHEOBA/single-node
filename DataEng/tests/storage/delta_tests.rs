#[cfg(test)]
mod tests {
    use data_eng::storage::write_to_delta;
    use polars::prelude::*;

    #[tokio::test]
    async fn test_write_happy_path() -> anyhow::Result<()> {
        let df = df!(
            "user_id" => [1],
            "total_spend" => [100.0]
        )?;
        let path = "./data/test_delta_table_acid";
        
        let result = write_to_delta(df, path).await;
        assert!(result.is_ok(), "Should successfully write to Delta Table");
        Ok(())
    }

    #[tokio::test]
    async fn test_write_invalid_path() -> anyhow::Result<()> {
        let df = df!(
            "user_id" => [1],
            "total_spend" => [100.0]
        )?;
        let path = "/root/no_permission"; // Assuming no permission
        
        let result = write_to_delta(df, path).await;
        assert!(result.is_err(), "Should fail when writing to restricted path");
        Ok(())
    }
}
