#[cfg(test)]
mod tests {
    use data_eng::transformations::aggregate_by_user_weekly;
    use polars::prelude::*;

    #[test]
    fn test_agg_happy_path() -> anyhow::Result<()> {
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
        
        let result = aggregate_by_user_weekly(df.lazy())?.collect()?;
        
        assert_eq!(result.height(), 2);
        // Add specific column value checks here
        Ok(())
    }

    #[test]
    fn test_agg_with_nulls() -> anyhow::Result<()> {
        let s0 = Series::new("user_id".into(), &[Some(1), Some(1), Some(2)]);
        let s1 = Series::new("timestamp".into(), &["2025-01-01 10:00:00", "2025-01-01 11:00:00", "2025-01-01 12:00:00"]);
        let s2 = Series::new("amount".into(), &[Some(100.0), None, Some(50.0)]);
        
        let df = DataFrame::new(vec![s0, s1, s2])?;
        
        let result = aggregate_by_user_weekly(df.lazy())?.collect()?;
        
        // Assert how nulls are handled (usually skipped in sum/mean)
        let user_1_sum = result.lazy()
            .filter(col("user_id").eq(1))
            .collect()?
            .column("total_spend")?
            .f64()?
            .get(0);
            
        assert_eq!(user_1_sum, Some(100.0));
        Ok(())
    }

    #[test]
    fn test_agg_year_boundary() -> anyhow::Result<()> {
        let df = df!(
            "user_id" => [1, 1],
            "timestamp" => [
                "2024-12-24 10:00:00",
                "2025-01-01 10:00:00"
            ],
            "amount" => [100.0, 100.0]
        )?;
        
        let result = aggregate_by_user_weekly(df.lazy())?.collect()?;
        
        // Should produce two different weeks if truncate works correctly
        assert_eq!(result.height(), 2, "Year boundary should be handled correctly by producing 2 separate weekly buckets");
        Ok(())
    }

    #[test]
    fn test_agg_empty_dataframe() -> anyhow::Result<()> {
        let df = df!(
            "user_id" => Vec::<i32>::new(),
            "timestamp" => Vec::<String>::new(),
            "amount" => Vec::<f64>::new()
        )?;
        
        let result = aggregate_by_user_weekly(df.lazy())?.collect()?;
        assert_eq!(result.height(), 0);
        Ok(())
    }
}
