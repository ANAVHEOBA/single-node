use polars::prelude::*;
use anyhow::Result;

pub fn aggregate_by_user_weekly(lf: LazyFrame) -> Result<LazyFrame> {
    // REAL STUFF: The most robust way to handle timestamps in Polars 0.41
    // We attempt a cast to Datetime. If it's already a Datetime, this is a no-op.
    // If it's a string, Polars will attempt to parse it using standard ISO formats.
    let lf = lf.with_column(
        col("timestamp")
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("timestamp")
    );

    Ok(lf
        .group_by([
            col("user_id"),
            col("timestamp").dt().truncate(lit("1w"))
        ])
        .agg([
            col("amount").sum().alias("total_spend"),
            col("amount").mean().alias("avg_spend"),
            col("amount").count().alias("transaction_count"),
        ])
        .sort(["timestamp", "total_spend"], SortMultipleOptions::default().with_order_descending(true)))
}
