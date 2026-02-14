use polars::prelude::*;

pub fn filter_invalid_transactions(lf: LazyFrame) -> LazyFrame {
    lf.filter(
        col("user_id").is_not_null()
        .and(col("amount").is_not_null())
        .and(col("amount").gt(0.0))
    )
}
