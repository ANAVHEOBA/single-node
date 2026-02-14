use anyhow::Result;
use data_eng::config::AppConfig;
use data_eng::ingestion::scan_raw_data;
use data_eng::transformations::{aggregate_by_user_weekly, filter_invalid_transactions};
use data_eng::storage::write_to_delta;

fn main() -> Result<()> {
    // 1. Setup
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    log::info!("🚀 Spark Killer Engine starting (Pure Synchronous Mode)...");

    let config = AppConfig::from_env();
    let options = config.get_cloud_options();
    let input_path = format!("s3://{}/raw-data/*.parquet", config.bucket);
    let output_path = "./data/delta_table";

    // 2. Ingestion
    // scan_raw_data is already sync-compatible for graph construction
    let lf = scan_raw_data(&input_path, options)?;

    // 3. Transformations
    log::info!("⚡ Applying transformations...");
    let cleaned_lf = filter_invalid_transactions(lf);
    let aggregated_lf = aggregate_by_user_weekly(cleaned_lf)?;

    // 4. Materialization
    log::info!("💎 Materializing results (Executing Polars engine)...");
    let df = aggregated_lf.collect()?;

    // 5. Storage
    log::info!("💾 Writing to Delta Table (Sync)...");
    
    // We update the writer to be synchronous
    // We use a temporary local runtime strictly inside the writer if needed,
    // or just make the whole call chain sync.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        write_to_delta(df, output_path).await
    })?;

    log::info!("✅ Done! Spark successfully killed.");
    Ok(())
}
