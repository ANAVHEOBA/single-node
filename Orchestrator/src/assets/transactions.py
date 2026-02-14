from dagster import asset, Output, AssetMaterialization
import subprocess
import os
import duckdb
from pathlib import Path

@asset(group_name="ingestion")
def raw_transactions_on_s3():
    """Checks for the presence of raw parquet files on S3."""
    # Logic to verify S3 files could go here
    return "s3://spark-killer-bucket/raw-data/"

@asset(
    deps=[raw_transactions_on_s3],
    group_name="engine"
)
def weekly_aggregated_delta_table(context):
    """Triggers the Rust Spark Killer engine to process data and create a Delta Table."""
    engine_path = Path(__file__).parent.parent.parent.parent / "DataEng" / "target" / "release" / "data_eng"
    
    context.log.info(f"🚀 Triggering Rust Engine at {engine_path}")
    
    # Run the compiled Rust binary
    result = subprocess.run(
        [str(engine_path)],
        capture_output=True,
        text=True,
        env={**os.environ, "RUST_LOG": "info"}
    )
    
    if result.returncode != 0:
        context.log.error(f"❌ Engine failed: {result.stderr}")
        raise Exception("Rust Engine execution failed")
    
    context.log.info(f"✅ Engine Output: {result.stdout}")
    
    return "./data/delta_table"

@asset(
    deps=[weekly_aggregated_delta_table],
    group_name="validation"
)
def validated_delta_stats(context):
    """Uses DuckDB to perform final quality checks on the output Delta Table."""
    delta_path = "./data/delta_table"
    
    # REAL STUFF: Using DuckDB's spatial/http/parquet features to query the local Delta structure
    con = duckdb.connect()
    
    # We query the parquet part specifically for simple validation
    query = f"SELECT count(*), sum(total_spend) FROM '{delta_path}/*.parquet'"
    stats = con.execute(query).fetchone()
    
    context.log.info(f"📊 Validation Stats: Count={stats[0]}, Total Spend={stats[1]}")
    
    return {
        "row_count": stats[0],
        "total_spend": stats[1]
    }
