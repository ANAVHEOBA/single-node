from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus
import boto3
import os

@sensor(
    asset_selection="raw_transactions_on_s3",
    default_status=DefaultSensorStatus.STOPPED
)
def s3_raw_data_sensor(context: SensorEvaluationContext):
    """Monitors S3 for new parquet files to trigger processing."""
    bucket_name = "spark-killer-bucket"
    endpoint_url = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw-data/")
        if 'Contents' in response:
            # We found files, trigger the run
            # In a production setup, we'd track 'LastModified' or specific keys
            context.log.info("📡 Data detected on S3! Triggering Spark Killer...")
            return RunRequest(run_key="new_data_detected")
    except Exception as e:
        context.log.error(f"❌ Sensor check failed: {e}")
        
    return None
