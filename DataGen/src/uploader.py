import boto3
import os
from botocore.client import Config
from tqdm import tqdm

def upload_to_localstack(bucket_name, data_dir, endpoint_url="http://localhost:4566"):
    """Uploads all files in a directory to LocalStack S3."""
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )
    
    # Create bucket if not exists
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return

    files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    print(f"Uploading {len(files)} files to {bucket_name}...")
    
    for filename in tqdm(files):
        file_path = os.path.join(data_dir, filename)
        s3.upload_file(file_path, bucket_name, f"raw-data/{filename}")

if __name__ == "__main__":
    import sys
    bucket = sys.argv[1] if len(sys.argv) > 1 else "spark-killer-bucket"
    data_directory = "DataGen/src/data"
    upload_to_localstack(bucket, data_directory)
