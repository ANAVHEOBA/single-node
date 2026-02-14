# DataGen Module

This module is responsible for generating synthetic transaction data and uploading it to a mock S3 bucket (LocalStack).

## Features
- **Chunked Generation:** Generates 100M+ rows in chunks to prevent memory issues.
- **Parquet Storage:** Uses the Parquet format for efficient storage and faster downstream processing.
- **S3 Upload:** Automated upload to a local S3 emulator.

## Quick Start

### 1. Start LocalStack
Ensure Docker is running and start LocalStack:
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Generate Data
Generate 1M rows for testing:
```bash
python src/generator.py 1000000
```
Generate the full 100M rows:
```bash
python src/generator.py 100000000
```

### 4. Upload to S3
```bash
python src/uploader.py spark-killer-bucket
```

## Data Schema
- `user_id`: Integer (1 - 1,000,000)
- `timestamp`: DateTime (within the year 2025)
- `amount`: Float (1.0 - 5000.0)
- `category`: Choice (Groceries, Tech, Entertainment, Travel, Health, Dining)
