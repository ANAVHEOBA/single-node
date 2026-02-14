# Orchestrator Module (Dagster)

This module manages the end-to-end orchestration of the Spark Killer pipeline.

## Features
- **Asset-Based Dataflow:** Tracks lineage from S3 raw data to validated Delta Table stats.
- **S3 Data Sensor:** Automatically triggers processing when fresh files appear in LocalStack.
- **Engine Integration:** Seamlessly executes the high-performance Rust processing engine.
- **ACID Validation:** Uses DuckDB to verify the integrity of the output Delta Table.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Dagster Webserver
```bash
dagster dev -f src/definitions.py
```

### 3. Usage
- Navigate to `localhost:3000` to view the asset graph.
- Activate the `s3_raw_data_sensor` to automate triggers.
- Manually materialize the `validated_delta_stats` asset to run the full pipeline.

## Modular Structure
- `src/assets/`: Core data entities and processing logic.
- `src/sensors/`: Automated trigger mechanisms.
- `src/definitions.py`: Central Dagster registry.
