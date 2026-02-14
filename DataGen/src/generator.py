import pandas as pd
import numpy as np
import uuid
import os
from datetime import datetime, timedelta
from tqdm import tqdm

def generate_chunk(chunk_id, rows_per_chunk, start_date):
    """Generates a single chunk of transaction data."""
    categories = ['Groceries', 'Tech', 'Entertainment', 'Travel', 'Health', 'Dining']
    
    data = {
        'user_id': np.random.randint(1, 1000000, size=rows_per_chunk),
        'timestamp': [start_date + timedelta(seconds=np.random.randint(0, 31536000)) for _ in range(rows_per_chunk)],
        'amount': np.random.uniform(1.0, 5000.0, size=rows_per_chunk).round(2),
        'category': np.random.choice(categories, size=rows_per_chunk)
    }
    
    df = pd.DataFrame(data)
    filename = f"transactions_chunk_{chunk_id}.parquet"
    df.to_parquet(filename, index=False)
    return filename

def main(total_rows=100_000_000, chunk_size=1_000_000):
    """Main execution to generate 100M rows in chunks."""
    if total_rows < chunk_size:
        chunk_size = total_rows
        num_chunks = 1
    else:
        num_chunks = total_rows // chunk_size
        
    start_date = datetime(2025, 1, 1)
    
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "data")
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Generating {total_rows} rows in {num_chunks} chunks...")
    for i in tqdm(range(num_chunks)):
        # Generate and save chunk
        categories = ['Groceries', 'Tech', 'Entertainment', 'Travel', 'Health', 'Dining']
        data = {
            'user_id': np.random.randint(1, 1000000, size=chunk_size),
            'timestamp': [start_date + timedelta(seconds=np.random.randint(0, 31536000)) for _ in range(chunk_size)],
            'amount': np.random.uniform(1.0, 5000.0, size=chunk_size).round(2),
            'category': np.random.choice(categories, size=chunk_size)
        }
        df = pd.DataFrame(data)
        filename = os.path.join(output_dir, f"transactions_chunk_{i}.parquet")
        df.to_parquet(filename, index=False)

if __name__ == "__main__":
    # For testing, we might want to start with a smaller number
    # total_rows = 100_000_000 
    # Let's default to a smaller test set and let the user override
    import sys
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    main(total_rows=rows)
