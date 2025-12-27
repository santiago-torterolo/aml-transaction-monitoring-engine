# -*- coding: utf-8 -*-

import duckdb
import os
import sys

# Fix for Windows console encoding with emojis
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# ============================================================
# CONFIGURATION: Dynamic project paths
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_FILE = os.path.join(DATA_DIR, "paysim.csv")
DB_FILE = os.path.join(DATA_DIR, "fraud_data.duckdb")


def load_data():
    """
    Load PaySim dataset (6.3M transactions) into DuckDB.
    
    Process:
    1. Verify CSV file exists
    2. Connect to DuckDB (creates .duckdb file if doesn't exist)
    3. Bulk load CSV using read_csv_auto (optimized)
    4. Validate successful load
    """
    print(f"🚀 Starting ETL process (Extract, Transform, Load)...")
    
    # STEP 1: Verify CSV exists
    if not os.path.exists(CSV_FILE):
        print(f"CRITICAL ERROR: Data file not found.")
        print(f"   Expected location: {CSV_FILE}")
        print(f"\n ACTION REQUIRED:")
        print(f"   1. Go to: https://www.kaggle.com/datasets/ealaxi/paysim1")
        print(f"   2. Download the dataset")
        print(f"   3. Unzip and rename file to 'paysim.csv'")
        print(f"   4. Place it in '{DATA_DIR}/' folder")
        return

    # STEP 2: Connect to DuckDB
    print(f"🔌 Connecting to DuckDB database...")
    print(f"   Location: {DB_FILE}")
    conn = duckdb.connect(DB_FILE)
    
    # STEP 3: Bulk load (DuckDB reads CSV 10x faster than Pandas)
    print(f"Importing massive dataset from CSV...")
    print(f"   (This may take 10-30 seconds depending on your machine)")
    
    try:
        # read_csv_auto automatically detects data types
        query = f"""
            CREATE OR REPLACE TABLE transactions AS 
            SELECT * FROM read_csv_auto('{CSV_FILE}', header=True)
        """
        conn.execute(query)
        
        # STEP 4: Load validation
        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        print(f"\n SUCCESS! Database created successfully.")
        print(f"   Total transactions loaded: {count:,}")
        
        # STEP 5: Data preview
        print(f"\n Preview (First 3 transactions):")
        preview_df = conn.execute("SELECT * FROM transactions LIMIT 3").df()
        print(preview_df)
        
        # INFO: Show table schema
        print(f"\n Table 'transactions' structure:")
        schema = conn.execute("DESCRIBE transactions").df()
        print(schema)

    except Exception as e:
        print(f"\n An error occurred during load:")
        print(f"   {e}")
        print(f"\n Possible causes:")
        print(f"   - CSV file is corrupted")
        print(f"   - Insufficient disk space")
        print(f"   - Permission issues in data/ folder")
    
    finally:
        # ALWAYS close the connection
        conn.close()
        print(f"\n Database connection closed.")


if __name__ == "__main__":
    load_data()
