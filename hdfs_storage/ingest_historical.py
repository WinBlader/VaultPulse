"""
Ingest Historical ATM Transactions into HDFS Partition Layout
Reads the synthetic Kaggle dataset and partitions it into HDFS standard date-based partitions.
"""

import os
import pandas as pd
from hdfs_storage.hdfs_client import hdfs_client

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
RAW_TXN_FILE = os.path.join(DATA_DIR, "raw_synthetic_atm_transactions.csv")


def ingest_to_hdfs(batch_days_chunk: int = 5):
    if not os.path.exists(RAW_TXN_FILE):
        print("Raw transaction dataset not found. Generating first...")
        from data.generate_dataset import generate_synthetic_data
        generate_synthetic_data(num_days=90)

    print("Loading raw transactions for HDFS ingestion...")
    df = pd.read_csv(RAW_TXN_FILE)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")
    
    unique_dates = df["date"].unique()
    print(f"Found {len(df):,} transactions across {len(unique_dates)} dates. Partitioning into HDFS...")

    for i, date_val in enumerate(unique_dates):
        day_txns = df[df["date"] == date_val].drop(columns=["date"]).to_dict(orient="records")
        hdfs_path = hdfs_client.write_transaction_batch(day_txns)
        if (i + 1) % 15 == 0 or (i + 1) == len(unique_dates):
            print(f"[{i+1}/{len(unique_dates)}] Ingested partition -> {hdfs_path}")

    print("HDFS Ingestion Complete!")
    status = hdfs_client.get_hdfs_status()
    print(f"HDFS Partitions: {status['partitions_count']}, Total Files: {status['total_files']}, Storage Size: {status['total_size_kb']} KB")


if __name__ == "__main__":
    ingest_to_hdfs()
