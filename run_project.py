"""
Single-Click Launcher for ATM Cash Demand Forecasting & Replenishment System
BDA Project Statement 47
Runs end-to-end Big Data Analytics pipeline:
1. Synthetic Kaggle ATM dataset verification / generation
2. Hadoop HDFS partition initialization
3. Time-Series ML forecasting model training
4. Kafka streaming pipeline
5. Interactive Web Replenishment Dashboard
"""

import os
import sys
import time
import webbrowser
import uvicorn

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main():
    print("=" * 75)
    print("  VAULTPULSE AI • SMART ATM CASH DEMAND & REPLENISHMENT PLATFORM")
    print("  Big Data Analytics Pipeline: Kafka | Hadoop HDFS | Time-Series ML")
    print("=" * 75)

    # Step 1: Check / Generate Dataset
    data_dir = os.path.join(PROJECT_ROOT, "data")
    raw_data_file = os.path.join(data_dir, "raw_synthetic_atm_transactions.csv")
    if not os.path.exists(raw_data_file):
        print("\n[Step 1/4] Generating Kaggle-style synthetic ATM transaction dataset...")
        from data.generate_dataset import generate_synthetic_data
        generate_synthetic_data(num_days=90)
    else:
        print("\n[Step 1/4] Synthetic ATM dataset verified.")

    # Step 2: Check HDFS partitions
    from hdfs_storage.hdfs_client import hdfs_client
    hdfs_status = hdfs_client.get_hdfs_status()
    if hdfs_status["total_files"] == 0:
        print("\n[Step 2/4] Partitioning historical transactions into Hadoop HDFS...")
        from hdfs_storage.ingest_historical import ingest_to_hdfs
        ingest_to_hdfs()
    else:
        print(f"\n[Step 2/4] Hadoop HDFS storage verified ({hdfs_status['partitions_count']} partitions, {hdfs_status['total_size_kb']} KB).")

    # Step 3: Check / Train Forecasting Model
    model_file = os.path.join(PROJECT_ROOT, "forecasting", "saved_models", "atm_forecast_model.pkl")
    if not os.path.exists(model_file):
        print("\n[Step 3/4] Training time-series forecasting model on HDFS data...")
        from forecasting.train_model import train_forecasting_model
        train_forecasting_model()
    else:
        print("\n[Step 3/4] Time-Series forecasting model verified.")

    # Step 4: Launch Web Dashboard
    host = "127.0.0.1"
    port = 8000
    url = f"http://{host}:{port}"
    print("\n[Step 4/4] Starting Replenishment Dashboard & Kafka stream...")
    print(f"\n🚀 System is LIVE at: {url}")
    print("   Press CTRL+C anytime to stop.\n")

    # Try opening browser after a short delay
    def open_browser():
        time.sleep(1.5)
        try:
            webbrowser.open(url)
        except Exception:
            pass

    import threading
    threading.Thread(target=open_browser, daemon=True).start()

    # Run FastAPI app with Uvicorn
    uvicorn.run("dashboard.app:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
