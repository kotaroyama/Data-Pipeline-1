import argparse
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
import pandas as pd

from src.cleaner import DataCleaner
from src.database import DatabaseManager
from src.metrics import MetricCalculator

load_dotenv(find_dotenv())

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

def run_pipeline(
    data_path: str, db_url: str, chunksize: int = 100000, limit: int = None        
):
    start_time = time.time()
    logging.info(f"Starting Event Audit Pipeline for: {data_path}")

    # Initialize DB connection
    db = DatabaseManager(db_url)
    db.init_db()

    # Process Data in Chunks
    cleaner = DataCleaner()
    calculator = MetricCalculator()
    product_metrics = []
    session_metrics = []

    total_rows = 0
    logging.info("Processing chunks...")

    for chunk in cleaner.load_and_clean_chunks(
        data_path, chunksize=chunksize, limit=limit
    ):
        total_rows += len(chunk)

        # Compute metrics on cleaned chunk
        product_metrics.append(calculator.compute_product_metrics(chunk))
        session_metrics.append(calculator.compute_session_metrics(chunk))
        
        # Load results to database
        db.bulk_insert_events(chunk)

    # Regrouping
    product_metrics_df = pd.concat(product_metrics, ignore_index=True)
    product_metrics_df = (
        product_metrics_df.groupby("product_id")
        .agg(
            avg_conversion_time=("avg_conversion_time", "mean"),
            total_purchases=("total_purchases", "sum")
        )
        .reset_index()
    )
    session_metrics_df = pd.concat(session_metrics, ignore_index=True)
    diff = (
        session_metrics_df.groupby(by="user_session")["event_time"].max()
        - session_metrics_df.groupby(by="user_session")["event_time"].min()
    )
    session_metrics_df["time_to_conversion"] = session_metrics_df["user_session"].map(diff)

    # Insert the metrics into the database
    db.save_metrics(product_metrics_df, table_name="product_metrics")
    db.save_metrics(session_metrics_df, table_name="session_metrics")

    logging.info("Runnning SQL data integrity audit...")
    audit_results = db.run_audit_queries()

    elapsed = round(time.time() - start_time, 2)
    logging.info("=" * 50)
    logging.info(f"PIPELINE COMPLETE in {elapsed} seconds")
    logging.info(f"Total Rows Processed {total_rows:,}")
    logging.info(
        f"Conversion Rate: {audit_results.get("overall_conversion_rate", 0)}%"
    )
    logging.info("=" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Event Data Ingestion & Audit Pipeline"
    )
    parser.add_argument(
        "--data",
        type=str,
        default="data/2019-Oct.csv",
        help="Path to raw events CSV"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5432/eventdb",
        help="Database connection URI",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100000,
        help="Pandas chunksize for streaming",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500000,
        help="Max rows to process (for quick testing)",
    )

    args = parser.parse_args()
    run_pipeline(
        data_path=args.data,
        db_url=args.db,
        chunksize=args.chunksize,
        # limit=args.limit,
    )