

from google.cloud import storage
from google.cloud.exceptions import NotFound, Conflict
import pandas as pd
import json
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ID = "analytics-warehouse-dev-major"
BUCKET_NAME = "analytics-warehouse-data-lake"



def create_bucket_if_not_exists(
    bucket_name: str,
    location: str = "asia-south1"
) -> storage.Bucket:
    """
    Create a GCS bucket if it does not already exist.

    Why idempotent creation:
    Pipelines run daily. The bucket creation step runs every time.
    Without idempotency, second run fails with BucketAlreadyExists.
    This pattern makes the function safe to run multiple times.

    Why asia-south1:
    Mumbai region. Closest to Indian users.
    Data residency compliance for Indian companies.
    Lower latency for Indian BigQuery datasets.
    """
    client = storage.Client(project=PROJECT_ID)

    try:
        bucket = client.create_bucket(
            bucket_name,
            location=location
        )
        logger.info(f"Created bucket: gs://{bucket_name} in {location}")
        return bucket

    except Conflict:
        # Bucket already exists — this is fine
        bucket = client.bucket(bucket_name)
        logger.info(f"Bucket already exists: gs://{bucket_name}")
        return bucket




def upload_file(
    local_path: str,
    bucket_name: str,
    destination_path: str
) -> str:
    """
    Upload a local file to GCS.

    Why structured destination paths:
    gs://bucket/raw/orders/year=2024/month=01/day=05/orders.parquet

    This is called Hive partitioning.
    Spark and BigQuery automatically recognize this pattern.
    When you filter by date, they scan only matching partitions.
    95% cost reduction on large datasets.

    Returns the full GCS URI for use in downstream tasks.
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)

    blob.upload_from_filename(local_path)

    gcs_uri = f"gs://{bucket_name}/{destination_path}"
    logger.info(f"Uploaded {local_path} → {gcs_uri}")
    return gcs_uri


def upload_dataframe_as_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    destination_path: str
) -> str:
    """
    Upload a pandas DataFrame directly to GCS as Parquet.

    Why write DataFrame directly without saving locally first:
    Saves disk I/O on the pipeline server.
    Pipeline servers often have limited disk space.
    Direct upload is faster and cleaner.

    Why Parquet not CSV:
    Parquet is the standard format for data lakes.
    Columnar storage, compression, schema embedded.
    Every downstream tool (Spark, BigQuery, Athena) reads it natively.
    """
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Write DataFrame to in-memory buffer
    # Why buffer not file: avoid writing to disk
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)

    blob.upload_from_file(buffer, content_type='application/octet-stream')

    gcs_uri = f"gs://{bucket_name}/{destination_path}"
    file_size_mb = buffer.tell() / (1024 * 1024)
    logger.info(f"Uploaded DataFrame ({len(df)} rows, {file_size_mb:.2f}MB) → {gcs_uri}")
    return gcs_uri



def download_file(
    bucket_name: str,
    source_path: str,
    local_path: str
) -> str:
    """
    Download a file from GCS to local disk.

    When to download vs read directly:
    Download: when you need to process with a local tool
              or when you need the file multiple times
    Read directly: when you process once and discard
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_path)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob.download_to_filename(local_path)

    logger.info(f"Downloaded gs://{bucket_name}/{source_path} → {local_path}")
    return local_path


def read_parquet_from_gcs(
    bucket_name: str,
    prefix: str
) -> pd.DataFrame:
    """
    Read all Parquet files under a prefix into a DataFrame.

    Why read all files under a prefix:
    In production, data is split into many files by date.
    orders/year=2024/month=01/ contains 31 files (one per day).
    This function reads all of them into one DataFrame.

    This is the Python equivalent of:
    SELECT * FROM orders WHERE year=2024 AND month=01
    """
    import io
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]

    if not parquet_blobs:
        logger.warning(f"No Parquet files found at gs://{bucket_name}/{prefix}")
        return pd.DataFrame()

    dfs = []
    for blob in parquet_blobs:
        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        dfs.append(pd.read_parquet(buffer))

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(
        f"Read {len(parquet_blobs)} Parquet files "
        f"from gs://{bucket_name}/{prefix} "
        f"→ {len(combined)} total rows"
    )
    return combined




def list_files(
    bucket_name: str,
    prefix: str = "",
    suffix: str = ""
) -> List[str]:
    """
    List all files in a bucket under a prefix.

    Why this matters in pipelines:
    Before processing you need to know which files exist.
    Incremental processing: which files arrived since last run?
    Validation: did yesterday's files land correctly?
    """
    client = storage.Client(project=PROJECT_ID)
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    files = [
        blob.name for blob in blobs
        if not suffix or blob.name.endswith(suffix)
    ]

    logger.info(f"Found {len(files)} files at gs://{bucket_name}/{prefix}")
    return files


def file_exists(bucket_name: str, path: str) -> bool:
    """
    Check if a specific file exists in GCS.

    Why: idempotent pipelines check before processing.
    If output file already exists, skip processing.
    Prevents reprocessing data that was already handled.

    This is the cloud storage equivalent of:
    if os.path.exists(local_path): skip
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    return blob.exists()




def write_partitioned_data(
    df: pd.DataFrame,
    bucket_name: str,
    table_name: str,
    partition_cols: List[str]
) -> List[str]:
    """
    Write a DataFrame to GCS partitioned by specified columns.

    This is the most important cloud storage pattern in data engineering.

    Why partition:
    Without partitioning — to get January orders you scan all orders.
    With partitioning — you scan only the January folder.
    At 1 billion rows this saves 90%+ of processing cost.

    Output structure:
    gs://bucket/orders/year=2024/month=01/day=05/part-000.parquet
                       ─────────── ────────── ───────── ──────────
                       partition   partition   partition  data file

    Spark and BigQuery read this pattern natively.
    """
    uploaded_uris = []


    partitions = df.groupby(partition_cols)

    for partition_values, partition_df in partitions:
        # Build partition path
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)

        partition_path = "/".join([
            f"{col}={val}"
            for col, val in zip(partition_cols, partition_values)
        ])

        destination = (
            f"{table_name}/"
            f"{partition_path}/"
            f"part-000.parquet"
        )

        uri = upload_dataframe_as_parquet(
            partition_df,
            bucket_name,
            destination
        )
        uploaded_uris.append(uri)

    logger.info(
        f"Wrote {len(df)} rows across "
        f"{len(uploaded_uris)} partitions to gs://{bucket_name}/{table_name}/"
    )
    return uploaded_uris




def get_unprocessed_files(
    source_bucket: str,
    source_prefix: str,
    processed_bucket: str,
    processed_prefix: str
) -> List[str]:
    """
    Find files in source that have not been processed yet.

    This is the incremental processing pattern.
    Every production pipeline uses some version of this.

    Logic:
    1. List all files in source (raw data)
    2. List all files in processed (already done)
    3. Return files in source but not in processed

    DSA concept: Set difference — same pattern as
    LeetCode 'Find the Difference' problems.
    """
    source_files = set(list_files(source_bucket, source_prefix, '.parquet'))
    processed_files = set(list_files(processed_bucket, processed_prefix, '.parquet'))

    source_names = {os.path.basename(f) for f in source_files}
    processed_names = {os.path.basename(f) for f in processed_files}

    unprocessed_names = source_names - processed_names

    unprocessed = [
        f for f in source_files
        if os.path.basename(f) in unprocessed_names
    ]

    logger.info(
        f"Source files: {len(source_files)}, "
        f"Processed: {len(processed_files)}, "
        f"Remaining: {len(unprocessed)}"
    )
    return unprocessed




class LocalStorageSimulator:
    """
    Simulates GCS behavior using local filesystem.

    Why this class exists:
    GCS costs money when you write/read data.
    During development you want to test logic without cloud costs.
    This class has the same interface as GCS operations
    but writes to local disk instead.

    This is the Mock Object pattern — same interface, different backend.
    In interviews: shows you understand testing and abstraction.
    """

    def __init__(self, base_path: str = "local_storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        logger.info(f"LocalStorageSimulator initialized at: {self.base_path}")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        bucket: str,
        path: str
    ) -> str:
        """Write DataFrame to local Parquet file"""
        full_path = self.base_path / bucket / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(full_path, index=False, compression='snappy')
        uri = f"local://{bucket}/{path}"
        logger.info(f"Saved {len(df)} rows → {full_path}")
        return uri

    def read_dataframe(self, bucket: str, path: str) -> pd.DataFrame:
        """Read Parquet file from local storage"""
        full_path = self.base_path / bucket / path
        df = pd.read_parquet(full_path)
        logger.info(f"Read {len(df)} rows from {full_path}")
        return df

    def list_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List files in local storage"""
        search_path = self.base_path / bucket / prefix
        if not search_path.exists():
            return []
        files = [
            str(p.relative_to(self.base_path / bucket))
            for p in search_path.rglob("*.parquet")
        ]
        return files

    def file_exists(self, bucket: str, path: str) -> bool:
        """Check if file exists in local storage"""
        return (self.base_path / bucket / path).exists()




def demo_without_cloud():
    """
    Demonstrates all storage patterns using local simulator.
    No GCS credentials needed.
    """
    logger.info("=== CLOUD STORAGE DEMO (Local Simulation) ===\n")

    storage = LocalStorageSimulator("demo_storage")

    # Create sample orders data
    import numpy as np

    np.random.seed(42)
    n = 1000

    orders = pd.DataFrame({
        'order_id': [f'ORD-{i:06d}' for i in range(n)],
        'customer_id': [f'CUST-{np.random.randint(1,100):04d}' for _ in range(n)],
        'seller_id': [f'SELL-{np.random.randint(1,50):04d}' for _ in range(n)],
        'amount': np.random.uniform(100, 5000, n).round(2),
        'city': np.random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata'], n),
        'status': np.random.choice(['delivered', 'pending', 'returned'], n, p=[0.7, 0.2, 0.1]),
        'year': 2024,
        'month': np.random.randint(1, 4, n),
        'day': np.random.randint(1, 29, n)
    })

    logger.info(f"Generated {len(orders)} sample orders\n")

    # Pattern 1: Write partitioned data
    logger.info("--- PATTERN 1: Partitioned Write ---")
    partitions_written = 0
    for (year, month), group in orders.groupby(['year', 'month']):
        path = f"orders/year={year}/month={month:02d}/part-000.parquet"
        storage.upload_dataframe(group, "raw-data", path)
        partitions_written += 1
    logger.info(f"Written to {partitions_written} partitions\n")

    # Pattern 2: List files
    logger.info("--- PATTERN 2: List Files ---")
    files = storage.list_files("raw-data", "orders/")
    logger.info(f"Found {len(files)} partition files")
    for f in files[:3]:
        logger.info(f"  {f}")
    logger.info("")

    # Pattern 3: Incremental processing
    logger.info("--- PATTERN 3: Incremental Processing ---")
    processed_files = set()
    all_files = storage.list_files("raw-data", "orders/")

    for f in all_files:
        filename = os.path.basename(f)
        if filename not in processed_files:
            logger.info(f"Processing new file: {filename}")
            processed_files.add(filename)

    logger.info(f"Processed {len(processed_files)} new files\n")

    # Pattern 4: Read and aggregate
    logger.info("--- PATTERN 4: Read and Aggregate ---")
    all_dfs = []
    for f in storage.list_files("raw-data", "orders/"):
        df = storage.read_dataframe("raw-data", f)
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total rows read: {len(combined)}")

    revenue_by_city = (combined
        .groupby('city')['amount']
        .agg(['sum', 'count', 'mean'])
        .round(2)
        .sort_values('sum', ascending=False)
    )
    logger.info("\nRevenue by City:")
    logger.info(revenue_by_city.to_string())

    # Pattern 5: Write processed output
    logger.info("\n--- PATTERN 5: Write Processed Output ---")
    storage.upload_dataframe(
        revenue_by_city.reset_index(),
        "processed-data",
        "gold/city_revenue/part-000.parquet"
    )
    logger.info("Gold layer written successfully")

    logger.info("\n=== DEMO COMPLETE ===")
    logger.info("In production: replace LocalStorageSimulator with GCS client")
    logger.info("Interface is identical — only credentials and bucket names change")


if __name__ == "__main__":
    demo_without_cloud()