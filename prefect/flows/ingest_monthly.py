"""Prefect flow: Ingest monthly Dota 2 match data (2026+) from Kaggle to GCS.

Downloads individual files via kagglehub (no full archive needed),
uploads to GCS.

Designed to run on a monthly schedule.
"""

import os
import sys
from datetime import datetime

import kagglehub
from google.cloud import storage
from prefect import flow, task, get_run_logger


BUCKET_NAME = "de-dota2"
DATASET = "bwandowando/dota-2-pro-league-matches-2023"
TARGET_FILE = "main_metadata.csv"
CONSTANTS_LEAGUES = "Constants/Constants.Leagues.csv"
GCS_LEAGUES_BLOB = "raw/Constant/Leagues.csv"
GCS_DEST_DIR = "raw"
CHUNK_SIZE = 8 * 1024 * 1024


def get_gcs_client():
    creds_file = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", "credentials/gcs_service_account.json"
    )
    if os.path.exists(creds_file):
        return storage.Client.from_service_account_json(creds_file)
    return storage.Client()


@task(retries=2, retry_delay_seconds=30)
def download_file(file_path: str) -> str:
    """Download a single file from Kaggle via kagglehub. Returns the local path."""
    logger = get_run_logger()
    logger.info(f"Downloading {file_path}...")
    local_path = kagglehub.dataset_download(DATASET, path=file_path)
    logger.info(f"Downloaded: {local_path}")
    return local_path


@task(retries=2, retry_delay_seconds=10)
def upload_to_gcs(local_path: str, blob_name: str):
    """Upload a file to GCS and verify."""
    logger = get_run_logger()
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info(f"Uploading {blob_name} ({size_mb:.1f} MB)...")
    blob.upload_from_filename(local_path)

    if blob.exists():
        logger.info(f"Upload verified: gs://{BUCKET_NAME}/{blob_name}")
    else:
        raise RuntimeError(f"Upload verification failed for {blob_name}")


@flow(name="ingest-dota2-monthly", log_prints=True)
def ingest_monthly(year: int = 0, month: int = 0):
    """Ingest a single month of Dota 2 data from Kaggle to GCS.

    If year/month are not provided, defaults to the previous month.
    """
    logger = get_run_logger()

    if year == 0 or month == 0:
        now = datetime.now()
        if now.month == 1:
            year, month = now.year - 1, 12
        else:
            year, month = now.year, now.month - 1

    yyyymm = f"{year}{month:02d}"
    logger.info(f"Target month: {yyyymm}")

    file_path = f"{yyyymm}/{TARGET_FILE}"
    local_path = download_file(file_path)
    blob_name = f"{GCS_DEST_DIR}/{file_path}"
    upload_to_gcs(local_path, blob_name)

    # Try to download and replace Constants/Leagues.csv (may 404 for some datasets)
    try:
        leagues_local = download_file(CONSTANTS_LEAGUES)
        upload_to_gcs(leagues_local, GCS_LEAGUES_BLOB)
    except Exception as e:
        logger.warning(f"Could not download {CONSTANTS_LEAGUES}: {e}")

    logger.info(f"Done! Uploaded {yyyymm}/{TARGET_FILE} to gs://{BUCKET_NAME}/{GCS_DEST_DIR}/")


@flow(name="backfill-dota2-monthly", log_prints=True)
def backfill(start_year: int = 2026, start_month: int = 1, end_year: int = 0, end_month: int = 0):
    """Backfill multiple months."""
    logger = get_run_logger()

    if end_year == 0 or end_month == 0:
        now = datetime.now()
        if now.month == 1:
            end_year, end_month = now.year - 1, 12
        else:
            end_year, end_month = now.year, now.month - 1

    # Build target month list
    target_months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        target_months.append(f"{y}{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    logger.info(f"Backfilling {len(target_months)} months: {target_months}")

    uploaded = 0
    for yyyymm in target_months:
        file_path = f"{yyyymm}/{TARGET_FILE}"
        try:
            local_path = download_file(file_path)
            blob_name = f"{GCS_DEST_DIR}/{file_path}"
            upload_to_gcs(local_path, blob_name)
            uploaded += 1
        except Exception as e:
            logger.warning(f"Skipping {yyyymm}: {e}")

    # Try to download and replace Constants/Leagues.csv
    try:
        leagues_local = download_file(CONSTANTS_LEAGUES)
        upload_to_gcs(leagues_local, GCS_LEAGUES_BLOB)
    except Exception as e:
        logger.warning(f"Could not download {CONSTANTS_LEAGUES}: {e}")

    logger.info(f"Backfill complete! Uploaded {uploaded}/{len(target_months)} file(s).")


@flow(name="ingest-constants-leagues", log_prints=True)
def ingest_leagues():
    """Download Constants/Leagues.csv from Kaggle and upload to GCS."""
    logger = get_run_logger()

    try:
        leagues_local = download_file(CONSTANTS_LEAGUES)
        upload_to_gcs(leagues_local, GCS_LEAGUES_BLOB)
        logger.info(f"Done! Uploaded to gs://{BUCKET_NAME}/{GCS_LEAGUES_BLOB}")
    except Exception as e:
        logger.error(f"Could not download {CONSTANTS_LEAGUES}: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        backfill()
    elif len(sys.argv) > 1 and sys.argv[1] == "leagues":
        ingest_leagues()
    elif len(sys.argv) == 3:
        ingest_monthly(year=int(sys.argv[1]), month=int(sys.argv[2]))
    else:
        ingest_monthly()
