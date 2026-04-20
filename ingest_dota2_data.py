import os
import sys

import kagglehub
from google.cloud import storage


BUCKET_NAME = "de-dota2"
DATASET = "bwandowando/dota-2-pro-league-matches-2023"
TARGET_FILE = "main_metadata.csv"
GCS_DEST_DIR = "raw"
CHUNK_SIZE = 8 * 1024 * 1024
MIN_YEAR = 2022
MAX_YEAR = 2025


def get_gcs_client():
    creds_file = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", "credentials/gcs_service_account.json"
    )
    if os.path.exists(creds_file):
        print(f"Using credentials file: {creds_file}")
        return storage.Client.from_service_account_json(creds_file)
    print("Using Application Default Credentials (ADC)")
    return storage.Client()


def upload_to_gcs(client, local_path, blob_name):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    print(f"  Uploading -> gs://{BUCKET_NAME}/{blob_name}")
    blob.upload_from_filename(local_path)

    if blob.exists():
        print(f"  Upload verified.")
    else:
        print(f"  WARNING: verification failed for {blob_name}")


def download_file(year):
    """Download a single main_metadata.csv for the given year via kagglehub."""
    file_path = f"{year}/{TARGET_FILE}"
    print(f"  Downloading {file_path}...")
    local_path = kagglehub.dataset_download(DATASET, path=file_path)
    return local_path


def main():
    client = get_gcs_client()

    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        print(f"ERROR: Bucket '{BUCKET_NAME}' does not exist.")
        sys.exit(1)
    print(f"Bucket '{BUCKET_NAME}' exists.\n")

    uploaded = 0
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        local_path = download_file(year)
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"  Downloaded: {year}/{TARGET_FILE} ({size_mb:.1f} MB)")

        blob_name = f"{GCS_DEST_DIR}/{year}/{TARGET_FILE}"
        upload_to_gcs(client, local_path, blob_name)
        uploaded += 1

    print(f"\nDone! Uploaded {uploaded} files to gs://{BUCKET_NAME}/{GCS_DEST_DIR}/")


if __name__ == "__main__":
    main()
