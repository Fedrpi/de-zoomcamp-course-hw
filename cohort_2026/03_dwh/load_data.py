import os
import re
import requests
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
from tqdm import tqdm
from datetime import datetime

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
MINIO_ENDPOINT = "http://localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"
TEMP_DIR = "/tmp/nyc_taxi"


def to_snake_case(name: str) -> str:
    """Convert column name to snake_case"""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    s3 = s2.replace(" ", "_").replace("-", "_")
    return re.sub("_+", "_", s3).lower().strip("_")


def get_s3fs_client():
    """Create S3FS client for MinIO"""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def download_file(url: str, dest_path: str) -> bool:
    """Download file with progress bar"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with (
            open(dest_path, "wb") as f,
            tqdm(
                desc=f"Download",
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar,
        ):
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    size = f.write(chunk)
                    pbar.update(size)
        return True
    except Exception as e:
        print(f"   ❌ Download error: {e}")
        return False


def transform_and_save(input_path: str, output_path: str) -> bool:
    """Transform columns to snake_case and save Parquet"""
    try:
        # Read Parquet
        table = pq.read_table(input_path)

        # Get original columns
        original_columns = table.column_names
        print(f"   Original: {original_columns}")

        # Create mapping
        column_mapping = {col: to_snake_case(col) for col in original_columns}
        new_columns = [column_mapping[col] for col in original_columns]
        print(f"   Snake case: {new_columns}")

        # Rename columns
        table = table.rename_columns(new_columns)

        # Add metadata
        metadata = {
            b"source_url": BASE_URL.encode(),
            b"processed_at": datetime.now().isoformat().encode(),
        }
        table = table.replace_schema_metadata(metadata)

        # Save to output
        pq.write_table(table, output_path, compression="zstd")

        print(f"   ✅ Transformed: {len(new_columns)} columns, {table.num_rows:,} rows")
        return True

    except Exception as e:
        print(f"   ❌ Transform error: {e}")
        return False


def upload_to_minio(local_path: str, s3_path: str, s3_client) -> bool:
    """Upload file to MinIO"""
    try:
        s3_client.put(local_path, s3_path)
        print(f"   ✅ Uploaded to {s3_path}")
        return True
    except Exception as e:
        print(f"   ❌ Upload error: {e}")
        return False


def process_file(file_name: str, s3_client) -> bool:
    """Process single file: download, transform, upload"""
    file_url = f"{BASE_URL}{file_name}"
    temp_input = os.path.join(TEMP_DIR, f"input_{file_name}")
    temp_output = os.path.join(TEMP_DIR, f"output_{file_name}")
    s3_path = f"{BUCKET_NAME}/{file_name}"

    print(f"\n{'=' * 70}")
    print(f"Processing: {file_name}")
    print(f"{'=' * 70}")

    # Check if already exists in MinIO
    try:
        if s3_client.exists(s3_path):
            print(f"   ℹ️  File already exists in MinIO, skipping...")
            return True
    except:
        pass

    # Download
    print(f"   📥 Downloading from {file_url}")
    if not download_file(file_url, temp_input):
        return False

    # Transform
    print(f"   🔄 Transforming columns to snake_case...")
    if not transform_and_save(temp_input, temp_output):
        os.remove(temp_input)
        return False

    # Upload
    print(f"   📤 Uploading to MinIO...")
    if not upload_to_minio(temp_output, s3_path, s3_client):
        os.remove(temp_input)
        os.remove(temp_output)
        return False

    # Cleanup
    os.remove(temp_input)
    os.remove(temp_output)

    return True


def main():
    print("\n" + "=" * 70)
    print("NYC TAXI ETL PIPELINE - 2024")
    print("=" * 70)
    print(f"Source: {BASE_URL}")
    print(f"Destination: MinIO ({MINIO_ENDPOINT}) bucket '{BUCKET_NAME}'")
    print(f"Transformation: Column names → snake_case")
    print("=" * 70 + "\n")

    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Connect to MinIO
    print("Connecting to MinIO...")
    s3_client = get_s3fs_client()

    # Check bucket
    try:
        if not s3_client.exists(BUCKET_NAME):
            print(f"Creating bucket '{BUCKET_NAME}'...")
            s3_client.mkdir(BUCKET_NAME)
        print(f"✅ Connected to MinIO\n")
    except Exception as e:
        print(f"❌ Cannot connect to MinIO: {e}")
        return

    # List files to process
    files = [f"yellow_tripdata_2024-{month:02d}.parquet" for month in range(1, 13)]

    print(f"Files to process ({len(files)} total):")
    for i, f in enumerate(files, 1):
        print(f"  {i}. {f}")
    print()

    # Process each file
    success_count = 0
    failed_count = 0
    failed_files = []

    for i, file_name in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] ", end="")

        if process_file(file_name, s3_client):
            success_count += 1
        else:
            failed_count += 1
            failed_files.append(file_name)

    # Summary
    print(f"\n\n{'=' * 70}")
    print("ETL PIPELINE SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total files: {len(files)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")

    if failed_files:
        print(f"\nFailed files:")
        for f in failed_files:
            print(f"  - {f}")

    print(f"{'=' * 70}\n")

    # List final contents
    print("Files in MinIO bucket 'raw':")
    try:
        for obj in s3_client.ls(f"{BUCKET_NAME}/"):
            print(f"  - {obj}")
    except Exception as e:
        print(f"  Error listing: {e}")


if __name__ == "__main__":
    main()
