import pandas as pd
import re
from pathlib import Path
from prefect import flow, task
from prefect.filesystems import GCS

# from prefect_gcp.cloud_storage import GcsBucket

from random import randint
from prefect.tasks import task_input_hash
from prefect.blocks.notifications import SlackWebhook
from datetime import timedelta

slack_webhook_block = SlackWebhook.load("notifyslack")

def camel_to_snake(name:str):
    """Function to convert camel case columns to snake_case
    """
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and column names"""
    df.columns = [camel_to_snake(col) for col in df.columns]
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    # df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    # df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    print(path)
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=2, retry_delay_seconds=5)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    # gcs_block = GcsBucket.load("dtc-zoomcamp")
    gcs_block = GCS.load("dtc-zoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
 
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def flows_to_gcs(
    color: str  = "yellow", year: int = 2021, months: list[int] = [1, 2]

):
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    months = [3]
    year = 2019
    flows_to_gcs(color, year, months)
