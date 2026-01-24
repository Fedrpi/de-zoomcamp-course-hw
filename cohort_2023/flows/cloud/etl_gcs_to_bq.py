from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtc-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True)
def write_bq(path: Path) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    df.to_gbq(
        destination_table="dtczoomcamp.green_rides",
        project_id="disco-bedrock-375516",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color:str, year: int, month:int):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    write_bq(path)

@flow()
def flows_gcs_to_bq(color: str, year: int, months: list[int]):
    """Parent ETL flow"""
    for month in months:
        etl_gcs_to_bq(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1,2,3]
    flows_gcs_to_bq(color, year, months)
