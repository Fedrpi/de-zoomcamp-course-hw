"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup datetime (unified from tpep/lpep)"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff datetime (unified from tpep/lpep)"
  - name: VendorID
    type: integer
    description: "TPEP/LPEP provider ID"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: RatecodeID
    type: float
    description: "Rate code ID"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"
  - name: PULocationID
    type: integer
    description: "Pickup TLC Taxi Zone ID"
  - name: DOLocationID
    type: integer
    description: "Dropoff TLC Taxi Zone ID"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: fare_amount
    type: float
    description: "Metered fare amount"
  - name: extra
    type: float
    description: "Extra charges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge"
  - name: total_amount
    type: float
    description: "Total amount"
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted"

@bruin"""

import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Map taxi type → raw pickup/dropoff column names in the parquet files
PICKUP_COL = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
}
DROPOFF_COL = {
    "yellow": "tpep_dropoff_datetime",
    "green": "lpep_dropoff_datetime",
}


def _generate_months(start_date: str, end_date: str):
    """Yield (year, month) tuples for each month in [start_date, end_date)."""
    current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    while current < end:
        yield current.year, current.month
        current += relativedelta(months=1)


def _fetch_and_normalize(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    """Download one parquet file and unify pickup/dropoff column names."""
    url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    print(f"Fetching {url}")

    response = requests.get(url, timeout=300)
    response.raise_for_status()

    df = pd.read_parquet(io.BytesIO(response.content))

    # Rename taxi-type-specific datetime columns to unified names
    df = df.rename(columns={
        PICKUP_COL[taxi_type]: "pickup_datetime",
        DROPOFF_COL[taxi_type]: "dropoff_datetime",
    })

    df["taxi_type"] = taxi_type
    df["extracted_at"] = datetime.now(timezone.utc)

    return df


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    frames = []
    for taxi_type in taxi_types:
        for year, month in _generate_months(start_date, end_date):
            df = _fetch_and_normalize(taxi_type, year, month)
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
