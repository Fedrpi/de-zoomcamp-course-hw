import re

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

PATH = "./docker/pipelines/raw_data/"


def to_snake_case(name):
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    return name.lower()


def df_colums_snake_caser(dataframe):
    dataframe.columns = [to_snake_case(col) for col in dataframe.columns]
    return dataframe


# Create table for green_tripdata
df_green_trips = pd.read_parquet(f"{PATH}green_tripdata_2025-11.parquet")
df_green_trips = df_colums_snake_caser(df_green_trips)
df_green_trips.head(0).to_sql(
    name="green_trip_data", schema="ny_taxi", con=engine, if_exists="replace"
)

# Injest to green_tripdata

data_to_injest = pq.ParquetFile(f"{PATH}green_tripdata_2025-11.parquet")
total_rows = data_to_injest.metadata.num_rows

with tqdm(total=total_rows, desc="Loading to Postgres") as pbar:
    for batch in data_to_injest.iter_batches(batch_size=10_000):
        chunk = batch.to_pandas()
        chunk = df_colums_snake_caser(chunk)
        chunk.to_sql(
            name="green_trip_data", schema="ny_taxi", con=engine, if_exists="append"
        )
        pbar.update(len(chunk))

# Create table for taxi_zone_lookup

df_taxi_zone_lookup = pd.read_csv(filepath_or_buffer=f"{PATH}taxi_zone_lookup.csv")

# Injest data to taxi_zone_lookup

df_taxi_zone_lookup = df_colums_snake_caser(df_taxi_zone_lookup)
df_taxi_zone_lookup.to_sql(
    name="taxi_zone_lookup", schema="ny_taxi", con=engine, if_exists="replace"
)
