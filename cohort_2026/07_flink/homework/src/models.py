import json
from dataclasses import dataclass

import pandas as pd


@dataclass
class Ride:
    lpep_pickup_datetime: int
    lpep_dropoff_datetime: int
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=int(row["lpep_pickup_datetime"].timestamp() * 1000),
        lpep_dropoff_datetime=int(row["lpep_dropoff_datetime"].timestamp() * 1000),
        PULocationID=int(row["PULocationID"]),
        DOLocationID=int(row["DOLocationID"]),
        passenger_count=int(row["passenger_count"])
        if not pd.isna(row["passenger_count"])
        else 0,
        trip_distance=float(row["trip_distance"]),
        tip_amount=float(row["tip_amount"]),
        total_amount=float(row["total_amount"]),
    )


def ride_deserializer(data):
    json_str = data.decode("utf-8")
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
