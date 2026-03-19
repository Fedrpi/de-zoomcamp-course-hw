import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.getcwd(), ".."))

from kafka import KafkaConsumer

from models import ride_deserializer

server = "localhost:9092"
topic_name = "green-trips"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset="latest",
    group_id="green-trips-console",
    value_deserializer=ride_deserializer,
)

print(f"Listening to {topic_name}...")
trip_distance_gt_five = 0
count = 0
for message in consumer:
    ride = message.value

    print(
        f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
        f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
    )
    count += 1
    if ride.trip_distance > 5:
        trip_distance_gt_five += 1
        print("")
        print(f"rides with trip_distance > 5 is {trip_distance_gt_five}")
        print("")

consumer.close()
