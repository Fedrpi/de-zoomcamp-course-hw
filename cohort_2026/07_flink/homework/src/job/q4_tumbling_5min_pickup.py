"""
Q4: 5-minute tumbling window — count trips per PULocationID.

Run:
    docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_5min_pickup.py

Query result:
    SELECT PULocationID, num_trips
    FROM pickup_trips_5min
    ORDER BY num_trips DESC
    LIMIT 3;
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_source(t_env):
    table_name = "green_trips_q4"
    t_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime  BIGINT,
            lpep_dropoff_datetime BIGINT,
            PULocationID     INTEGER,
            DOLocationID     INTEGER,
            passenger_count  INTEGER,
            trip_distance    DOUBLE,
            tip_amount       DOUBLE,
            total_amount     DOUBLE,
            event_timestamp AS TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips-streaming',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """)
    return table_name


def create_sink(t_env):
    table_name = "pickup_trips_5min"
    t_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips    BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    return table_name


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source = create_source(t_env)
        sink = create_sink(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {sink}
            SELECT
                window_start,
                PULocationID,
                COUNT(*) AS num_trips
            FROM TABLE(
                TUMBLE(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY window_start, PULocationID
        """).wait()

    except Exception as e:
        print("Q4 job failed:", str(e))
        raise


if __name__ == "__main__":
    main()
