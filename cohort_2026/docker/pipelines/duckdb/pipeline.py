import duckdb

conn = duckdb.connect()

PATH = "/Users/fdr/SqlProjects/Datatalks/de_zoomcamp_2023/cohort_2026/docker/pipelines/raw_data/"


conn.execute("INSTALL postgres; LOAD postgres;")

# Подключаемся к PostgreSQL
conn.execute("""
    ATTACH 'dbname=ny_taxi user=root password=root host=localhost port=5432'
    AS pg (TYPE POSTGRES)
""")


conn.execute(f"""
    CREATE TABLE pg.duckdb.taxi_zone_lookup AS
    SELECT * FROM read_csv('{PATH}taxi_zone_lookup.csv')
""")

# Загружаем Parquet
conn.execute(f"""
    CREATE TABLE pg.duckdb.green_trip_data AS
    SELECT * FROM read_parquet('{PATH}green_tripdata_2025-11.parquet')
""")

conn.close()
