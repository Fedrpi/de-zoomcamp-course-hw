from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'

fhv_spark = T.StructType([
    T.StructField("dispatching_base_num", T.StringType(), True),
    T.StructField("pickup_datetime", T.StringType(), True),
    T.StructField("dropOff_datetime", T.StringType(), True),
    T.StructField("PULocationID", T.IntegerType(), True),
    T.StructField("DOLocationID", T.IntegerType(), True),
    T.StructField("SR_Flag", T.IntegerType(), True),
    T.StructField("Affiliated_base_number", T.StringType(), True),
])

green_spark = T.StructType([
    T.StructField("VendorID", T.IntegerType(), True),
    T.StructField("lpep_pickup_datetime", T.StringType(), True),
    T.StructField("lpep_dropoff_datetime", T.StringType(), True),
    T.StructField("store_and_fwd_flag", T.StringType(), True),
    T.StructField("RatecodeID", T.IntegerType(), True),
    T.StructField("PULocationID", T.IntegerType(), True),
    T.StructField("DOLocationID", T.IntegerType(), True),
    T.StructField("passenger_count", T.IntegerType(), True),
    T.StructField("trip_distance", T.FloatType(), True),
    T.StructField("fare_amount", T.FloatType(), True),
    T.StructField("extra", T.FloatType(), True),
    T.StructField("mta_tax", T.FloatType(), True),
    T.StructField("tip_amount", T.FloatType(), True),
    T.StructField("tolls_amount", T.FloatType(), True),
    T.StructField("ehail_fee", T.IntegerType(), True),
    T.StructField("improvement_surcharge", T.FloatType(), True),
    T.StructField("payment_type", T.FloatType(), True),
    T.StructField("trip_type", T.FloatType(), True),
    T.StructField("congestion_surcharge", T.IntegerType(), True)
])

def read_df_kafka(topic:str):
    stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "checkpoint") \
    .option("failOnDataLoss", "false") \
    .load()
    return stream_df

def parse_df(stream_df, spark_schema):
    parsed_df = stream_df \
        .select(F.from_json(F.col("value").cast("string"), spark_schema).alias("df")) \
        .select("df.PULocationID")
    return parsed_df

def union_df(df1, df2):
    union_df = df1.union(df2)
    return union_df

def aggregate_df(union_df):
    aggregated_df = union_df \
        .groupBy('PULocationID') \
        .agg({"PULocationID": "count"}) \
        .withColumnRenamed("count(PULocationID)", "countLocID") \
        .select("PULocationID", "countLocID")
    return aggregated_df

def write_kafka(df, topic:str):

    write_query = df.select(F.to_json(F.struct("PULocationID", "countLocID")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoint_stream") \
        .start()
    return write_query

def write_console(df):
    write_query = df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .outputMode("complete") \
        .option("checkpointLocation", "checkpoint_stream") \
        .start() 
    return write_query

if __name__ == "__main__":
    spark = SparkSession.builder.appName('taxi_streaming').getOrCreate()

    fhv_stream = read_df_kafka('fhv_rides_head_json')
    fhv_parsed = parse_df(fhv_stream, fhv_spark)

    green_stream = read_df_kafka('green_rides_head_json')
    green_parsed = parse_df(green_stream, green_spark)

    union = union_df(fhv_parsed, green_parsed)
    agg_df = aggregate_df(union)
    write_kafka(agg_df, 'agg_taxi')

    spark.streams.awaitAnyTermination()
