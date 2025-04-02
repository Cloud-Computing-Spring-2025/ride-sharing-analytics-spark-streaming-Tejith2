Real-Time Ride-Sharing Analytics with Apache Spark

This project implements a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming.

You will process streaming data, perform real-time aggregations, and analyze trends over time.

Task 1: Basic Streaming Ingestion and Parsing

✅ Objective:

Read real-time JSON data from socket (localhost:9999).

Parse it into structured columns.

✅ Steps:

Start netcat listener:

nc -lk 9999

In another terminal, run the Spark script:

spark-submit task1_stream_ingest.py

✅ Code Snippet (task1_stream_ingest.py):

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("Task1Ingest").getOrCreate()
schema = StructType().add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

raw_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = df.writeStream.outputMode("append").format("csv") \
    .option("path", "output/task1_csv") \
    .option("checkpointLocation", "checkpoints/task1") \
    .start()

query.awaitTermination()

📊 Task 2: Real-Time Aggregations (Driver-Level)

✅ Objective:

Calculate:

Total fare by driver_id

Average distance by driver_id

Output result to CSV

✅ Steps:

spark-submit task2_aggregations.py

✅ Code Snippet (task2_aggregations.py):

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("Task2Aggregations").getOrCreate()
schema = StructType().add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

raw_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

agg_df = df.groupBy("driver_id") \
    .agg(sum("fare_amount").alias("total_fare"), avg("distance_km").alias("avg_distance"))

query = agg_df.writeStream.outputMode("complete").format("csv") \
    .option("path", "output/task2_csv") \
    .option("checkpointLocation", "checkpoints/task2") \
    .start()

query.awaitTermination()

⏱️ Task 3: Windowed Time-Based Analytics

✅ Objective:

Convert timestamp to TimestampType.

Aggregate fare_amount in a 5-minute window sliding every 1 minute.

✅ Steps:

spark-submit task3_windowed_trends.py

✅ Code Snippet (task3_windowed_trends.py):

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("Task3WindowedTrends").getOrCreate()
schema = StructType().add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

raw_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
df = df.withColumn("event_time", col("timestamp").cast(TimestampType()))

agg_df = df.groupBy(window(col("event_time"), "5 minutes", "1 minute"), "driver_id") \
    .sum("fare_amount").withColumnRenamed("sum(fare_amount)", "total_fare")

final_df = agg_df.withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")).drop("window")

query = final_df.writeStream.outputMode("complete").format("csv") \
    .option("path", "output/task3_csv") \
    .option("checkpointLocation", "checkpoints/task3") \
    .start()

query.awaitTermination()

🔁 Sample Data Generator (Optional)

If you want to simulate streaming data:

python3 data_generator.py

Make sure port 9999 is free before running.

✅ Final Notes

Ensure the folders checkpoints/ and output/ exist or are created by Spark.

All .csv files are stored under output/ in respective task folders.

Use cat output/taskX_csv/part-*.csv to view outputs.

🧠 Skills Gained

Real-time ingestion using Spark

Structured Streaming transformations

Time-windowed aggregations

CSV output in append/complete modes


