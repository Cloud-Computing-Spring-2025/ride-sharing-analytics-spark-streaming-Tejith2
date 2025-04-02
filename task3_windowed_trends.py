from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder.appName("Task3WindowedTrendsCSV").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Define schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# 3. Read stream from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse JSON and convert timestamp
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))

# 5. Write to CSV in append mode (no aggregation!)
query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/task3_csv") \
    .option("checkpointLocation", "checkpoints/task3_csv") \
    .option("header", "true") \
    .start()

# 6. Keep streaming
query.awaitTermination()

