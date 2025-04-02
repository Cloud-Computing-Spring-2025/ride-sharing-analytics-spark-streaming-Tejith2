from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder.appName("RideSharingTask1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON
parsed_df = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Print to console
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()

