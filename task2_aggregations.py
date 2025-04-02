from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Step 1: Spark session
spark = SparkSession.builder.appName("Task2DriverAggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Step 2: Define schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Step 3: Ingest from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse JSON
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Step 5: Group by driver_id and aggregate
agg_df = parsed_df.groupBy("driver_id") \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# Step 6: Output to console
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

