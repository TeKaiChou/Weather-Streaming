from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, to_timestamp
from pyspark.sql.types import *
import pyspark
import os

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default_pw")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/weather_agg")

spark_ver = pyspark.__version__
print(f"Using Spark version: {spark_ver}")

spark = SparkSession.builder \
    .appName("weather-stream") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather.raw.v1") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_kmh", DoubleType(), True),
    StructField("conditions", StringType(), True)
])

parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("json", from_json(col("value"), schema)) \
    .select("json.*") \
    .withColumn("event_ts", to_timestamp("event_time"))

# 1 minute window, sliding every 30 seconds, allowing 1 minute late data
agg = parsed.withWatermark("event_ts", "2 minutes") \
    .groupBy(
        window(col("event_ts"), "5 minutes", "30 seconds"),
        col("city")
    ).agg(
        avg("temp_c").alias("avg_temp_c"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_kmh").alias("avg_wind_kmh"),
        count("*").alias("count")
    ).select(
        col("city"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("count"),
        col("avg_temp_c"),
        col("avg_humidity"),
        col("avg_wind_kmh"),
    )

def foreach_batch_function(batch_df, batch_id):
    batch_df.write.mode("append").format("jdbc") \
    .option("url", "jdbc:clickhouse:http://clickhouse:8123/weather") \
    .option("dbtable", "agg_5m_by_city") \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASSWORD) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .save()

query = agg.writeStream.outputMode("update") \
    .foreachBatch(foreach_batch_function) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()
query.awaitTermination()

