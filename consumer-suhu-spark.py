from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, FloatType, StructType, StructField
import json

spark = SparkSession.builder \
    .appName("KafkaPySparkConsumer") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", FloatType(), True)
])

# Konfigurasi stream untuk membaca data dari Kafka
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .load()

# Parsing data suhu JSON dari Kafka
suhu_df = suhu_df.selectExpr("CAST(value AS STRING) as json") \
                 .select(from_json(col("json"), schema).alias("data")) \
                 .select("data.*")

# Filter data suhu yang melebihi 80°C
peringatan_suhu_df = suhu_df.filter(col("suhu") > 80)

query = peringatan_suhu_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
