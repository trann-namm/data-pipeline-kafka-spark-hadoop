from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract,col, to_date, year, month, dayofmonth
from config import *
# Create Spark session

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()
    
df = spark.createDataFrame([("test", 1), ("sample", 2)], ["key", "value"])
df.write.mode("overwrite").parquet(hdfs_test_path)

# Read streaming data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()
# Match the log_regex vs each line
log_pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d{3}) (\d+) "(.*?)" "(.*?)"'

valid_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .filter(col("value").rlike(log_pattern)) 

parsed_stream = valid_stream.selectExpr("CAST(value AS STRING)") \
    .select(
        regexp_extract(col("value"), log_pattern, 1).alias("client"),
        regexp_extract(col("value"), log_pattern, 2).alias("datetime"),
        regexp_extract(col("value"), log_pattern, 3).alias("request"),
        regexp_extract(col("value"), log_pattern, 4).alias("status"),
        regexp_extract(col("value"), log_pattern, 5).alias("size"),
        regexp_extract(col("value"), log_pattern, 6).alias("referrer"),
        regexp_extract(col("value"), log_pattern, 7).alias("useragent")
    )
# reformatting the date columns, adding year, month, day columns for partitioning
parsed_stream = parsed_stream.withColumn("date", to_date("datetime", "dd/MMM/yyyy:HH:mm:ss Z")) \
                             .withColumn("year", year("date")) \
                             .withColumn("month", month("date")) \
                             .withColumn("day", dayofmonth("date"))


# Write data hdfs as Parquet format
parsed_stream.writeStream \
    .format("parquet") \
    .option("path", hdfs_target_url) \
    .option("checkpointLocation", hdfs_offset_path) \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

