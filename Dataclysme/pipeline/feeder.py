from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("weather-feeder-bronze")
    .getOrCreate()
)


input_path = "file:///source/daily_weather.parquet"
df = (
    spark.read
    .parquet(input_path)
)

df2 = (
    df
    .withColumn("date", F.to_date(F.col("date")))
    .withColumn("year", F.year(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
    .withColumn("ingestion_ts", F.current_timestamp())
)

row_count = df2.count()
print("Bronze rows ingested: {}".format(row_count))

output_base = "hdfs://namenode:9000/data/bronze/weather_daily"

(
    df2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(output_base)
)

spark.stop()