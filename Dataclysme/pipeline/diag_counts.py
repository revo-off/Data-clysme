from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("diag-counts")
    .getOrCreate()
)

path = "hdfs://namenode:9000/data/bronze/weather_daily"
df = spark.read.parquet(path)

print("TOTAL={}".format(df.count()))
print("DATE_NOT_NULL={}".format(df.where(F.col("date").isNotNull()).count()))
print(
    "CITY_NOT_NULL={}".format(
        df.where(F.col("city_name").isNotNull() & (F.trim(F.col("city_name")) != "")).count()
    )
)
print("YEAR_NOT_NULL={}".format(df.where(F.col("year").isNotNull()).count()))

spark.stop()
