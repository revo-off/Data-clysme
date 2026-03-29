from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("diag-silver-counts")
    .enableHiveSupport()
    .getOrCreate()
)

bronze_df = spark.read.parquet("hdfs://namenode:9000/data/bronze/weather_daily")
silver_parquet_df = spark.read.parquet("hdfs://namenode:9000/data/silver/weather_curated")

print("BRONZE_ROWS={}".format(bronze_df.count()))
print("SILVER_PARQUET_ROWS={}".format(silver_parquet_df.count()))

try:
    silver_table_df = spark.table("default.weather_silver")
    print("SILVER_TABLE_ROWS={}".format(silver_table_df.count()))
except Exception as exc:
    print("SILVER_TABLE_ERROR={}".format(exc))

spark.stop()
