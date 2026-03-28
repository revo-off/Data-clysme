from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("weather-processor-silver")
    .enableHiveSupport()
    .getOrCreate()
)


input_path = "hdfs://namenode:9000/data/bronze/weather_daily"
silver_path = "hdfs://namenode:9000/data/silver/weather_curated"

df = (
    spark.read.parquet(input_path)
)

numeric_cols = [
    "avg_temp_c",
    "min_temp_c",
    "max_temp_c",
    "precipitation_mm",
    "snow_depth_mm",
    "avg_wind_dir_deg",
    "avg_wind_speed_kmh",
    "peak_wind_gust_kmh",
    "avg_sea_level_pres_hpa",
    "sunshine_total_min",
]

existing_numeric_cols = [c for c in numeric_cols if c in df.columns]

df2 = (
    df
    .withColumn("date", F.to_date(F.col("date")))
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.month(F.col("date")))
)

for c in existing_numeric_cols:
    df2 = df2.withColumn(c, F.col(c).cast("double"))

df2 = (
    df2
    .dropna(subset=["date", "year"])
    .dropDuplicates()
)

silver_count = df2.count()
print("Silver rows curated: {}".format(silver_count))

(
    df2
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(silver_path)
)

(
    spark.read.parquet(silver_path)
   .write
   .mode("overwrite")
   .format("parquet")
   .partitionBy("year", "month")
   .saveAsTable("default.weather_silver")
)

spark.stop()