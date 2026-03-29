# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .appName("weather-processor")
    .enableHiveSupport()
    .getOrCreate()
)

input_path = "hdfs://namenode:9000/data/bronze/weather_daily"
df = spark.read.parquet(input_path)

# Liste de toutes les colonnes nécessaires aux datamarts (Risques, Tourisme, Agriculture)
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
    "sunshine_total_min"
]

df2 = (
    df
    .withColumn(
        "date",
        F.coalesce(
            F.to_date(F.col("date")),
            F.to_date(F.col("date").cast("string"), "yyyy-MM-dd"),
            F.to_date(F.col("date").cast("string"), "yyyy/MM/dd"),
        ),
    )
)

if "city" in df2.columns:
    df2 = df2.withColumn("city_name", F.coalesce(F.col("city_name"), F.col("city")))

if "country_name" in df2.columns:
    df2 = df2.withColumn("country", F.coalesce(F.col("country"), F.col("country_name")))

# On s'assure que chaque colonne numerique est bien typee en double
for col_name in numeric_cols:
    if col_name in df2.columns:
        df2 = df2.withColumn(col_name, F.col(col_name).cast("double"))

# Nettoyage de base : conserver les lignes exploitables pour silver
df2 = df2.dropna(subset=["date", "year"])

# On peut aussi s'assurer qu'il n'y ait pas de doublons purs
df2 = df2.dropDuplicates()

df2.cache()

r = df2.count()
print("Nombre de lignes après nettoyage : {}".format(r))

output_base = "hdfs://namenode:9000/data/silver/weather_curated"

time.sleep(10)

(
    df2.repartition(4)
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year")
    .parquet(output_base)
)

(
    spark.read.parquet(output_base)
   .write
   .mode("overwrite")
   .format("parquet")
   .partitionBy("year")
   .saveAsTable("default.weather_silver")
)

spark.stop()