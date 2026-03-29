# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .appName("weather-feeder")
    .getOrCreate()
)

input_path = "file:///source/daily_weather.parquet" 
df = spark.read.parquet(input_path)

jdbc_url = "jdbc:mysql://mysql:3306/dataclysme"
properties = {
    "user": "root",
    "password": "my-secret-pw",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_cities = spark.read.jdbc(url=jdbc_url, table="cities", properties=properties)
df_countries = spark.read.jdbc(url=jdbc_url, table="countries", properties=properties)

df_countries_clean = df_countries.drop("country", "iso2")
df_geo = df_cities.join(df_countries_clean, on="iso3", how="left")
df_joined = df.join(df_geo, on="city_name", how="left")
df_joined = df_joined.drop("iso2", "iso3", "station_id", "__index_level_0__")

df2 = df_joined.withColumn("year", F.year(F.col("date")))

df2.cache()

df2.show(5)

r =  df2.count()
print("Nombre de lignes : {}".format(r))

output_base = "hdfs://namenode:9000/data/bronze/weather_daily"

time.sleep(30)

(
    df2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(output_base)
)

spark.stop()

