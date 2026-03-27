from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("weather-feeder-bronze")
    .getOrCreate()
)

jdbc_url = "jdbc:mysql://mysql:3306/dataclysme"
properties = {
    "user": "root",
    "password": "my-secret-pw",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_cities = spark.read.jdbc(url=jdbc_url, table="cities", properties=properties)
df_countries = spark.read.jdbc(url=jdbc_url, table="countries", properties=properties).drop("iso2", "iso3")
df_locations = df_cities.join(df_countries, on="country", how="left")
df_locations.cache()
print("Locations ingerees depuis MySQL : {}".format(df_locations.count()))

input_path = "file:///source/daily_weather.parquet"
df_weather = (
    spark.read
    .parquet(input_path)
)

df_final = df_weather.join(df_locations, on="city_name", how="left")
df2 = (
    df_final.withColumn("year", F.year(F.col("date")))
)

df_clean = df2.drop("iso2", "iso3", "station_id")

row_count = df_clean.count()
print("Bronze rows ingested: {}".format(row_count))

output_base = "hdfs://namenode:9000/data/bronze/weather_daily"

(
    df_clean.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(output_base)
)

spark.stop()
