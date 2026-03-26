from datetime import date
from pyspark.sql import SparkSession, functions as F
import time


spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)


input_path = "file:///source/daily_weather.parquet" 
df = (
    spark.read
    .parquet(input_path)
)

df2 = (
    df.withColumn("year", F.year(F.col("date")))
)

df2.cache()

df2.show(5)

r =  df2.count()
print("Nombre de lignes : {}".format(r))

output_base = "hdfs://namenode:9000/data/raw/weather_data_partitioned"

time.sleep(120)

(
    df2.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(output_base)
)