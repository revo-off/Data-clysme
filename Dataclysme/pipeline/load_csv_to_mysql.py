from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("load_csv_to_mysql")
    .getOrCreate()
)

# Infos de connexion MySQL
jdbc_url = "jdbc:mysql://mysql:3306/dataclysme"
properties = {
    "user": "root",
    "password": "my-secret-pw",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 1. Lecture et ecriture de cities.csv
df_cities = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///source/cities.csv")
df_cities.write.jdbc(url=jdbc_url, table="cities", mode="overwrite", properties=properties)

# 2. Lecture et ecriture de countries.csv
df_countries = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///source/countries.csv")
df_countries.write.jdbc(url=jdbc_url, table="countries", mode="overwrite", properties=properties)

print("Chargement CSV -> MySQL termine avec succes.")
spark.stop()