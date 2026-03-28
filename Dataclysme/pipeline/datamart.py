# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Definition des agregations pour chaque datamart
RISK_RULES = [
    ("avg_temp_c", "avg"),
    ("min_temp_c", "min"),
    ("max_temp_c", "max"),
    ("precipitation_mm", "sum"),
    ("snow_depth_mm", "avg"),
    ("avg_wind_dir_deg", "avg"),
    ("avg_wind_speed_kmh", "avg"),
    ("peak_wind_gust_kmh", "max"),
    ("avg_sea_level_pres_hpa", "avg"),
    ("sunshine_total_min", "sum"),
]

TOURISM_RULES = [
    ("avg_temp_c", "avg"),
    ("min_temp_c", "min"),
    ("max_temp_c", "max"),
    ("precipitation_mm", "sum"),
    ("snow_depth_mm", "avg"),
    ("avg_wind_speed_kmh", "avg"),
    ("sunshine_total_min", "sum"),
]

AGRI_RULES = [
    ("avg_temp_c", "avg"),
    ("min_temp_c", "min"),
    ("max_temp_c", "max"),
    ("precipitation_mm", "sum"),
    ("sunshine_total_min", "sum"),
]

def build_datamart(df, metric_rules):
    # Conserver uniquement les regles dont les colonnes existent
    available_rules = [(col_name, agg_name) for col_name, agg_name in metric_rules if col_name in df.columns]
    
    agg_exprs = []
    for col_name, agg_name in available_rules:
        if agg_name == "avg":
            agg_exprs.append(F.avg(F.col(col_name)).alias(col_name))
        elif agg_name == "min":
            agg_exprs.append(F.min(F.col(col_name)).alias(col_name))
        elif agg_name == "max":
            agg_exprs.append(F.max(F.col(col_name)).alias(col_name))
        elif agg_name == "sum":
            agg_exprs.append(F.sum(F.col(col_name)).alias(col_name))

    # Colonnes de regroupement de base
    base_group_cols = [c for c in ["year", "country", "region", "city_name"] if c in df.columns]

    return df.groupBy(*base_group_cols).agg(*agg_exprs)

def main():
    spark = (
        SparkSession.builder
        .appName("weather-datamart-gold")
        .enableHiveSupport()
        .getOrCreate()
    )

    print("Chargement de default.weather_silver depuis Hive...")
    df = spark.table("default.weather_silver")

    print("Construction des aggrégations (Datamarts)...")
    dm_risks = build_datamart(df, RISK_RULES)
    dm_tourism = build_datamart(df, TOURISM_RULES)
    dm_agri = build_datamart(df, AGRI_RULES)

    # Informations de connexion MySQL
    jdbc_url = "jdbc:mysql://mysql:3306/dataclysme"
    jdbc_props = {
        "user": "root",
        "password": "my-secret-pw",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        print("Sauvegarde de dm_risks dans MySQL...")
        dm_risks.write.jdbc(url=jdbc_url, table="dm_risks", mode="overwrite", properties=jdbc_props)
        
        print("Sauvegarde de dm_tourism dans MySQL...")
        dm_tourism.write.jdbc(url=jdbc_url, table="dm_tourism", mode="overwrite", properties=jdbc_props)
        
        print("Sauvegarde de dm_agriculture dans MySQL...")
        dm_agri.write.jdbc(url=jdbc_url, table="dm_agriculture", mode="overwrite", properties=jdbc_props)
        
        print("Terminé avec succès !")
    except Exception as e:
        print("Erreur lors de l'export vers MySQL: {}".format(str(e)))
        raise e

if __name__ == "__main__":
    main()