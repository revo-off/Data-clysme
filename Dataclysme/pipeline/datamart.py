from pyspark.sql import SparkSession, functions as F


spark = (
	SparkSession.builder
	.appName("weather-datamart-gold")
	.enableHiveSupport()
	.getOrCreate()
)


source_table = "default.weather_silver"
df = spark.table(source_table)

group_cols = [c for c in ["year", "month", "country", "region", "city", "station_id"] if c in df.columns]
if "year" not in group_cols or "month" not in group_cols:
	raise ValueError("weather_silver must contain year and month columns to build datamarts.")


def build_datamart(metric_rules, target_table):
	available_rules = [(col_name, agg_name) for col_name, agg_name in metric_rules if col_name in df.columns]
	if not available_rules:
		raise ValueError("No expected metric columns found for {}".format(target_table))

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
		else:
			raise ValueError("Unsupported aggregation: {}".format(agg_name))

	datamart_df = df.groupBy(*group_cols).agg(*agg_exprs)

	(
		datamart_df.write
		.mode("overwrite")
		.format("parquet")
		.partitionBy("year", "month")
		.saveAsTable(target_table)
	)


risk_rules = [
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

tourism_rules = [
    ("avg_temp_c", "avg"),
    ("min_temp_c", "min"),
    ("max_temp_c", "max"),
    ("precipitation_mm", "sum"),
    ("snow_depth_mm", "avg"),
    ("avg_wind_speed_kmh", "avg"),
    ("sunshine_total_min", "sum"),
]

agri_rules = [
    ("avg_temp_c", "avg"),
    ("min_temp_c", "min"),
    ("max_temp_c", "max"),
    ("precipitation_mm", "sum"),
    ("sunshine_total_min", "sum"),
]

build_datamart(risk_rules, "default.dm_risks")
build_datamart(tourism_rules, "default.dm_tourism")
build_datamart(agri_rules, "default.dm_agriculture")

spark.stop()
