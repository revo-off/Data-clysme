import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def build_parser():
	parser = argparse.ArgumentParser(description="Build gold datamarts and publish them to MySQL.")
	parser.add_argument("--source-table", default=os.getenv("DATAMART_SOURCE_TABLE", "default.weather_silver"))
	parser.add_argument("--jdbc-url", default=os.getenv("JDBC_URL"))
	parser.add_argument("--jdbc-user", default=os.getenv("JDBC_USER"))
	parser.add_argument("--jdbc-password", default=os.getenv("JDBC_PASSWORD"))
	parser.add_argument("--jdbc-driver", default="com.mysql.cj.jdbc.Driver")
	parser.add_argument("--risk-table", default=os.getenv("DATAMART_RISK_TABLE", "dm_risks"))
	parser.add_argument("--tourism-table", default=os.getenv("DATAMART_TOURISM_TABLE", "dm_tourism"))
	parser.add_argument("--agri-table", default=os.getenv("DATAMART_AGRI_TABLE", "dm_agriculture"))
	parser.add_argument("--app-name", default="weather-datamart-gold")
	parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
	parser.add_argument("--log-file", default=os.getenv("DATAMART_LOG_FILE", "/opt/pipeline/logs/datamart.txt"))
	return parser


def configure_logger(log_file):
	log_dir = os.path.dirname(log_file)
	if log_dir:
		os.makedirs(log_dir, exist_ok=True)

	logging.basicConfig(
		filename=log_file,
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(message)s",
	)
	return logging.getLogger("datamart")


def require_args(args, logger):
	missing = []
	if not args.jdbc_url:
		missing.append("--jdbc-url or JDBC_URL")
	if not args.jdbc_user:
		missing.append("--jdbc-user or JDBC_USER")
	if not args.jdbc_password:
		missing.append("--jdbc-password or JDBC_PASSWORD")

	if missing:
		message = "Missing required configuration: {}".format(", ".join(missing))
		logger.error(message)
		raise ValueError(message)


def build_datamart(df, metric_rules):
	available_rules = [(col_name, agg_name) for col_name, agg_name in metric_rules if col_name in df.columns]
	if not available_rules:
		raise ValueError("No expected metric columns found in silver dataset")

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

	base_group_cols = [
		c for c in ["weather_year", "weather_month", "country", "region", "city_name"]
		if c in df.columns
	]
	if "weather_year" not in base_group_cols or "weather_month" not in base_group_cols:
		raise ValueError("weather_silver must contain weather_year and weather_month columns")

	return (
		df.groupBy(*base_group_cols)
		.agg(*agg_exprs)
		.withColumnRenamed("weather_year", "year")
		.withColumnRenamed("weather_month", "month")
	)


def main():
	args = build_parser().parse_args()
	logger = configure_logger(args.log_file)

	spark = (
		SparkSession.builder
		.appName(args.app_name)
		.enableHiveSupport()
		.getOrCreate()
	)

	try:
		require_args(args, logger)
		logger.info("Reading silver table: %s", args.source_table)
		df = spark.table(args.source_table)

		jdbc_props = {
			"user": args.jdbc_user,
			"password": args.jdbc_password,
			"driver": args.jdbc_driver,
		}

		dm_risks = build_datamart(df, RISK_RULES)
		dm_tourism = build_datamart(df, TOURISM_RULES)
		dm_agri = build_datamart(df, AGRI_RULES)

		dm_risks.write.jdbc(url=args.jdbc_url, table=args.risk_table, mode=args.write_mode, properties=jdbc_props)
		dm_tourism.write.jdbc(url=args.jdbc_url, table=args.tourism_table, mode=args.write_mode, properties=jdbc_props)
		dm_agri.write.jdbc(url=args.jdbc_url, table=args.agri_table, mode=args.write_mode, properties=jdbc_props)

		logger.info("Datamarts exported to MySQL tables: %s, %s, %s", args.risk_table, args.tourism_table, args.agri_table)

	except Exception:
		logger.error("Datamart execution failed", exc_info=True)
		raise
	finally:
		spark.stop()


if __name__ == "__main__":
	main()
