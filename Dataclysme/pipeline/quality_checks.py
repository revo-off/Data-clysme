import argparse
import os

from pyspark.sql import SparkSession, functions as F


def build_parser():
    parser = argparse.ArgumentParser(description="Run pipeline quality checks.")
    parser.add_argument("--bronze-path", default=os.getenv("BRONZE_PATH", "hdfs://namenode:9000/data/raw/weather_daily"))
    parser.add_argument("--silver-table", default=os.getenv("SILVER_TABLE", "default.weather_silver"))
    parser.add_argument("--jdbc-url", default=os.getenv("JDBC_URL"))
    parser.add_argument("--jdbc-user", default=os.getenv("JDBC_USER"))
    parser.add_argument("--jdbc-password", default=os.getenv("JDBC_PASSWORD"))
    parser.add_argument("--jdbc-driver", default="com.mysql.cj.jdbc.Driver")
    parser.add_argument("--risk-table", default=os.getenv("DATAMART_RISK_TABLE", "dm_risks"))
    parser.add_argument("--tourism-table", default=os.getenv("DATAMART_TOURISM_TABLE", "dm_tourism"))
    parser.add_argument("--agri-table", default=os.getenv("DATAMART_AGRI_TABLE", "dm_agriculture"))
    return parser


spark = (
    SparkSession.builder
    .appName("weather-quality-checks")
    .enableHiveSupport()
    .getOrCreate()
)

EXPECTED_SILVER_COLUMNS = [
    "date",
    "year",
    "month",
    "day",
    "weather_year",
    "weather_month",
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

EXPECTED_GOLD_COLUMNS = {
    "dm_risks": [
        "year",
        "month",
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
    ],
    "dm_tourism": [
        "year",
        "month",
        "avg_temp_c",
        "min_temp_c",
        "max_temp_c",
        "precipitation_mm",
        "snow_depth_mm",
        "avg_wind_speed_kmh",
        "sunshine_total_min",
    ],
    "dm_agriculture": [
        "year",
        "month",
        "avg_temp_c",
        "min_temp_c",
        "max_temp_c",
        "precipitation_mm",
        "sunshine_total_min",
    ],
}


def hdfs_path_exists(path):
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except Exception as e:
        if "Path does not exist" in str(e) or "FileNotFoundException" in str(e):
            return False
        return False


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def missing_columns(df, expected_cols):
    current_cols = set(df.columns)
    return [c for c in expected_cols if c not in current_cols]


def null_rates(df, cols):
    existing_cols = [c for c in cols if c in df.columns]
    if not existing_cols:
        return {}

    total = df.count()
    if total == 0:
        return {c: None for c in existing_cols}

    exprs = [
        (
            F.sum(
                F.when(
                    F.col(c).isNull() | (F.col(c) == "") | (F.trim(F.col(c).cast("string")) == ""),
                    1,
                ).otherwise(0)
            )
            / F.lit(total)
        ).alias(c)
        for c in existing_cols
    ]
    row = df.select(*exprs).collect()[0].asDict()
    return row


def print_section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


print_section("DATA-CLYSME QUALITY CHECKS")

args = build_parser().parse_args()
gold_tables = [args.risk_table, args.tourism_table, args.agri_table]

results = []

# Bronze checks
if hdfs_path_exists(args.bronze_path):
    bronze_df = spark.read.parquet(args.bronze_path)
    bronze_count = bronze_df.count()
    print("[OK] Bronze path exists: {}".format(args.bronze_path))
    print("Bronze row count: {}".format(bronze_count))
    results.append(("bronze_exists", "OK"))
    results.append(("bronze_count", str(bronze_count)))
else:
    print("[KO] Bronze path missing: {}".format(args.bronze_path))
    results.append(("bronze_exists", "KO"))

# Silver checks
if table_exists(args.silver_table):
    silver_df = spark.table(args.silver_table)
    silver_count = silver_df.count()
    silver_missing = missing_columns(silver_df, EXPECTED_SILVER_COLUMNS)

    print("[OK] Silver table exists: {}".format(args.silver_table))
    print("Silver row count: {}".format(silver_count))
    print("Silver missing columns: {}".format(silver_missing if silver_missing else "none"))

    silver_nulls = null_rates(silver_df, ["date", "year", "month", "day", "avg_temp_c", "precipitation_mm"])
    print("Silver null rates: {}".format(silver_nulls))

    results.append(("silver_exists", "OK"))
    results.append(("silver_count", str(silver_count)))
    results.append(("silver_missing_columns", ",".join(silver_missing)))
else:
    print("[KO] Silver table missing: {}".format(args.silver_table))
    results.append(("silver_exists", "KO"))

# Gold checks (MySQL relationnel)
jdbc_props = {
    "user": args.jdbc_user,
    "password": args.jdbc_password,
    "driver": args.jdbc_driver,
}

for table_name in gold_tables:
    try:
        dm_df = spark.read.jdbc(url=args.jdbc_url, table=table_name, properties=jdbc_props)
        dm_count = dm_df.count()
        dm_missing = missing_columns(dm_df, EXPECTED_GOLD_COLUMNS[table_name])

        print("[OK] Gold table exists: {}".format(table_name))
        print("{} row count: {}".format(table_name, dm_count))
        print("{} missing columns: {}".format(table_name, dm_missing if dm_missing else "none"))

        results.append(("{}_exists".format(table_name), "OK"))
        results.append(("{}_count".format(table_name), str(dm_count)))
        results.append(("{}_missing_columns".format(table_name), ",".join(dm_missing)))
    except Exception:
        print("[KO] Gold table missing: {}".format(table_name))
        results.append(("{}_exists".format(table_name), "KO"))

# Persist run summary in Hive
summary_df = (
    spark.createDataFrame(results, ["check_name", "check_value"])
    .withColumn("run_ts", F.current_timestamp())
)

(
    summary_df.write
    .mode("append")
    .format("parquet")
    .saveAsTable("default.pipeline_quality_audit")
)

print_section("QUALITY CHECKS COMPLETED")
print("Audit table updated: default.pipeline_quality_audit")

spark.stop()
