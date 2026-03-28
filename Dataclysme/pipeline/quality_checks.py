from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("weather-quality-checks")
    .enableHiveSupport()
    .getOrCreate()
)


BRONZE_PATH = "hdfs://namenode:9000/data/bronze/weather_daily"
SILVER_TABLE = "default.weather_silver"
GOLD_TABLES = [
    "default.dm_risks",
    "default.dm_tourism",
    "default.dm_agriculture",
]

EXPECTED_SILVER_COLUMNS = [
    "date",
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
]

EXPECTED_GOLD_COLUMNS = {
    "default.dm_risks": [
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
    "default.dm_tourism": [
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
    "default.dm_agriculture": [
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

results = []

# Bronze checks
if hdfs_path_exists(BRONZE_PATH):
    bronze_df = spark.read.parquet(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print("[OK] Bronze path exists: {}".format(BRONZE_PATH))
    print("Bronze row count: {}".format(bronze_count))
    results.append(("bronze_exists", "OK"))
    results.append(("bronze_count", str(bronze_count)))
else:
    print("[KO] Bronze path missing: {}".format(BRONZE_PATH))
    results.append(("bronze_exists", "KO"))

# Silver checks
if table_exists(SILVER_TABLE):
    silver_df = spark.table(SILVER_TABLE)
    silver_count = silver_df.count()
    silver_missing = missing_columns(silver_df, EXPECTED_SILVER_COLUMNS)

    print("[OK] Silver table exists: {}".format(SILVER_TABLE))
    print("Silver row count: {}".format(silver_count))
    print("Silver missing columns: {}".format(silver_missing if silver_missing else "none"))

    silver_nulls = null_rates(silver_df, ["date", "year", "month", "avg_temp_c", "precipitation_mm"])
    print("Silver null rates: {}".format(silver_nulls))

    results.append(("silver_exists", "OK"))
    results.append(("silver_count", str(silver_count)))
    results.append(("silver_missing_columns", ",".join(silver_missing)))
else:
    print("[KO] Silver table missing: {}".format(SILVER_TABLE))
    results.append(("silver_exists", "KO"))

# Gold checks
for table_name in GOLD_TABLES:
    if table_exists(table_name):
        dm_df = spark.table(table_name)
        dm_count = dm_df.count()
        dm_missing = missing_columns(dm_df, EXPECTED_GOLD_COLUMNS[table_name])

        print("[OK] Gold table exists: {}".format(table_name))
        print("{} row count: {}".format(table_name, dm_count))
        print("{} missing columns: {}".format(table_name, dm_missing if dm_missing else "none"))

        results.append(("{}_exists".format(table_name), "OK"))
        results.append(("{}_count".format(table_name), str(dm_count)))
        results.append(("{}_missing_columns".format(table_name), ",".join(dm_missing)))
    else:
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
