import argparse
import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


NUMERIC_COLS = [
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


def build_parser():
    parser = argparse.ArgumentParser(description="Clean and enrich raw weather data into silver layer.")
    parser.add_argument("--raw-input-path", default=os.getenv("PROCESSOR_RAW_INPUT_PATH"))
    parser.add_argument("--silver-output-path", default=os.getenv("PROCESSOR_SILVER_OUTPUT_PATH"))
    parser.add_argument("--silver-table", default=os.getenv("PROCESSOR_SILVER_TABLE", "default.weather_silver"))
    parser.add_argument("--jdbc-url", default=os.getenv("JDBC_URL"))
    parser.add_argument("--jdbc-user", default=os.getenv("JDBC_USER"))
    parser.add_argument("--jdbc-password", default=os.getenv("JDBC_PASSWORD"))
    parser.add_argument("--cities-table", default=os.getenv("CITIES_TABLE", "cities"))
    parser.add_argument("--countries-table", default=os.getenv("COUNTRIES_TABLE", "countries"))
    parser.add_argument("--jdbc-driver", default="com.mysql.cj.jdbc.Driver")
    parser.add_argument("--app-name", default="weather-processor-silver")
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--log-file", default=os.getenv("PROCESSOR_LOG_FILE", "/opt/pipeline/logs/processor.txt"))
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
    return logging.getLogger("processor")


def require_args(args, logger):
    missing = []
    if not args.raw_input_path:
        missing.append("--raw-input-path or PROCESSOR_RAW_INPUT_PATH")
    if not args.silver_output_path:
        missing.append("--silver-output-path or PROCESSOR_SILVER_OUTPUT_PATH")
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


def log_validation_metrics(df, logger):
    metrics_row = df.select(
        F.sum(F.when(F.col("date").isNull(), 1).otherwise(0)).alias("rule_date_not_null_failed"),
        F.sum(F.when(F.col("city_name").isNull() | (F.trim(F.col("city_name")) == ""), 1).otherwise(0)).alias("rule_city_name_not_null_failed"),
        F.sum(F.when(F.col("precipitation_mm") < 0, 1).otherwise(0)).alias("rule_precipitation_non_negative_failed"),
        F.sum(F.when(F.col("snow_depth_mm") < 0, 1).otherwise(0)).alias("rule_snow_depth_non_negative_failed"),
        F.sum(F.when(F.col("avg_wind_speed_kmh") < 0, 1).otherwise(0)).alias("rule_wind_speed_non_negative_failed"),
        F.sum(
            F.when(
                (F.col("min_temp_c").isNotNull())
                & (F.col("avg_temp_c").isNotNull())
                & (F.col("max_temp_c").isNotNull())
                & (
                    (F.col("min_temp_c") > F.col("avg_temp_c"))
                    | (F.col("avg_temp_c") > F.col("max_temp_c"))
                ),
                1,
            ).otherwise(0)
        ).alias("rule_temperature_consistency_failed"),
    ).collect()[0]

    logger.info("Validation metrics: %s", metrics_row.asDict())


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
        logger.info("Reading raw data from %s", args.raw_input_path)
        df = spark.read.parquet(args.raw_input_path)

        existing_numeric_cols = [c for c in NUMERIC_COLS if c in df.columns]

        df2 = (
            df
            .withColumn("date", F.to_date(F.col("date")))
            .withColumn("year", F.col("year").cast("int"))
            .withColumn("month", F.col("month").cast("int"))
            .withColumn("day", F.col("day").cast("int"))
            .withColumn("weather_year", F.year(F.col("date")))
            .withColumn("weather_month", F.month(F.col("date")))
        )

        for col_name in existing_numeric_cols:
            df2 = df2.withColumn(col_name, F.col(col_name).cast("double"))

        log_validation_metrics(df2, logger)

        valid_df = (
            df2
            .where(F.col("date").isNotNull())
            .where(F.col("city_name").isNotNull() & (F.trim(F.col("city_name")) != ""))
            .where((F.col("precipitation_mm").isNull()) | (F.col("precipitation_mm") >= 0))
            .where((F.col("snow_depth_mm").isNull()) | (F.col("snow_depth_mm") >= 0))
            .where((F.col("avg_wind_speed_kmh").isNull()) | (F.col("avg_wind_speed_kmh") >= 0))
            .where(
                (F.col("min_temp_c").isNull())
                | (F.col("avg_temp_c").isNull())
                | (F.col("max_temp_c").isNull())
                | (
                    (F.col("min_temp_c") <= F.col("avg_temp_c"))
                    & (F.col("avg_temp_c") <= F.col("max_temp_c"))
                )
            )
            .dropDuplicates()
        )

        jdbc_props = {
            "user": args.jdbc_user,
            "password": args.jdbc_password,
            "driver": args.jdbc_driver,
        }

        cities_df = spark.read.jdbc(url=args.jdbc_url, table=args.cities_table, properties=jdbc_props)
        countries_df = spark.read.jdbc(url=args.jdbc_url, table=args.countries_table, properties=jdbc_props)
        location_df = cities_df.join(countries_df, on="country", how="left")

        enriched_df = valid_df.join(location_df, on="city_name", how="left")

        monthly_agg = (
            enriched_df
            .groupBy("city_name", "weather_year", "weather_month")
            .agg(
                F.avg("avg_temp_c").alias("city_month_avg_temp_c"),
                F.sum("precipitation_mm").alias("city_month_total_precipitation_mm"),
                F.max("peak_wind_gust_kmh").alias("city_month_peak_wind_gust_kmh"),
            )
        )

        merged_df = enriched_df.join(
            monthly_agg,
            on=["city_name", "weather_year", "weather_month"],
            how="left",
        )

        temp_window = Window.partitionBy("city_name", "weather_year", "weather_month")
        final_df = (
            merged_df
            .withColumn("city_month_avg_temp_window", F.avg("avg_temp_c").over(temp_window))
            .withColumn(
                "temp_delta_from_city_month_avg",
                F.col("avg_temp_c") - F.col("city_month_avg_temp_window"),
            )
            .persist()
        )

        silver_count = final_df.count()
        logger.info("Silver rows curated: %s", silver_count)

        (
            final_df
            .write
            .mode(args.write_mode)
            .partitionBy("year", "month", "day")
            .parquet(args.silver_output_path)
        )

        (
            spark.read.parquet(args.silver_output_path)
            .write
            .mode(args.write_mode)
            .format("parquet")
            .partitionBy("year", "month", "day")
            .saveAsTable(args.silver_table)
        )

        logger.info(
            "Silver write complete to %s and table %s",
            args.silver_output_path,
            args.silver_table,
        )

    except Exception:
        logger.error("Processor execution failed", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()