import argparse
import logging
import os

from pyspark.sql import SparkSession, functions as F


def build_parser():
    parser = argparse.ArgumentParser(description="Ingest open data weather files into raw layer.")
    parser.add_argument("--input-path", default=os.getenv("FEEDER_INPUT_PATH"))
    parser.add_argument("--raw-output-path", default=os.getenv("FEEDER_RAW_OUTPUT_PATH"))
    parser.add_argument("--run-date", default=os.getenv("RUN_DATE"))
    parser.add_argument("--app-name", default="weather-feeder-raw")
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--output-partitions", type=int, default=4)
    parser.add_argument("--log-file", default=os.getenv("FEEDER_LOG_FILE", "/opt/pipeline/logs/feeder.txt"))
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
    return logging.getLogger("feeder")


def require_args(args, logger):
    missing = []
    if not args.input_path:
        missing.append("--input-path or FEEDER_INPUT_PATH")
    if not args.raw_output_path:
        missing.append("--raw-output-path or FEEDER_RAW_OUTPUT_PATH")

    if missing:
        message = "Missing required configuration: {}".format(", ".join(missing))
        logger.error(message)
        raise ValueError(message)


def main():
    args = build_parser().parse_args()
    logger = configure_logger(args.log_file)

    spark = (
        SparkSession.builder
        .appName(args.app_name)
        .getOrCreate()
    )

    try:
        require_args(args, logger)
        logger.info("Reading source data from %s", args.input_path)

        df_weather = spark.read.parquet(args.input_path)

        if args.run_date:
            ingestion_date = F.to_date(F.lit(args.run_date))
        else:
            ingestion_date = F.current_date()

        df_raw = (
            df_weather
            .withColumn("ingestion_date", ingestion_date)
            .withColumn("year", F.year(F.col("ingestion_date")))
            .withColumn("month", F.month(F.col("ingestion_date")))
            .withColumn("day", F.dayofmonth(F.col("ingestion_date")))
            .persist()
        )

        row_count = df_raw.count()
        logger.info("Raw rows ingested: %s", row_count)

        (
            df_raw.repartition(args.output_partitions)
            .write
            .mode(args.write_mode)
            .partitionBy("year", "month", "day")
            .parquet(args.raw_output_path)
        )

        logger.info(
            "Raw write complete to %s with partitioning year/month/day",
            args.raw_output_path,
        )

    except Exception:
        logger.error("Feeder execution failed", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
