# Exam Pipeline Runbook (spark-submit)

## 1) Feeder -> raw (partition ingestion date)

```bash
spark-submit /opt/pipeline/feeder.py \
  --input-path file:///source/daily_weather.parquet \
  --raw-output-path hdfs://namenode:9000/data/raw/weather_daily \
  --run-date 2026-03-28 \
  --write-mode overwrite \
  --output-partitions 4 \
  --log-file /opt/pipeline/logs/feeder.txt
```

## 2) Processor -> silver (validation + join + agg + window)

```bash
spark-submit /opt/pipeline/processor.py \
  --raw-input-path hdfs://namenode:9000/data/raw/weather_daily \
  --silver-output-path hdfs://namenode:9000/data/silver/weather_curated \
  --silver-table default.weather_silver \
  --jdbc-url jdbc:mysql://mysql:3306/dataclysme \
  --jdbc-user root \
  --jdbc-password my-secret-pw \
  --cities-table cities \
  --countries-table countries \
  --write-mode overwrite \
  --log-file /opt/pipeline/logs/processor.txt
```

## 3) Datamart -> MySQL (relationnel)

```bash
spark-submit /opt/pipeline/datamart.py \
  --source-table default.weather_silver \
  --jdbc-url jdbc:mysql://mysql:3306/dataclysme \
  --jdbc-user root \
  --jdbc-password my-secret-pw \
  --risk-table dm_risks \
  --tourism-table dm_tourism \
  --agri-table dm_agriculture \
  --write-mode overwrite \
  --log-file /opt/pipeline/logs/datamart.txt
```

## 4) Quality checks

```bash
spark-submit /opt/pipeline/quality_checks.py \
  --bronze-path hdfs://namenode:9000/data/raw/weather_daily \
  --silver-table default.weather_silver \
  --jdbc-url jdbc:mysql://mysql:3306/dataclysme \
  --jdbc-user root \
  --jdbc-password my-secret-pw \
  --risk-table dm_risks \
  --tourism-table dm_tourism \
  --agri-table dm_agriculture
```

## 5) API test (Postman)

- POST http://localhost:8000/auth/login
- GET http://localhost:8000/api/v1/datamarts
- GET http://localhost:8000/api/v1/datamarts/risks?page=1&page_size=50

Use `Authorization: Bearer <token>` on protected endpoints.

## 6) Simple visualization (API-based, 3 charts)

Option A: with Docker Compose

```bash
docker compose up -d --build viz
```

Open http://localhost:8501

Option B: local Python

```bash
cd viz
pip install -r requirements.txt
streamlit run app.py
```

Notes:

- This satisfies the visualization requirement with 3 graphs based on datamarts through the API.
- Power BI is optional, not required for this exam if you provide a working visualization layer.
