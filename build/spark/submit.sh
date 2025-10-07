#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_MASTER_URL:=spark://spark-master:7077}"
: "${KAFKA_BOOTSTRAP:=kafka:9092}"
: "${KAFKA_TOPIC:=spotify-tracks}"

: "${BRONZE_PATH:=/data/bronze}"
: "${BRONZE_CHECKPOINT:=/data/checkpoints/bronze}"

: "${RUN_SILVER:=false}"
: "${SILVER_PATH:=/data/silver}"
: "${SILVER_CHECKPOINT:=/data/checkpoints/silver}"

: "${RUN_DB_LOADER:=false}"
: "${DB_URL:=jdbc:postgresql://db:5432/spotifydb}"
: "${DB_USER:=user}"
: "${DB_PASS:=password}"

PKGS="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"

echo "[submit] starting bronze (Kafka -> Parquet)"
/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages "${PKGS}" \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  /opt/spark-app/jobs/ingest_kafka_to_parquet.py \
    --kafka-bootstrap "${KAFKA_BOOTSTRAP}" \
    --kafka-topic "${KAFKA_TOPIC}" \
    --out-path "${BRONZE_PATH}" \
    --checkpoint "${BRONZE_CHECKPOINT}" &

if [ "${RUN_SILVER}" = "true" ]; then
  echo "[submit] starting silver conform (Bronze -> Silver entities)"
  /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --packages "${PKGS}" \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    /opt/spark-app/jobs/silver_conform_entities.py \
      --bronze "${BRONZE_PATH}" \
      --silver_root "${SILVER_PATH}" \
      --checkpoint "${SILVER_CHECKPOINT}" &
fi

if [ "${RUN_DB_LOADER}" = "true" ]; then
  echo "[submit] starting silver -> postgres loader"
  /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --packages "${PKGS}" \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    /opt/spark-app/jobs/silver_to_postgres.py \
      --silver_root "${SILVER_PATH}" \
      --checkpoint "${SILVER_CHECKPOINT}/to_db" \
      --db_url "${DB_URL}" \
      --db_user "${DB_USER}" \
      --db_pass "${DB_PASS}" &
fi

wait -n
