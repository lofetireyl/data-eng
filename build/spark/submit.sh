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

# Gemeinsame Packages (Kafka + optional JDBC für später)
PKGS="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"

echo "[submit] starting bronze job (Kafka -> Parquet)"
/opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages "${PKGS}" \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  /opt/spark-app/jobs/ingest_kafka_to_parquet.py \
    --kafka-bootstrap "${KAFKA_BOOTSTRAP}" \
    --kafka-topic "${KAFKA_TOPIC}" \
    --out-path "${BRONZE_PATH}" \
    --checkpoint "${BRONZE_CHECKPOINT}" \
  &

if [ "${RUN_SILVER}" = "true" ]; then
  echo "[submit] starting silver job (Parquet -> windowed stats)"
  /opt/bitnami/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --packages "${PKGS}" \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    /opt/spark-app/jobs/silver_window_agg.py \
      --in-path "${BRONZE_PATH}" \
      --out-path "${SILVER_PATH}" \
      --checkpoint "${SILVER_CHECKPOINT}" \
    &
fi

# Halten, damit Container im Vordergrund bleibt
wait -n
