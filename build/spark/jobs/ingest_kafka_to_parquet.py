import argparse
from pyspark.sql import SparkSession, functions as F, types as T
import os

SCHEMA = T.StructType([
    T.StructField("market", T.StringType()),
    T.StructField("ingest_ts", T.StringType()),
    T.StructField("album_id", T.StringType()),
    T.StructField("album_name", T.StringType()),
    T.StructField("album_artists", T.StringType()),
    T.StructField("album_total_tracks", T.IntegerType()),
    T.StructField("album_release_date", T.StringType()),
    T.StructField("album_release_date_precision", T.StringType()),
    T.StructField("track_id", T.StringType()),
    T.StructField("track_name", T.StringType()),
    T.StructField("track_number", T.IntegerType()),
    T.StructField("track_duration_ms", T.LongType()),
    T.StructField("track_popularity", T.IntegerType()),
])

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", required=True)
    p.add_argument("--kafka-topic", required=True)
    p.add_argument("--out-path", required=True)
    p.add_argument("--checkpoint", required=True)
    return p.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("ingest-kafka-to-parquet")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka Stream lesen
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.kafka_topic)
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS","latest"))
        .load()
    )

    # value ist bytes -> string -> json -> schema
    parsed = (
        raw.select(F.col("key").cast("string").alias("key"),
                   F.col("value").cast("string").alias("value"))
           .withColumn("json", F.from_json("value", SCHEMA))
           .select("key", "json.*")
           .withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
           .withColumn("ingest_date", F.to_date("ingest_ts"))
    )

    # Partitionierung: market / ingest_date
    writer = (
        parsed.writeStream
        .format("parquet")
        .option("path", args.out_path)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("market", "ingest_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
    )

    q = writer.start()
    q.awaitTermination()

if __name__ == "__main__":
    main()