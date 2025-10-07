import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in-path", required=True)
    p.add_argument("--out-path", required=True)
    p.add_argument("--checkpoint", required=True)
    return p.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("silver-window-agg")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Streaming File Source (neue Parquet-Dateien)
    src = (
        spark.readStream
        .schema(  # Schema explizit angeben (schneller & stabiler)
            T.StructType([
                T.StructField("market", T.StringType()),
                T.StructField("ingest_ts", T.TimestampType()),
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
                T.StructField("ingest_date", T.DateType()),
            ])
        )
        .format("parquet")
        .load(args.in_path)
    )

    # Windowed KPIs (z.B. pro 5 Min, sliding 5 Min)
    agg = (
        src.withWatermark("ingest_ts", "10 minutes")
           .groupBy(
               F.window("ingest_ts", "5 minutes"),
               F.col("market"))
           .agg(
               F.countDistinct("track_id").alias("distinct_tracks"),
               F.avg("track_popularity").alias("avg_popularity"))
           .select(
               F.col("market"),
               F.col("window.start").alias("window_start"),
               F.col("window.end").alias("window_end"),
               "distinct_tracks",
               F.round("avg_popularity", 2).alias("avg_popularity"))
    )

    # Output als Parquet (Silver), zusätzlich nach market partitionieren
    q = (
        agg.writeStream
           .format("parquet")
           .option("path", args.out_path)
           .option("checkpointLocation", args.checkpoint)
           .partitionBy("market")
           .outputMode("append")
           .trigger(processingTime="1 minute")
           .start()
    )

    q.awaitTermination()

if __name__ == "__main__":
    main()