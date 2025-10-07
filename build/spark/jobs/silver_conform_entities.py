import argparse
from pyspark.sql import SparkSession, functions as F, types as T

BRONZE_SCHEMA = T.StructType([
    T.StructField("market", T.StringType()),
    T.StructField("ingest_ts", T.TimestampType()),
    T.StructField("album_id", T.StringType()),
    T.StructField("album_name", T.StringType()),
    T.StructField("album_artists", T.StringType()),   # comma-separated names
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

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver_root", required=True)
    p.add_argument("--checkpoint", required=True)
    return p.parse_args()

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("silver-conform-entities").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    src = (spark.readStream
           .schema(BRONZE_SCHEMA)
           .format("parquet")
           .load(args.bronze))

    # Albums (JSON array of artist names)
    albums = (src
      .select(
        F.col("album_id"),
        F.col("album_name"),
        F.col("album_release_date").alias("release_date"),
        F.col("album_release_date_precision").alias("release_date_precision"),
        F.lit(None).cast("string").alias("label"),
        F.col("album_total_tracks").alias("total_tracks"),
        F.col("market"),
        F.when(F.col("album_artists").isNotNull(),
               F.to_json(F.split(F.col("album_artists"), ",\\s*"))
        ).alias("artists"),
        F.col("ingest_ts").alias("fetched_at"),
        F.current_timestamp().alias("ingested_at"),
      ).dropDuplicates(["album_id", "market"])
    )

    # Tracks (basic)
    tracks = (src
      .select(
        F.col("track_id"),
        F.col("track_name"),
        F.col("album_id"),
        F.lit(None).cast("int").alias("disc_number"),
        F.col("track_number"),
        F.col("track_duration_ms").cast("int").alias("duration_ms"),
        F.lit(None).cast("boolean").alias("explicit"),
        F.col("market"),
        F.col("album_release_date").alias("release_date"),
        F.current_timestamp().alias("ingested_at"),
      ).dropDuplicates(["track_id", "market"])
    )

    # Track meta (use what we have)
    track_meta = (src
      .select(
        F.col("track_id"),
        F.col("album_id"),
        F.lit(None).cast("string").alias("primary_artist_id"),
        F.col("track_popularity").cast("int").alias("popularity"),
        F.lit(None).cast("boolean").alias("explicit"),
        F.col("track_duration_ms").cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("available_markets_count"),
        F.current_timestamp().alias("ingested_at"),
      ).dropDuplicates(["track_id"])
    )

    # Helpers to write each entity to its own Silver dir
    def writer(path):
        return (dict(
            format="parquet",
            options={"path": path, "checkpointLocation": f"{args.checkpoint}/{path.split('/')[-1]}"},
            mode="append",
            trigger="30 seconds",
            partitionBy=None
        ))

    w_albums = writer(f"{args.silver_root}/album")
    w_tracks = writer(f"{args.silver_root}/track")
    w_track_meta = writer(f"{args.silver_root}/track_meta")

    q1 = (albums.writeStream
          .format(w_albums["format"])
          .options(**w_albums["options"])
          .outputMode("append")
          .start())
    q2 = (tracks.writeStream
          .format(w_tracks["format"])
          .options(**w_tracks["options"])
          .outputMode("append")
          .start())
    q3 = (track_meta.writeStream
          .format(w_track_meta["format"])
          .options(**w_track_meta["options"])
          .outputMode("append")
          .start())

    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()

if __name__ == "__main__":
    main()
