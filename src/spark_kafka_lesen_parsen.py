from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, current_timestamp
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("spotify-new-releases")
         .getOrCreate())

# Kafka-Quelle
df_kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "spotify.new_releases.raw")
  .option("startingOffsets", "latest")
  .load())

# JSON-Schema (vereinfacht)
schema = StructType([
    StructField("fetched_at", StringType()),
    StructField("market", StringType()),
    StructField("album", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("artists", ArrayType(StringType())),
        StructField("total_tracks", IntegerType()),
        StructField("release_date", StringType()),
        StructField("release_date_precision", StringType())
    ])),
    StructField("track", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("number", IntegerType()),
        StructField("duration_ms", IntegerType()),
        StructField("popularity", IntegerType())
    ]))
])

parsed = (df_kafka
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .select(from_json(col("value"), schema).alias("js"))
)

albums_df = (parsed.select(
    col("js.album.id").alias("album_id"),
    col("js.album.name").alias("album_name"),
    col("js.album.total_tracks").alias("album_total_tracks"),
    col("js.album.release_date").alias("album_release_date_str"),
    col("js.album.release_date_precision").alias("album_release_date_precision"),
    current_timestamp().alias("ingested_at")
).withColumn("album_release_date",
             to_timestamp("album_release_date_str").cast("date"))
 .drop("album_release_date_str")
 .dropDuplicates(["album_id"])
)

tracks_df = (parsed.select(
    col("js.track.id").alias("track_id"),
    col("js.track.name").alias("track_name"),
    col("js.track.number").alias("track_number"),
    col("js.track.duration_ms").alias("track_duration_ms"),
    col("js.track.popularity").alias("track_popularity"),
    col("js.album.id").alias("album_id"),
    current_timestamp().alias("ingested_at")
).dropDuplicates(["track_id"])
)

# Optional: Popularitäts-Snapshots je Tag
from pyspark.sql.functions import to_date
pop_hist_df = (tracks_df
  .withColumn("snapshot_date", to_date(col("ingested_at")))
  .select("track_id", "snapshot_date", "track_popularity")
)
