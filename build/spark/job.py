import os
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, expr
from pyspark.sql.types import *

# --------- Config from env ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR",  "/checkpoints")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB   = os.getenv("PG_DB",   "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spotify")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_URL      = os.getenv("PG_URL",      "jdbc:postgresql://postgres:5432/spotify")
PG_DRIVER   = "org.postgresql.Driver"

# --------- Schemas ----------
album_schema = StructType([
    StructField("album_id", StringType()),
    StructField("album_name", StringType()),
    StructField("release_date", StringType()),
    StructField("release_date_precision", StringType()),
    StructField("artists", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ]))),
    StructField("market", StringType()),
    StructField("fetched_at", StringType()),
    StructField("label", StringType()),
    StructField("total_tracks", IntegerType()),
])

track_schema = StructType([
    StructField("track_id", StringType()),
    StructField("track_name", StringType()),
    StructField("disc_number", IntegerType()),
    StructField("track_number", IntegerType()),
    StructField("duration_ms", IntegerType()),
    StructField("explicit", BooleanType()),
    StructField("album_id", StringType()),
    StructField("market", StringType()),
    StructField("release_date", StringType()),
])

tmeta_schema = StructType([
    StructField("track_id", StringType()),
    StructField("popularity", IntegerType()),
    StructField("explicit", BooleanType()),
    StructField("duration_ms", IntegerType()),
    StructField("album_id", StringType()),
    StructField("primary_artist_id", StringType()),
    StructField("available_markets_count", IntegerType()),
])

artist_schema = StructType([
    StructField("artist_id", StringType()),
    StructField("artist_name", StringType()),
    StructField("genres", ArrayType(StringType())),
    StructField("popularity", IntegerType()),
])

# --------- Spark session ----------
spark = (SparkSession.builder
         .appName("spotify-kafka-to-postgres")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def read_kafka(topic: str) -> DataFrame:
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("json"),
                col("timestamp").alias("kafka_ts")
            ))

def with_parsed(df: DataFrame, schema: StructType) -> DataFrame:
    parsed = df.select(
        "kafka_key", "kafka_ts",
        from_json(col("json"), schema).alias("obj")
    ).select("kafka_key", "kafka_ts", "obj.*")
    return parsed.withColumn("ingested_at", current_timestamp())

albums = with_parsed(read_kafka("spotify.new_releases.album"), album_schema) \
    .withColumn("fetched_at_ts", to_timestamp(col("fetched_at"))) \
    .dropDuplicates(["album_id"])

tracks = with_parsed(read_kafka("spotify.new_releases.track"), track_schema) \
    .dropDuplicates(["track_id"])

tmeta  = with_parsed(read_kafka("spotify.track_meta"), tmeta_schema) \
    .dropDuplicates(["track_id"])

artists = with_parsed(read_kafka("spotify.artist_meta"), artist_schema) \
    .dropDuplicates(["artist_id"])

import psycopg2
from pyspark.sql.functions import expr

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB   = os.getenv("PG_DB",   "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spotify")
PG_PORT = int(os.getenv("PG_PORT", "5432"))

def foreach_upsert(table: str, key_cols: list):
    def _fn(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        df = batch_df
        if table == "spotify_album":
            df = (
                df.withColumn("artists", expr("to_json(artists)"))
                  .drop("fetched_at")                         # <<< drop the original string column
                  .withColumnRenamed("fetched_at_ts", "fetched_at")  # keep the timestamp one
            )
            cols = [
                "album_id","album_name","release_date","release_date_precision",
                "label","total_tracks","market","artists","fetched_at","ingested_at"
            ]
        elif table == "spotify_track":
            cols = ["track_id","track_name","album_id","disc_number","track_number",
                    "duration_ms","explicit","market","release_date","ingested_at"]
        elif table == "spotify_track_meta":
            cols = ["track_id","album_id","primary_artist_id","popularity","explicit",
                    "duration_ms","available_markets_count","ingested_at"]
        elif table == "spotify_artist_meta":
            df = df.withColumn("genres", expr("to_json(genres)"))
            cols = ["artist_id","artist_name","genres","popularity","ingested_at"]
        else:
            raise ValueError(f"Unknown table {table}")

        df = df.select(*cols)
        tmp = f"{table}_stg_{batch_id}"

        # 1) stage to JDBC
        (df.write.format("jdbc")
           .option("url", PG_URL)
           .option("user", PG_USER)
           .option("password", PG_PASSWORD)
           .option("driver", PG_DRIVER)
           .option("dbtable", tmp)
           .mode("overwrite")
           .save())

        # 2) merge via psycopg2
        key_set = set(key_cols)
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in key_set])

        # Cast JSON text -> JSONB when merging
        select_cols = []
        for c in cols:
            if table == "spotify_album" and c == "artists":
                select_cols.append(f"({c})::jsonb")
            elif table == "spotify_artist_meta" and c == "genres":
                select_cols.append(f"({c})::jsonb")
            else:
                select_cols.append(c)
        select_list_sql = ", ".join(select_cols)
        col_list = ", ".join(cols)

        merge_sql = f"""
            INSERT INTO {table} ({col_list})
            SELECT {select_list_sql} FROM {tmp}
            ON CONFLICT ({", ".join(key_cols)})
            DO UPDATE SET {set_clause};
            DROP TABLE IF EXISTS {tmp};
        """

        conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(merge_sql)
        conn.close()
    return _fn

# Start streaming sinks
q1 = (albums.writeStream
      .outputMode("update")
      .foreachBatch(foreach_upsert("spotify_album", ["album_id"]))
      .option("checkpointLocation", f"{CHECKPOINT_DIR}/albums")
      .start())

q2 = (tracks.writeStream
      .outputMode("update")
      .foreachBatch(foreach_upsert("spotify_track", ["track_id"]))
      .option("checkpointLocation", f"{CHECKPOINT_DIR}/tracks")
      .start())

q3 = (tmeta.writeStream
      .outputMode("update")
      .foreachBatch(foreach_upsert("spotify_track_meta", ["track_id"]))
      .option("checkpointLocation", f"{CHECKPOINT_DIR}/tmeta")
      .start())

q4 = (artists.writeStream
      .outputMode("update")
      .foreachBatch(foreach_upsert("spotify_artist_meta", ["artist_id"]))
      .option("checkpointLocation", f"{CHECKPOINT_DIR}/artists")
      .start())

spark.streams.awaitAnyTermination()

