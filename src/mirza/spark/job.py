import os
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    expr,
    explode,
    to_json,
    lit,
)
from pyspark.sql.types import *

# --------- Config from env ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spotify")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/spotify")
PG_DRIVER = "org.postgresql.Driver"

# --------- Schemas ----------
album_schema = StructType(
    [
        StructField("album_id", StringType()),
        StructField("album_name", StringType()),
        StructField("release_date", StringType()),
        StructField("release_date_precision", StringType()),
        StructField(
            "artists",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType()),
                        StructField("name", StringType()),
                    ]
                )
            ),
        ),
        StructField("market", StringType()),
        StructField("fetched_at", StringType()),
        StructField("label", StringType()),
        StructField("total_tracks", IntegerType()),
    ]
)

track_schema = StructType(
    [
        StructField("track_id", StringType()),
        StructField("track_name", StringType()),
        StructField("disc_number", IntegerType()),
        StructField("track_number", IntegerType()),
        StructField("duration_ms", IntegerType()),
        StructField("explicit", BooleanType()),
        StructField("album_id", StringType()),
        StructField("market", StringType()),
        StructField("release_date", StringType()),
    ]
)

tmeta_schema = StructType(
    [
        StructField("track_id", StringType()),
        StructField("popularity", IntegerType()),
        StructField("explicit", BooleanType()),
        StructField("duration_ms", IntegerType()),
        StructField("album_id", StringType()),
        StructField("primary_artist_id", StringType()),
        StructField("available_markets_count", IntegerType()),
    ]
)

artist_schema = StructType(
    [
        StructField("artist_id", StringType()),
        StructField("artist_name", StringType()),
        StructField("genres", ArrayType(StringType())),
        StructField("popularity", IntegerType()),
    ]
)

# --------- GetSongBPM schema (artist_bpm_meta) ----------
# Wir halten das Event generisch: items als Map/String-Pairs, damit Rohdaten robust bleiben.
getsong_event_schema = StructType(
    [
        StructField("source", StringType()),
        StructField(
            "artist",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                ]
            ),
        ),
        StructField("queried_at", StringType()),
        StructField("count", IntegerType()),
        # items kann variable Felder enthalten -> als Map(String,String) für robustes Parsen
        StructField("items", ArrayType(MapType(StringType(), StringType(), True), True)),
    ]
)

# --------- Spark session ----------
spark = (
    SparkSession.builder.appName("spotify-kafka-to-postgres")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def read_kafka(topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json"),
            col("timestamp").alias("kafka_ts"),
        )
    )

def with_parsed(df: DataFrame, schema: StructType) -> DataFrame:
    parsed = df.select("kafka_key", "kafka_ts", from_json(col("json"), schema).alias("obj"))
    parsed = parsed.select("kafka_key", "kafka_ts", "obj.*")
    return parsed.withColumn("ingested_at", current_timestamp())

# --------- Streams (bestehende Topics) ----------
albums = (
    with_parsed(read_kafka("spotify.new_releases.album"), album_schema)
    .withColumn("fetched_at_ts", to_timestamp(col("fetched_at")))
    .dropDuplicates(["album_id"])
)

tracks = with_parsed(read_kafka("spotify.new_releases.track"), track_schema).dropDuplicates(
    ["track_id"]
)

tmeta = with_parsed(read_kafka("spotify.track_meta"), tmeta_schema).dropDuplicates(
    ["track_id"]
)

artists = with_parsed(read_kafka("spotify.artist_meta"), artist_schema).dropDuplicates(
    ["artist_id"]
)

# --------- GetSongBPM Stream ----------
bpm_events = with_parsed(read_kafka("spotify.artist_bpm_meta"), getsong_event_schema)

# Eine Zeile pro Event für artist_bpm_queries
bpm_queries_df = bpm_events.select(
    col("kafka_key").alias("event_key"),
    col("artist.id").alias("artist_id"),
    col("artist.name").alias("artist_name"),
    to_timestamp(col("queried_at")).alias("queried_at"),
    col("count").alias("item_count"),
    col("ingested_at"),
)

# Explode items -> artist_bpm_items (mit Rohdaten als JSON-String)
bpm_items_df = (
    bpm_events.withColumn("item", explode(col("items")))
    .select(
        col("kafka_key").alias("event_key"),
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        to_timestamp(col("queried_at")).alias("queried_at"),
        to_json(col("item")).alias("raw_item_json"),
        col("item")["id"].alias("item_id"),
        col("item")["title"].alias("title"),
        col("item")["artist.name"].alias("item_artist_name"),
        col("item")["bpm"].cast("double").alias("bpm"),
        col("item")["key"].alias("musical_key"),
        col("item")["camelot"].alias("camelot"),
        col("item")["energy"].cast("double").alias("energy"),
        col("item")["danceability"].cast("double").alias("danceability"),
        col("ingested_at"),
    )
)

# --------- JDBC Upserts (bestehende Logik) ----------
import psycopg2
from psycopg2.extras import execute_values

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "spotify")
PG_USER = os.getenv("PG_USER", "spotify")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spotify")
PG_PORT = int(os.getenv("PG_PORT", "5432"))

def foreach_upsert(table: str, key_cols: list):
    def _fn(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        df = batch_df
        if table == "spotify_album":
            df = df.withColumn("artists", expr("to_json(artists)")).drop("fetched_at").withColumnRenamed(
                "fetched_at_ts", "fetched_at"
            )
            cols = [
                "album_id",
                "album_name",
                "release_date",
                "release_date_precision",
                "label",
                "total_tracks",
                "market",
                "artists",
                "fetched_at",
                "ingested_at",
            ]
        elif table == "spotify_track":
            cols = [
                "track_id",
                "track_name",
                "album_id",
                "disc_number",
                "track_number",
                "duration_ms",
                "explicit",
                "market",
                "release_date",
                "ingested_at",
            ]
        elif table == "spotify_track_meta":
            cols = [
                "track_id",
                "album_id",
                "primary_artist_id",
                "popularity",
                "explicit",
                "duration_ms",
                "available_markets_count",
                "ingested_at",
            ]
        elif table == "spotify_artist_meta":
            df = df.withColumn("genres", expr("to_json(genres)"))
            cols = ["artist_id", "artist_name", "genres", "popularity", "ingested_at"]
        else:
            raise ValueError(f"Unknown table {table}")

        df = df.select(*cols)
        tmp = f"{table}_stg_{batch_id}"

        # 1) stage to JDBC
        (
            df.write.format("jdbc")
            .option("url", PG_URL)
            .option("user", PG_USER)
            .option("password", PG_PASSWORD)
            .option("driver", PG_DRIVER)
            .option("dbtable", tmp)
            .mode("overwrite")
            .save()
        )

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

# --------- Neue Writer für GetSongBPM ---------
def foreach_bpm_queries(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    rows = [
        tuple(r)
        for r in batch_df.select(
            "artist_id", "artist_name", "queried_at", "item_count"
        ).collect()
    ]
    if not rows:
        return

    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        sql = """
        INSERT INTO artist_bpm_queries (artist_id, artist_name, queried_at, item_count)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """
        execute_values(cur, sql, rows, page_size=1000)
    conn.close()

def foreach_bpm_items(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.select(
        "artist_id",
        "artist_name",
        "queried_at",
        "item_id",
        "title",
        "item_artist_name",
        "bpm",
        "musical_key",
        "camelot",
        "energy",
        "danceability",
        "raw_item_json",
    ).toPandas()

    if pdf.empty:
        return

    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        # Mapping (artist_id, artist_name, queried_at) -> query_id
        keys = pdf[["artist_id", "artist_name", "queried_at"]].drop_duplicates()
        mapping = {}
        for aid, aname, qts in keys.to_records(index=False):
            cur.execute(
                """
                SELECT query_id FROM artist_bpm_queries
                WHERE ((%s IS NOT NULL AND artist_id = %s)
                       OR (%s IS NULL AND artist_id IS NULL AND artist_name = %s))
                  AND queried_at = %s
                ORDER BY query_id DESC
                LIMIT 1
                """,
                (aid, aid, aid, aname, qts),
            )
            row = cur.fetchone()
            if row:
                mapping[(aid, aname, qts)] = row[0]

        insert_rows = []
        for _, r in pdf.iterrows():
            key = (r["artist_id"], r["artist_name"], r["queried_at"])
            qid = mapping.get(key)
            if not qid:
                # Fallback: Query-Row erzeugen, falls noch nicht vorhanden
                cur.execute(
                    """
                    INSERT INTO artist_bpm_queries (artist_id, artist_name, queried_at, item_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING query_id
                    """,
                    (r["artist_id"], r["artist_name"], r["queried_at"], 0),
                )
                ret = cur.fetchone()
                if ret:
                    qid = ret[0]
                    mapping[key] = qid
                else:
                    cur.execute(
                        """
                        SELECT query_id FROM artist_bpm_queries
                        WHERE ((%s IS NOT NULL AND artist_id = %s)
                               OR (%s IS NULL AND artist_id IS NULL AND artist_name = %s))
                          AND queried_at = %s
                        ORDER BY query_id DESC
                        LIMIT 1
                        """,
                        (
                            r["artist_id"],
                            r["artist_id"],
                            r["artist_id"],
                            r["artist_name"],
                            r["queried_at"],
                        ),
                    )
                    row2 = cur.fetchone()
                    qid = row2[0] if row2 else None

            if not qid:
                continue

            insert_rows.append(
                (
                    int(qid),
                    (r["item_id"] if r["item_id"] is not None else ""),
                    r["title"],
                    r["item_artist_name"],
                    float(r["bpm"]) if r["bpm"] is not None else None,
                    r["musical_key"],
                    r["camelot"],
                    float(r["energy"]) if r["energy"] is not None else None,
                    float(r["danceability"]) if r["danceability"] is not None else None,
                    r["raw_item_json"],
                )
            )

        if insert_rows:
            sql = """
            INSERT INTO artist_bpm_items
            (query_id, item_id, title, artist_name, bpm, musical_key, camelot, energy, danceability, raw)
            VALUES %s
            ON CONFLICT (query_id, item_id)
            DO UPDATE SET
              title = EXCLUDED.title,
              artist_name = EXCLUDED.artist_name,
              bpm = EXCLUDED.bpm,
              musical_key = EXCLUDED.musical_key,
              camelot = EXCLUDED.camelot,
              energy = EXCLUDED.energy,
              danceability = EXCLUDED.danceability,
              raw = EXCLUDED.raw::jsonb;
            """
            execute_values(cur, sql, insert_rows, page_size=1000)
    conn.close()

# --------- Start streaming sinks ----------
q1 = (
    albums.writeStream.outputMode("update")
    .foreachBatch(foreach_upsert("spotify_album", ["album_id"]))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/albums")
    .start()
)

q2 = (
    tracks.writeStream.outputMode("update")
    .foreachBatch(foreach_upsert("spotify_track", ["track_id"]))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/tracks")
    .start()
)

q3 = (
    tmeta.writeStream.outputMode("update")
    .foreachBatch(foreach_upsert("spotify_track_meta", ["track_id"]))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/tmeta")
    .start()
)

q4 = (
    artists.writeStream.outputMode("update")
    .foreachBatch(foreach_upsert("spotify_artist_meta", ["artist_id"]))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/artists")
    .start()
)

# Neue Sinks
q5 = (
    bpm_queries_df.writeStream.outputMode("update")
    .foreachBatch(foreach_bpm_queries)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/getsongbpm_queries")
    .start()
)

q6 = (
    bpm_items_df.writeStream.outputMode("update")
    .foreachBatch(foreach_bpm_items)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/getsongbpm_items")
    .start()
)

spark.streams.awaitAnyTermination()