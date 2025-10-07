import argparse, re
from pyspark.sql import SparkSession, functions as F, types as T
import psycopg2
import os

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--silver_root", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--db_url", required=True)
    p.add_argument("--db_user", required=True)
    p.add_argument("--db_pass", required=True)
    return p.parse_args()

def parse_jdbc_url(jdbc_url):
    # jdbc:postgresql://host:port/db
    m = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)", jdbc_url)
    if not m:
        raise ValueError(f"Unrecognized JDBC URL: {jdbc_url}")
    host, port, database = m.group(1), (m.group(2) or "5432"), m.group(3)
    return host, int(port), database

def merge_sql(target, staging, key_cols, update_cols):
    on = " AND ".join([f"t.{c}=s.{c}" for c in key_cols])
    set_clause = ", ".join([f'{c}=s.{c}' for c in update_cols if c not in key_cols])
    cols = key_cols + [c for c in update_cols if c not in key_cols]
    cols_csv = ", ".join(cols)
    return f"""
    MERGE INTO {target} AS t
    USING {staging} AS s
      ON ({on})
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({cols_csv})
      VALUES ({", ".join([f"s.{c}" for c in cols])});
    """

def upsert_batch(df, table, staging, key_cols, all_cols, jdbc_opts):
    if df.rdd.isEmpty():
        return
    # write to staging via Spark JDBC
    (df.write
       .format("jdbc")
       .mode("append")
       .options(**jdbc_opts, dbtable=staging)
       .save())
    # run MERGE + clean staging via psycopg2
    host, port, database = parse_jdbc_url(jdbc_opts["url"])
    conn = psycopg2.connect(host=host, port=port, dbname=database,
                            user=jdbc_opts["user"], password=jdbc_opts["password"])
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(merge_sql(table, staging, key_cols, all_cols))
        cur.execute(f"TRUNCATE TABLE {staging};")
    conn.close()

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("silver-to-postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_opts = {
        "url": args.db_url,
        "user": args.db_user,
        "password": args.db_pass,
        "driver": "org.postgresql.Driver",
    }

    # Define explicit schemas for stable writes
    album_schema = T.StructType([
        T.StructField("album_id", T.StringType()),
        T.StructField("album_name", T.StringType()),
        T.StructField("release_date", T.StringType()),
        T.StructField("release_date_precision", T.StringType()),
        T.StructField("label", T.StringType()),
        T.StructField("total_tracks", T.IntegerType()),
        T.StructField("market", T.StringType()),
        T.StructField("artists", T.StringType()),
        T.StructField("fetched_at", T.TimestampType()),
        T.StructField("ingested_at", T.TimestampType()),
    ])
    track_schema = T.StructType([
        T.StructField("track_id", T.StringType()),
        T.StructField("track_name", T.StringType()),
        T.StructField("album_id", T.StringType()),
        T.StructField("disc_number", T.IntegerType()),
        T.StructField("track_number", T.IntegerType()),
        T.StructField("duration_ms", T.IntegerType()),
        T.StructField("explicit", T.BooleanType()),
        T.StructField("market", T.StringType()),
        T.StructField("release_date", T.StringType()),
        T.StructField("ingested_at", T.TimestampType()),
    ])
    meta_schema = T.StructType([
        T.StructField("track_id", T.StringType()),
        T.StructField("album_id", T.StringType()),
        T.StructField("primary_artist_id", T.StringType()),
        T.StructField("popularity", T.IntegerType()),
        T.StructField("explicit", T.BooleanType()),
        T.StructField("duration_ms", T.IntegerType()),
        T.StructField("available_markets_count", T.IntegerType()),
        T.StructField("ingested_at", T.TimestampType()),
    ])

    for d in ["album","artist","track","track_meta"]:
        os.makedirs(os.path.join(args.silver_root, d), exist_ok=True)

    album_stream = (spark.readStream.schema(album_schema).format("parquet").load(f"{args.silver_root}/album"))
    track_stream = (spark.readStream.schema(track_schema).format("parquet").load(f"{args.silver_root}/track"))
    meta_stream  = (spark.readStream.schema(meta_schema ).format("parquet").load(f"{args.silver_root}/track_meta"))

    # foreachBatch upserts
    def upsert_albums(df, epoch_id):
        # cast artists JSON text -> PG jsonb automatically by ::jsonb via MERGE (raw text accepted)
        upsert_batch(
            df=df.select(
                "album_id","album_name","release_date","release_date_precision",
                "label","total_tracks","market","artists","fetched_at","ingested_at"
            ),
            table="spotify_album",
            staging="stg_spotify_album",
            key_cols=["album_id"],  # album_id uniquely identifies; market kept as attribute
            all_cols=["album_id","album_name","release_date","release_date_precision",
                      "label","total_tracks","market","artists","fetched_at","ingested_at"],
            jdbc_opts=jdbc_opts
        )

    def upsert_tracks(df, epoch_id):
        upsert_batch(
            df=df.select(
                "track_id","track_name","album_id","disc_number","track_number",
                "duration_ms","explicit","market","release_date","ingested_at"
            ),
            table="spotify_track",
            staging="stg_spotify_track",
            key_cols=["track_id"],
            all_cols=["track_id","track_name","album_id","disc_number","track_number",
                      "duration_ms","explicit","market","release_date","ingested_at"],
            jdbc_opts=jdbc_opts
        )

    def upsert_meta(df, epoch_id):
        upsert_batch(
            df=df.select(
                "track_id","album_id","primary_artist_id","popularity","explicit",
                "duration_ms","available_markets_count","ingested_at"
            ),
            table="spotify_track_meta",
            staging="stg_spotify_track_meta",
            key_cols=["track_id"],
            all_cols=["track_id","album_id","primary_artist_id","popularity","explicit",
                      "duration_ms","available_markets_count","ingested_at"],
            jdbc_opts=jdbc_opts
        )

    q1 = (album_stream
          .writeStream
          .outputMode("update")   # micro-batches as they appear
          .option("checkpointLocation", f"{args.checkpoint}/albums_to_db")
          .foreachBatch(upsert_albums)
          .start())

    q2 = (track_stream
          .writeStream
          .outputMode("update")
          .option("checkpointLocation", f"{args.checkpoint}/tracks_to_db")
          .foreachBatch(upsert_tracks)
          .start())

    q3 = (meta_stream
          .writeStream
          .outputMode("update")
          .option("checkpointLocation", f"{args.checkpoint}/meta_to_db")
          .foreachBatch(upsert_meta)
          .start())

    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()

if __name__ == "__main__":
    main()