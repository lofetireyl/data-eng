import pymysql  # Stelle sicher, dass verfügbar ist
from pyspark.sql.functions import lit

def write_upsert_albums(batch_df, batch_id):
    rows = (batch_df
        .selectExpr(
            "album_id",
            "album_name",
            "album_total_tracks",
            "CAST(album_release_date AS DATE) as album_release_date",
            "album_release_date_precision",
            "ingested_at as ts"
        ).collect())

    if not rows:
        return

    conn = pymysql.connect(host="mysql-host", user="user", password="pw", database="spotify", autocommit=True)
    cur = conn.cursor()
    sql = """
    INSERT INTO albums
      (album_id, album_name, album_total_tracks, album_release_date, album_release_date_precision, first_seen_at, last_seen_at)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      album_name=VALUES(album_name),
      album_total_tracks=VALUES(album_total_tracks),
      album_release_date=VALUES(album_release_date),
      album_release_date_precision=VALUES(album_release_date_precision),
      last_seen_at=VALUES(last_seen_at);
    """
    params = []
    for r in rows:
        params.append((
            r.album_id, r.album_name, r.album_total_tracks, r.album_release_date,
            r.album_release_date_precision, r.ts, r.ts
        ))
    cur.executemany(sql, params)
    cur.close(); conn.close()

def write_upsert_tracks(batch_df, batch_id):
    rows = (batch_df
        .selectExpr(
            "track_id", "album_id", "track_name", "track_number", "track_duration_ms", "ingested_at as ts"
        ).collect())
    if not rows:
        return
    conn = pymysql.connect(host="mysql-host", user="user", password="pw", database="spotify", autocommit=True)
    cur = conn.cursor()
    sql = """
    INSERT INTO tracks
      (track_id, album_id, track_name, track_number, track_duration_ms, first_seen_at, last_seen_at)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      album_id=VALUES(album_id),
      track_name=VALUES(track_name),
      track_number=VALUES(track_number),
      track_duration_ms=VALUES(track_duration_ms),
      last_seen_at=VALUES(last_seen_at);
    """
    params = []
    for r in rows:
        params.append((r.track_id, r.album_id, r.track_name, r.track_number, r.track_duration_ms, r.ts, r.ts))
    cur.executemany(sql, params)
    cur.close(); conn.close()

def write_upsert_pop_history(batch_df, batch_id):
    rows = batch_df.select("track_id", "snapshot_date", "track_popularity").collect()
    if not rows:
        return
    conn = pymysql.connect(host="mysql-host", user="user", password="pw", database="spotify", autocommit=True)
    cur = conn.cursor()
    sql = """
    INSERT INTO track_popularity_history
      (track_id, snapshot_date, popularity)
    VALUES
      (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
      popularity=VALUES(popularity);
    """
    cur.executemany(sql, [(r.track_id, r.snapshot_date, r.track_popularity) for r in rows])
    cur.close(); conn.close()

# Streams starten
(albums_df.writeStream
  .outputMode("update")
  .foreachBatch(write_upsert_albums)
  .option("checkpointLocation", "/chk/spotify/albums")
  .start())

(tracks_df.writeStream
  .outputMode("update")
  .foreachBatch(write_upsert_tracks)
  .option("checkpointLocation", "/chk/spotify/tracks")
  .start())

# Optional
(pop_hist_df.writeStream
  .outputMode("update")
  .foreachBatch(write_upsert_pop_history)
  .option("checkpointLocation", "/chk/spotify/pop_hist")
  .start())
