-- 01_schema.sql

-- Optional: create a dedicated schema
CREATE SCHEMA IF NOT EXISTS spotify AUTHORIZATION "user";
SET search_path TO spotify, public;

-- Postgres ENUM for release date precision
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'album_release_date_precision') THEN
    CREATE TYPE album_release_date_precision AS ENUM ('day','month','year');
  END IF;
END$$;

-- Albums
CREATE TABLE IF NOT EXISTS albums (
  album_id VARCHAR(64) PRIMARY KEY,
  album_name VARCHAR(512),
  album_total_tracks INT,
  album_release_date DATE NULL,
  album_release_date_precision album_release_date_precision NULL,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL
);

-- Tracks
CREATE TABLE IF NOT EXISTS tracks (
  track_id VARCHAR(64) PRIMARY KEY,
  album_id VARCHAR(64) NOT NULL,
  track_name VARCHAR(512),
  track_number INT,
  track_duration_ms INT,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL,
  CONSTRAINT fk_tracks_album
    FOREIGN KEY (album_id) REFERENCES albums(album_id) ON DELETE CASCADE
);

-- Track popularity history
CREATE TABLE IF NOT EXISTS track_popularity_history (
  track_id VARCHAR(64) NOT NULL,
  snapshot_date DATE NOT NULL,
  popularity INT,
  PRIMARY KEY (track_id, snapshot_date),
  CONSTRAINT fk_pop_track
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE
);

-- Artists
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR(64) PRIMARY KEY,
  artist_name VARCHAR(512)
);

-- Album <-> Artists (many-to-many)
CREATE TABLE IF NOT EXISTS album_artists (
  album_id VARCHAR(64) NOT NULL,
  artist_id VARCHAR(64) NOT NULL,
  PRIMARY KEY (album_id, artist_id),
  FOREIGN KEY (album_id) REFERENCES albums(album_id) ON DELETE CASCADE,
  FOREIGN KEY (artist_id) REFERENCES artists(artist_id) ON DELETE CASCADE
);


-- Pro API-Abfrage (Event aus spotify.artist_bpm_meta)
CREATE TABLE IF NOT EXISTS artist_bpm_queries (
  query_id BIGSERIAL PRIMARY KEY,
  artist_id VARCHAR(64) NULL,
  artist_name VARCHAR(512) NOT NULL,
  queried_at TIMESTAMP NOT NULL,
  item_count INT NOT NULL,
  received_at TIMESTAMP NOT NULL DEFAULT NOW()
);

--GetSongBPM Songs pro Abfrage
  CREATE TABLE IF NOT EXISTS artist_bpm_items (
    query_id BIGINT NOT NULL REFERENCES artist_bpm_queries(query_id) ON DELETE CASCADE,
    item_id TEXT NOT NULL,
    title TEXT,
    artist_name TEXT,
    bpm NUMERIC(6,2) NULL,
    musical_key TEXT NULL,
    camelot TEXT NULL,
    energy NUMERIC(6,3) NULL,
    danceability NUMERIC(6,3) NULL,
    raw JSONB NOT NULL,
    PRIMARY KEY (query_id, item_id)
  );