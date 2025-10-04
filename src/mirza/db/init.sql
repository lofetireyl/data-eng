CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Albums
CREATE TABLE IF NOT EXISTS spotify_album (
  album_id TEXT PRIMARY KEY,
  album_name TEXT,
  release_date TEXT,
  release_date_precision TEXT,
  label TEXT,
  total_tracks INT,
  market TEXT,
  artists JSONB,
  fetched_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tracks (no FK; streaming-friendly)
CREATE TABLE IF NOT EXISTS spotify_track (
  track_id TEXT PRIMARY KEY,
  track_name TEXT,
  album_id TEXT,                 -- keep, but no FK
  disc_number INT,
  track_number INT,
  duration_ms INT,
  explicit BOOL,
  market TEXT,
  release_date TEXT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track metadata
CREATE TABLE IF NOT EXISTS spotify_track_meta (
  track_id TEXT PRIMARY KEY REFERENCES spotify_track(track_id),
  album_id TEXT,
  primary_artist_id TEXT,
  popularity INT,
  explicit BOOL,
  duration_ms INT,
  available_markets_count INT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Artist metadata
CREATE TABLE IF NOT EXISTS spotify_artist_meta (
  artist_id TEXT PRIMARY KEY,
  artist_name TEXT,
  genres JSONB,
  popularity INT,
  ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Optional indexes for analytics
CREATE INDEX IF NOT EXISTS idx_album_release_date ON spotify_album (release_date);
CREATE INDEX IF NOT EXISTS idx_track_meta_popularity ON spotify_track_meta (popularity);

