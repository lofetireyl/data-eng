-- Optional indexes for analytics
CREATE INDEX IF NOT EXISTS idx_album_release_date ON spotify_album (release_date);
CREATE INDEX IF NOT EXISTS idx_track_meta_popularity ON spotify_track_meta (popularity);