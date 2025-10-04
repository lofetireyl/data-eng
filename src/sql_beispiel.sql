-- Alben (einmalig pro album_id)
CREATE TABLE albums (
  album_id VARCHAR(64) PRIMARY KEY,
  album_name VARCHAR(512),
  album_total_tracks INT,
  album_release_date DATE NULL,
  album_release_date_precision ENUM('day','month','year') NULL,
  first_seen_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL,
  KEY (album_release_date)
);

-- Tracks (einmalig pro track_id)
CREATE TABLE tracks (
  track_id VARCHAR(64) PRIMARY KEY,
  album_id VARCHAR(64) NOT NULL,
  track_name VARCHAR(512),
  track_number INT,
  track_duration_ms INT,
  first_seen_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL,
  CONSTRAINT fk_tracks_album FOREIGN KEY (album_id) REFERENCES albums(album_id)
);

-- Optional: Popularität im Zeitverlauf (falls du Historie willst)
CREATE TABLE track_popularity_history (
  track_id VARCHAR(64) NOT NULL,
  snapshot_date DATE NOT NULL,
  popularity INT,
  PRIMARY KEY (track_id, snapshot_date),
  CONSTRAINT fk_pop_track FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);

-- Optional & besser als Stringliste: Künstlertabellen (wenn du später brauchst)
CREATE TABLE artists (
  artist_id VARCHAR(64) PRIMARY KEY,
  artist_name VARCHAR(512)
);

CREATE TABLE album_artists (
  album_id VARCHAR(64) NOT NULL,
  artist_id VARCHAR(64) NOT NULL,
  PRIMARY KEY (album_id, artist_id),
  FOREIGN KEY (album_id) REFERENCES albums(album_id),
  FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);