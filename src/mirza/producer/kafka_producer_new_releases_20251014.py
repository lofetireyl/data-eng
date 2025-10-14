#!/Users/moerz/Documents/study/fhtw_mai/s1/demai/group_project_spotify/.venv/bin/python

"""
Playlist-free producer for "New Releases Radar" that avoids deprecated endpoints.
It polls:
  • /v1/browse/new-releases      (albums by market)
  • /v1/albums/{id}/tracks       (tracks per album)
  • /v1/tracks?ids=...           (track metadata: popularity, explicit, etc.)
  • /v1/artists?ids=...          (optional: genres/popularity)

Publishes to Kafka topics:
  - spotify.new_releases.album
  - spotify.new_releases.track
  - spotify.track_meta
  - spotify.artist_meta   (optional, toggle via FETCH_ARTISTS)

Env:
  SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
  KAFKA_BOOTSTRAP=localhost:9092
  MARKETS=AT,DE,US
  POLL_SEC=1800
  FETCH_ARTISTS=1

Notes:
  • The new-releases endpoint is paginated (max 50 per page). This producer now
    fetches up to 100 items per market by paging with offset.
"""

import os, time, json, signal, logging
from typing import List, Dict, Any, Set

from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nr-producer")

# ---------- env ----------
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET):
    raise SystemExit("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MARKETS = [x.strip().upper() for x in os.getenv("MARKETS", "AT").split(",") if x.strip()]
POLL_SEC = int(os.getenv("POLL_SEC", "1800"))
FETCH_ARTISTS = os.getenv("FETCH_ARTISTS", "1") not in ("0", "false", "False")

# Maximum albums to fetch from /browse/new-releases per market (Spotify caps at 50/page)
MAX_NEW_RELEASES = 100

# ---------- kafka ----------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks="all",
    linger_ms=20,
    retries=5,
    key_serializer=lambda v: v.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
)
TOPIC_ALBUM   = "spotify.new_releases.album"
TOPIC_TRACK   = "spotify.new_releases.track"
TOPIC_TMETA   = "spotify.track_meta"
TOPIC_ARTIST  = "spotify.artist_meta"

# ---------- spotify ----------
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET
))

# ---------- control ----------
_running = True
def _graceful(*_): 
    global _running; _running = False; log.info("Shutting down…")
signal.signal(signal.SIGINT, _graceful)
signal.signal(signal.SIGTERM, _graceful)

# ---------- helpers ----------
def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def safe_call(fn, *args, **kwargs):
    """Retry 429/5xx with simple backoff. Returns (ok, result)."""
    for attempt in range(6):
        try:
            return True, fn(*args, **kwargs)
        except SpotifyException as e:
            status = getattr(e, "http_status", None)
            if status in (429, 500, 502, 503, 504):
                ra = None
                headers = getattr(e, "http_headers", None) or {}
                if isinstance(headers, dict) and headers.get("Retry-After"):
                    try: ra = float(headers["Retry-After"])
                    except: ra = None
                time.sleep(ra if ra else min(2**attempt, 30))
                continue
            log.error("Spotify error %s on %s: %s", status, fn.__name__, e)
            return False, None
        except Exception as e:
            log.warning("Error calling %s: %s", fn.__name__, e)
            time.sleep(min(2**attempt, 30))
    return False, None

# ---------- fetch ----------
def fetch_new_releases(country: str, max_total: int = MAX_NEW_RELEASES) -> List[Dict[str, Any]]:
    """Fetch up to max_total albums by paging /browse/new-releases (50 per page)."""
    items: List[Dict[str, Any]] = []
    offset = 0
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    while len(items) < max_total:
        page_limit = min(50, max_total - len(items))
        ok, page = safe_call(sp.new_releases, country=country, limit=page_limit, offset=offset)
        if not ok or not page:
            break
        albums = page.get("albums", {})
        page_items = albums.get("items", []) or []
        for a in page_items:
            if not a or not a.get("id"):
                continue
            items.append({
                "album_id": a["id"],
                "album_name": a.get("name"),
                "release_date": a.get("release_date"),
                "release_date_precision": a.get("release_date_precision"),
                "artists": [{"id": ar.get("id"), "name": ar.get("name")} 
                            for ar in (a.get("artists") or []) if ar.get("id")],
                "market": country,
                "fetched_at": fetched_at,
                "label": a.get("label"),
                "total_tracks": a.get("total_tracks"),
            })
        # stop if no next page
        if not albums.get("next"):
            break
        offset += page_limit

    return items


def fetch_album_tracks(album_id: str, market: str) -> List[Dict[str, Any]]:
    tracks = []
    offset = 0
    while True:
        ok, page = safe_call(sp.album_tracks, album_id, market=market, offset=offset, limit=50)
        if not ok or not page: break
        for t in page.get("items", []):
            if not t or not t.get("id"): continue
            tracks.append({
                "track_id": t["id"],
                "track_name": t.get("name"),
                "disc_number": t.get("disc_number"),
                "track_number": t.get("track_number"),
                "duration_ms": t.get("duration_ms"),
                "explicit": t.get("explicit"),
            })
        if not page.get("next"): break
        offset += 50
    return tracks


def publish_track_meta(track_ids: List[str], seen: Set[str]) -> int:
    """Use /v1/tracks?ids=... (max 50) to collect popularity, explicit, markets count, etc."""
    new_ids = [tid for tid in track_ids if tid and tid not in seen]
    if not new_ids: return 0
    published = 0
    for batch in chunked(new_ids, 50):  # /tracks max 50
        ok, res = safe_call(sp.tracks, batch)
        tracks = (res or {}).get("tracks") if ok and res else []
        for t in (tracks or []):
            if not t or not t.get("id"): continue
            msg = {
                "track_id": t["id"],
                "popularity": t.get("popularity"),
                "explicit": t.get("explicit"),
                "duration_ms": t.get("duration_ms"),
                "album_id": (t.get("album") or {}).get("id"),
                "primary_artist_id": ((t.get("artists") or [{}])[0]).get("id"),
                "available_markets_count": len(t.get("available_markets") or []),
            }
            producer.send(TOPIC_TMETA, key=msg["track_id"], value=msg)
            seen.add(msg["track_id"]); published += 1
    if published:
        log.info("Published %d track_meta", published)
    return published


def publish_artists(artist_ids: List[str], seen: Set[str]) -> int:
    if not FETCH_ARTISTS: return 0
    new_ids = [aid for aid in artist_ids if aid and aid not in seen]
    if not new_ids: return 0
    published = 0
    for batch in chunked(new_ids, 50):
        ok, res = safe_call(sp.artists, batch)
        artists = (res or {}).get("artists") if ok and res else []
        for a in (artists or []):
            if not a or not a.get("id"): continue
            msg = {
                "artist_id": a["id"],
                "artist_name": a.get("name"),
                "genres": a.get("genres", []) or [],
                "popularity": a.get("popularity"),
            }
            producer.send(TOPIC_ARTIST, key=msg["artist_id"], value=msg)
            seen.add(msg["artist_id"]); published += 1
    if published:
        log.info("Published %d artist_meta", published)
    return published

# ---------- main ----------

def main():
    log.info("MARKETS=%s POLL_SEC=%s FETCH_ARTISTS=%s MAX_NEW_RELEASES=%s",
             MARKETS, POLL_SEC, FETCH_ARTISTS, MAX_NEW_RELEASES)
    seen_tracks: Set[str] = set()
    seen_artists: Set[str] = set()

    while _running:
        start = time.time()
        tot_albums = tot_tracks = tot_tmeta = tot_art = 0

        for market in MARKETS:
            try:
                albums = fetch_new_releases(market)
                if not albums:
                    log.info("No albums for %s", market); continue

                log.info("Fetched %d new albums for %s", len(albums), market)
                all_tids: List[str] = []
                all_aids: List[str] = []

                for a in albums:
                    producer.send(TOPIC_ALBUM, key=a["album_id"], value=a)
                    tot_albums += 1
                    all_aids.extend([ar["id"] for ar in a.get("artists", []) if ar.get("id")])

                    tracks = fetch_album_tracks(a["album_id"], market)
                    for t in tracks:
                        t["album_id"] = a["album_id"]
                        t["market"] = market
                        t["release_date"] = a.get("release_date")
                        producer.send(TOPIC_TRACK, key=t["track_id"], value=t)
                        all_tids.append(t["track_id"])
                        tot_tracks += 1

                # Track metadata (popularity/explicit/duration/markets_count)
                tot_tmeta += publish_track_meta(all_tids, seen_tracks)
                # Optional artist metadata (genres, popularity)
                tot_art += publish_artists(list(set(all_aids)), seen_artists)

            except Exception as e:
                log.exception("Error processing market %s: %s", market, e)

        producer.flush()
        log.info("Cycle done: albums=%d tracks=%d track_meta=%d artists=%d",
                 tot_albums, tot_tracks, tot_tmeta, tot_art)

        # sleep until next cycle
        elapsed = time.time() - start
        for _ in range(max(5, POLL_SEC - int(elapsed))):
            if not _running: break
            time.sleep(1)

if __name__ == "__main__":
    main()
