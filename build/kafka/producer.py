import os
import json
import time
import datetime
import requests
from itertools import islice
from kafka import KafkaProducer

# === ENV ===
CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "spotify-tracks")
MARKET = os.environ.get("MARKET", "AT")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))

_session = requests.Session()
_token = None
_token_expires_at = 0

def _get_token():
    global _token, _token_expires_at
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = f"grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"
    r = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
    r.raise_for_status()
    payload = r.json()
    _token = payload["access_token"]
    _token_expires_at = time.time() + max(int(payload.get("expires_in", 3600)) - 60, 60)
    _session.headers.update({"Authorization": f"Bearer {_token}"})

def _ensure_token():
    if not _token or time.time() >= _token_expires_at:
        _get_token()

def request_json(method, url, **kwargs):
    while True:
        _ensure_token()
        r = _session.request(method, url, **kwargs)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(wait)
            continue
        if r.status_code == 401:
            _get_token()
            r = _session.request(method, url, **kwargs)
        r.raise_for_status()
        return r.json()

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

def get_all_new_releases(market, limit=50, max_total=None):
    url = "https://api.spotify.com/v1/browse/new-releases"
    params = {"limit": limit, "offset": 0}
    if market:
        params["market"] = market
    albums_all = []
    page = request_json("GET", url, params=params)
    albums = page["albums"]
    albums_all.extend(albums["items"])
    total = albums.get("total", len(albums_all))
    next_url = albums.get("next")
    target_total = min(max_total, total) if max_total is not None else total
    while next_url and len(albums_all) < target_total:
        page = request_json("GET", next_url)
        albums = page["albums"]
        albums_all.extend(albums["items"])
        next_url = albums.get("next")
    if max_total is not None and len(albums_all) > max_total:
        albums_all = albums_all[:max_total]
    return albums_all

def get_all_album_tracks(album_id, market="AT", limit=50):
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    params = {"limit": limit, "offset": 0}
    if market:
        params["market"] = market
    tracks_all = []
    page = request_json("GET", url, params=params)
    tracks_all.extend(page.get("items", []))
    next_url = page.get("next")
    while next_url:
        page = request_json("GET", next_url)
        tracks_all.extend(page.get("items", []))
        next_url = page.get("next")
    return tracks_all

def get_tracks_details(track_ids, market="AT"):
    details = {}
    for chunk in chunked(track_ids, 50):
        params = {"ids": ",".join(chunk)}
        if market:
            params["market"] = market
        data = request_json("GET", "https://api.spotify.com/v1/tracks", params=params)
        for t in data.get("tracks", []):
            if t:
                details[t["id"]] = {
                    "name": t["name"],
                    "duration_ms": t["duration_ms"],
                    "popularity": t.get("popularity"),
                }
    return details

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks="all",
    linger_ms=50,
    retries=10,
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def build_messages(market="AT"):
    all_albums = get_all_new_releases(market=market, limit=50)
    rows, all_track_ids = [], []
    for album in all_albums:
        album_meta = {
            "album_id": album["id"],
            "album_name": album["name"],
            "album_artists": ", ".join(a["name"] for a in album.get("artists", [])),
            "album_total_tracks": album.get("total_tracks"),
            "album_release_date": album.get("release_date"),
            "album_release_date_precision": album.get("release_date_precision"),
        }
        for t in get_all_album_tracks(album["id"], market=market, limit=50):
            tid = t.get("id")
            if not tid:
                continue
            rows.append({**album_meta,
                         "track_id": tid,
                         "track_name": t.get("name"),
                         "track_number": t.get("track_number"),
                         "track_duration_ms": t.get("duration_ms")})
            all_track_ids.append(tid)
    details_map = get_tracks_details(all_track_ids, market=market)
    for r in rows:
        d = details_map.get(r["track_id"])
        if d:
            r["track_popularity"] = d["popularity"]
            r["track_duration_ms"] = d["duration_ms"] or r["track_duration_ms"]
            r["track_name"] = d["name"] or r["track_name"]
    return rows

def main():
    sent_ids = set()
    while True:
        try:
            ts = datetime.datetime.utcnow().isoformat() + "Z"
            rows = build_messages(MARKET)
            sent = 0
            for r in rows:
                tid = r["track_id"]
                if tid in sent_ids:
                    continue
                msg = {"market": MARKET, "ingest_ts": ts, **r}
                producer.send(TOPIC, key=tid, value=msg)
                sent_ids.add(tid)
                sent += 1
            producer.flush()
            print(f"[producer] sent={sent} total={len(sent_ids)}")
        except Exception as e:
            print(f"[producer][error] {e}", flush=True)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
