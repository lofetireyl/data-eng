import requests
import os
import pandas as pd
import datetime
import time
from itertools import islice
import matplotlib.pyplot as plt

CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]

# Token holen
header = {"Content-Type": "application/x-www-form-urlencoded"}
data = f"grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"
resp = requests.post("https://accounts.spotify.com/api/token", headers=header, data=data)
resp.raise_for_status()
token = resp.json()["access_token"]

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}"})

def request_json(method, url, **kwargs):
    """Requests mit 429-Handling (Retry-After)."""
    while True:
        r = session.request(method, url, **kwargs)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

# paginate new releases 
def get_all_new_releases(market, limit=50, max_total=None):
    url = "https://api.spotify.com/v1/browse/new-releases"
    params = {"limit": limit, "offset": 0}
    if market:
        params["market"] = market

    albums_all = []

    # first page
    page = request_json("GET", url, params=params)
    albums = page["albums"]
    albums_all.extend(albums["items"])

    total = albums.get("total", len(albums_all))
    next_url = albums.get("next")

    # optional cap
    target_total = min(max_total, total) if max_total is not None else total

    # follow 'next' until None
    while next_url and len(albums_all) < target_total:
        page = request_json("GET", next_url)
        albums = page["albums"]
        albums_all.extend(albums["items"])
        next_url = albums.get("next")

    # trim to max_total if requested
    if max_total is not None and len(albums_all) > max_total:
        albums_all = albums_all[:max_total]

    return albums_all


# paginate album tracks
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

market = "AT"
all_albums = get_all_new_releases(market=market, limit=50)
rows = []
all_track_ids = []

for album in all_albums:
    album_id = album["id"]
    album_name = album["name"]
    artists = ", ".join(a["name"] for a in album.get("artists", []))

    album_meta = {
        "album_id": album_id,
        "album_name": album_name,
        "album_artists": artists,
        "album_total_tracks": album.get("total_tracks"),
        "album_release_date": album.get("release_date"),
        "album_release_date_precision": album.get("release_date_precision"),
    }

    tracks = get_all_album_tracks(album_id, market=market, limit=50)

    for t in tracks:
        tid = t.get("id")
        if not tid:
            continue

        rows.append({
            **album_meta,
            "track_id": tid,
            "track_name": t.get("name"),
            "track_number": t.get("track_number"),
            "track_duration_ms": t.get("duration_ms"),
        })
        all_track_ids.append(tid)

details_map = get_tracks_details(all_track_ids, market=market)
for r in rows:
    d = details_map.get(r["track_id"])
    if d:
        r["track_popularity"] = d["popularity"]
        r["track_duration_ms"] = d["duration_ms"] or r["track_duration_ms"]
        r["track_name"] = d["name"] or r["track_name"]

df = pd.DataFrame(rows)

today = str(datetime.date.today())
df.to_excel(f"new_releases_{today}.xlsx", sheet_name=today, index=False)