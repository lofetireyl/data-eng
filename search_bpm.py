import time
import string
import requests
import json
from pathlib import Path
from typing import Dict, List, Any

BASE_URL = "https://api.getsong.co"
API_KEY = "8dbafd87de8bde42760f94b6104452db"

session = requests.Session()
session.params = {"api_key": API_KEY}

def both_search(song_prefix: str, artist_name: str, limit: int = 100) -> List[Dict[str, Any]]:
    lookup = f"song:{song_prefix} artist:{artist_name}"
    r = session.get(
        f"{BASE_URL}/search/",
        params={"type": "both", "lookup": lookup, "limit": limit},
        timeout=10,
    )
    if r.status_code == 429:
        time.sleep(int(r.headers.get("Retry-After", "2")))
        r = session.get(
            f"{BASE_URL}/search/",
            params={"type": "both", "lookup": lookup, "limit": limit},
            timeout=10,
        )
    r.raise_for_status()
    data = r.json()
    res = data.get("search", [])
    return res if isinstance(res, list) else []

def sweep_artist_songs_by_name(artist_name: str, limit_per_call: int = 100, delay: float = 0.3) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    prefixes = ["e"] + list(string.ascii_lowercase) + list(string.digits)  # Start mit e als Prefix
    for p in prefixes:
        items = both_search(p, artist_name, limit=limit_per_call)
        for s in items:
            a = s.get("artist") or {}
            if a.get("name", "").lower() == artist_name.lower():
                sid = s.get("id")
                if sid:
                    seen[sid] = s
        time.sleep(delay)
    return list(seen.values())

def save_songs_for_artist(artist_name: str, limit_per_call: int = 200, delay: float = 0.3, out_dir: Optional[Path] = None, filename: Optional[str] = None,) -> Path:
    base_dir = Path(__file__).resolve().parent
    target_dir = Path(out_dir) if out_dir is not None else base_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    safe_artist = artist_name.lower().replace("", "_")
    default_name = f"songs_{safe_artist}.json"
    fname = filename if filename is not None else default_name

    out_path = target_dir / fname

    songs = sweep_artist_songs_by_name(
        artist_name, limit_per_call=limit_per_call, delay=delay
    )

    #Speichern
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(songs, f, ensure_ascii=False, indent=2)

    return out_path

if __name__ == "__main__":
    artist = "Justin Bieber"
    out_file = save_songs_for_artist(artist)
    print(f"Gespeichert als: {out_file}")