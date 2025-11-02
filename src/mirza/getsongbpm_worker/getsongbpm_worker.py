import os
import json
import time
import logging
import string
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import requests
from kafka import KafkaConsumer, KafkaProducer

# ------------ config ------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "spotify.artist_meta")
OUT_TOPIC = os.getenv("OUT_TOPIC", "spotify.artist_bpm_meta")
GROUP_ID = os.getenv("GROUP_ID", "getsongbpm-workers")

GETSONG_BASE_URL = os.getenv("GETSONG_BASE_URL", "https://api.getsong.co")
GETSONG_API_KEY = os.getenv("GETSONG_API_KEY")  # set via .env
REQ_TIMEOUT = float(os.getenv("REQ_TIMEOUT", "10"))
LIMIT_PER_CALL = int(os.getenv("LIMIT_PER_CALL", "100"))
SLEEP_BETWEEN_PREFIX = float(os.getenv("SLEEP_BETWEEN_PREFIX", "0.25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "7200")) 

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("getsongbpm-worker")

# ------------ kafka ------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks="all",
    linger_ms=20,
    retries=5,
    key_serializer=lambda v: v.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
)

consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    enable_auto_commit=True,
    auto_offset_reset="latest",
    key_deserializer=lambda b: b.decode("utf-8") if b else None,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

session = requests.Session()
if not GETSONG_API_KEY:
    log.warning("GETSONG_API_KEY not set. Put it in src/.env or your environment.")
session.params = {"api_key": GETSONG_API_KEY} if GETSONG_API_KEY else {}

def http_get(path: str, params: Dict[str, Any], max_retries: int = MAX_RETRIES):
    backoff = 1.0
    for _ in range(max_retries):
        try:
            r = session.get(f"{GETSONG_BASE_URL}{path}", params=params, timeout=REQ_TIMEOUT)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff
                log.warning("429 from getsong.co; sleeping %.2fs", sleep_s)
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 30.0)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning("HTTP error on %s params=%s: %s", path, params, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    return None

def both_search(song_prefix: str, artist_name: str, limit: int = LIMIT_PER_CALL) -> List[Dict[str, Any]]:
    lookup = f"song:{song_prefix} artist:{artist_name}"
    r = http_get("/search/", {"type": "both", "lookup": lookup, "limit": limit})
    if not r:
        return []
    data = r.json()
    res = data.get("search", [])
    return res if isinstance(res, list) else []

def normalize_name(s: str) -> str:
    return " ".join(s.strip().lower().split())

def sweep_artist_songs_by_name(artist_name: str) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    prefixes = list(string.ascii_lowercase) + list(string.digits)
    target = normalize_name(artist_name)
    for p in prefixes:
        items = both_search(p, artist_name, limit=LIMIT_PER_CALL)
        for s in items:
            a = s.get("artist") or {}
            if normalize_name(a.get("name", "")) == target:
                sid = s.get("id")
                if sid:
                    seen[sid] = s
        time.sleep(SLEEP_BETWEEN_PREFIX)
    return list(seen.values())

def build_event(artist_name: str, artist_id: Optional[str], items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "source": "getsongbpm",
        "artist": {"id": artist_id, "name": artist_name},
        "queried_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }

def out_key_for(artist_name: str, artist_id: Optional[str]) -> str:
    return artist_id or normalize_name(artist_name)

_seen: Dict[str, float] = {}
def should_query(artist_name: str, artist_id: Optional[str]) -> bool:
    k = out_key_for(artist_name, artist_id)
    now = time.time()
    ts = _seen.get(k)
    if ts is None or now - ts > DEDUP_TTL_SEC:
        _seen[k] = now
        return True
    return False

def process_artist_msg(msg: Dict[str, Any]) -> None:
    artist_id = msg.get("artist_id") or msg.get("id")
    artist_name = msg.get("artist_name") or msg.get("name")
    if not artist_name:
        return
    if not should_query(artist_name, artist_id):
        return
    items = sweep_artist_songs_by_name(artist_name)
    evt = build_event(artist_name, artist_id, items)
    producer.send(OUT_TOPIC, key=out_key_for(artist_name, artist_id), value=evt)
    producer.flush()
    log.info("Published getsongbpm for %s (id=%s) items=%d", artist_name, artist_id, len(items))

def main():
    log.info("Starting getsongbpm worker. IN=%s OUT=%s BOOTSTRAP=%s", IN_TOPIC, OUT_TOPIC, KAFKA_BOOTSTRAP)
    for m in consumer:
        try:
            process_artist_msg(m.value or {})
        except Exception as e:
            log.exception("Error processing message: %s", e)

if __name__ == "__main__":
    main()
