from confluent_kafka import Producer
import json, os

p = Producer({"bootstrap.servers": os.getenv("KAFKA_BROKERS","localhost:9092")})
topic = "spotify.new_releases.raw"

def delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    # else: ok

def send_event(track, album, market, fetched_at):
    key = track["id"]
    event = {
        "fetched_at": fetched_at,
        "market": market,
        "album": {
            "id": album["album_id"],
            "name": album["album_name"],
            "artists": album["album_artists"].split(", "),
            "total_tracks": album["album_total_tracks"],
            "release_date": album["album_release_date"],
            "release_date_precision": album["album_release_date_precision"]
        },
        "track": {
            "id": track["track_id"],
            "name": track["track_name"],
            "number": track["track_number"],
            "duration_ms": track["track_duration_ms"],
            "popularity": track.get("track_popularity")
        }
    }
    p.produce(topic=topic, key=key, value=json.dumps(event), callback=delivery)

# in deiner Schleife:
# send_event(track_row, album_meta, market, datetime.datetime.utcnow().isoformat() + "Z")
# p.flush()
