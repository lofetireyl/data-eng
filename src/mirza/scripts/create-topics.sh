#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP:-kafka:29092}"

echo "Waiting for Kafka at $BROKER ..."
for i in {1..60}; do
  if kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

create() {
  local topic="$1"
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 || true
}

# Only these four:
create spotify.new_releases.album
create spotify.new_releases.track
create spotify.track_meta
create spotify.artist_meta

echo "Topics present:"
kafka-topics --bootstrap-server "$BROKER" --list

