#!/usr/bin/env bash
set -euo pipefail

BOOT="${KAFKA_BOOTSTRAP:-kafka:29092}"
HOST="${BOOT%%:*}"
PORT="${BOOT##*:}"

echo "[wait-for-kafka] waiting for ${HOST}:${PORT}"

# 1) Wait for DNS to resolve the host
for i in {1..60}; do
  if getent hosts "${HOST}" >/dev/null 2>&1; then
    break
  fi
  echo "[wait-for-kafka] DNS not ready for ${HOST} (${i}/60)..."
  sleep 2
done

# 2) Wait for TCP port to be reachable
for i in {1..60}; do
  if (exec 3<>"/dev/tcp/${HOST}/${PORT}") 2>/dev/null; then
    break
  fi
  echo "[wait-for-kafka] TCP not open on ${HOST}:${PORT} (${i}/60)..."
  sleep 2
done

# 3) If kafka CLI is present, try one light metadata command (non-fatal)
if command -v kafka-broker-api-versions >/dev/null 2>&1; then
  if ! kafka-broker-api-versions --bootstrap-server "${BOOT}" >/dev/null 2>&1; then
    echo "[wait-for-kafka] broker API versions check failed (continuing anyway)"
  fi
fi

echo "[wait-for-kafka] Kafka looks reachable at ${BOOT}"
exec "$@"

