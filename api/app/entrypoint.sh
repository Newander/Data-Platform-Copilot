#!/usr/bin/env sh
set -e

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8080}"
WORKERS="${WORKERS:-1}"

exec granian --interface asgi --host "$HOST" --port "$PORT" --workers "$WORKERS" main:app