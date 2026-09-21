#!/usr/bin/env bash
# Flood the shorten endpoint: boots a temp server, fires TOTAL requests.
#   TOTAL=100000 CONNS=64 bash scripts/flood.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-4833}"
TOTAL="${TOTAL:-100000}"
CONNS="${CONNS:-64}"
DIR="$(mktemp -d)"
BIN="$(mktemp -u)/shrt"

cargo build --release --bin shrt -q
mkdir -p "$(dirname "$BIN")"
cp target/release/shrt "$BIN"
DATA_DIR="$DIR" PORT="$PORT" SERVER=mini "$BIN" >/tmp/shrt-flood.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null; rm -rf "$DIR" "$(dirname "$BIN")"' EXIT

for i in $(seq 1 50); do curl -sf "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1 && break; sleep 0.1; done

echo "flood: $TOTAL POSTs, $CONNS conns -> :$PORT"
if command -v autocannon >/dev/null 2>&1; then
  autocannon -c "$CONNS" -a "$TOTAL" -m POST \
    -H 'content-type: application/json' \
    -b '{"url":"https://flood.example/payload"}' \
    "http://127.0.0.1:$PORT/api/shorten"
else
  echo "(autocannon not found — install it or use 'cargo run --release --bin shrt-bench')"
fi

echo "log on disk: $(du -h "$DIR"/data-*.log 2>/dev/null | cut -f1)  ($(wc -l < "$DIR"/data-*.log 2>/dev/null || echo 0) rows)"
curl -s "http://127.0.0.1:$PORT/api/metrics" | head -c 200; echo
