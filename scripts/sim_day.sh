#!/bin/sh
# sim_day.sh — boot shrt on a scratch backend, run the paced links/day sim.
#
#   ./scripts/sim_day.sh                    # rocksdb, ~100M/day for 10min
#   STORE=kv KV_ADDR=127.0.0.1:6379 ./scripts/sim_day.sh
#   DURATION=3600 RATE=1157 ./scripts/sim_day.sh  # real-pace hour
#   COMPRESSED=1 ./scripts/sim_day.sh       # 100M links via bulk, fast
#
# Env: STORE RATE DURATION PORT DATA_DIR + any sim_day.py passthrough via "$@"
set -e
cd "$(dirname "$0")/.."

PORT=${PORT:-8080}
STORE=${STORE:-rocksdb}
RATE=${RATE:-1157}
DURATION=${DURATION:-600}
DATA_DIR=${DATA_DIR:-$(mktemp -d)/sim}
mkdir -p "$DATA_DIR"

BIN=./target/release/shrt
cargo build --release 2>&1 | tail -1

case "$STORE" in
  rocksdb|rocks) env_store="STORE=rocksdb ROCKSDB_PATH=$DATA_DIR/rocks" ;;
  kv|dragonfly|redis) env_store="STORE=$STORE KV_ADDR=${KV_ADDR:-127.0.0.1:6379} KV_LAYOUT=${KV_LAYOUT:-key}" ;;
  aof|local) env_store="STORE=aof DATA_DIR=$DATA_DIR/aof" ;;
  *) echo "unknown STORE=$STORE" >&2; exit 2 ;;
esac

echo "starting shrt: $env_store PORT=$PORT (data: $DATA_DIR)"
env $env_store PORT="$PORT" RATE_LIMIT="${RATE_LIMIT:-0}" \
    WORKERS="${WORKERS:-1}" "$BIN" >"$DATA_DIR/server.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT

for i in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$PORT/api/health" >/dev/null && break
  sleep 0.2
done
curl -sf "http://127.0.0.1:$PORT/api/health" >/dev/null \
  || { echo "server failed to start"; cat "$DATA_DIR/server.log"; exit 1; }

if [ "$COMPRESSED" = 1 ]; then
  python3 scripts/sim_day.py --url "http://127.0.0.1:$PORT" \
    --count "${COUNT:-100000000}" --bulk "${BULK:-1000}" \
    --rate "${RATE:-50000}" --pid $SRV "$@"
else
  python3 scripts/sim_day.py --url "http://127.0.0.1:$PORT" \
    --rate "$RATE" --duration "$DURATION" --pid $SRV \
    ${READ_RATE:+--read-rate "$READ_RATE"} "$@"
fi

echo
echo "corpus on disk:"
du -sh "$DATA_DIR"/* 2>/dev/null || true
