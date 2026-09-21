#!/usr/bin/env bash
# Self-contained CLI smoke test: boots a temp server, exercises the API, kills it.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-4567}"
DIR="$(mktemp -d)"
BASE="http://127.0.0.1:$PORT"
BIN="$(mktemp -u)/shrt"

cargo build --release --bin shrt -q
mkdir -p "$(dirname "$BIN")"
cp target/release/shrt "$BIN"
DATA_DIR="$DIR" PORT="$PORT" SERVER=mini "$BIN" >/tmp/shrt-smoke.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null; rm -rf "$DIR" "$(dirname "$BIN")"' EXIT

for i in $(seq 1 50); do curl -sf "$BASE/api/health" >/dev/null 2>&1 && break; sleep 0.1; done

echo "== health =="
curl -s "$BASE/api/health"; echo

echo "== POST /api/shorten (random 8-char code) =="
CODE=$(curl -s -X POST "$BASE/api/shorten" -H 'content-type: application/json' \
  -d '{"url":"https://devin.ai/docs"}' | sed -E 's/.*"code":"([^"]+)".*/\1/')
echo "code=$CODE"

echo "== GET /{code} (302) =="
curl -s -o /dev/null -w "%{http_code} -> %{redirect_url}\n" "$BASE/$CODE"

echo "== POST /api/shorten with alias =="
curl -s -X POST "$BASE/api/shorten" -H 'content-type: application/json' \
  -d '{"url":"https://github.com/cognitionai","alias":"gh"}'; echo
curl -s -o /dev/null -w "%{http_code} -> %{redirect_url}\n" "$BASE/gh"

echo "== duplicate alias (409) =="
curl -s -w " [%{http_code}]" -X POST "$BASE/api/shorten" -H 'content-type: application/json' \
  -d '{"url":"https://other.com","alias":"gh"}'; echo

echo "== POST /api/shorten/bulk =="
curl -s -X POST "$BASE/api/shorten/bulk" -H 'content-type: application/json' \
  -d '{"urls":["https://a.com","https://b.com","https://c.com"]}'; echo

echo "== GET /api/stats/{code} (shows hit count) =="
curl -s "$BASE/api/stats/$CODE"; echo

echo "== GET /api/links (UI list, sort=hits) =="
curl -s "$BASE/api/links?limit=3&sort=hits"; echo

echo "== PATCH/DELETE public -> 404 (immutable links) =="
curl -s -o /dev/null -w "patch:  %{http_code}\n" -X PATCH "$BASE/api/links/$CODE" \
  -H 'content-type: application/json' -d '{"url":"https://x.example"}'
curl -s -o /dev/null -w "delete: %{http_code}\n" -X DELETE "$BASE/api/links/$CODE"

echo "== OPTIONS preflight (CORS) =="
curl -s -o /dev/null -w "%{http_code} allow-origin: %{header_json} " -X OPTIONS "$BASE/api/shorten" 2>/dev/null \
  || curl -s -D - -o /dev/null -X OPTIONS "$BASE/api/shorten" | grep -i access-control

echo "== ttl_ms=1 link -> expires =="
TTL_CODE=$(curl -s -X POST "$BASE/api/shorten" -H 'content-type: application/json' \
  -d '{"url":"https://gone.com","ttl_ms":1}' | sed -E 's/.*"code":"([^"]+)".*/\1/')
echo "code=$TTL_CODE"
sleep 0.05
curl -s -o /dev/null -w "after ttl: %{http_code}\n" "$BASE/$TTL_CODE"

echo "== GET / (UI) =="
curl -s -o /dev/null -w "%{http_code} %{content_type}\n" "$BASE/"

echo "== 404 / 400 cases =="
curl -s -o /dev/null -w "unknown code: %{http_code}\n" "$BASE/zzzz"
curl -s -o /dev/null -w "invalid url:      %{http_code}\n" -X POST "$BASE/api/shorten" \
  -H 'content-type: application/json' -d '{"url":"notaurl"}'

echo
echo "log dir contents:"; ls "$DIR"
