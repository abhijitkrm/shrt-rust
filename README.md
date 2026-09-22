# shrt-rust

High-performance URL shortener backend — Rust port of
[shrt-ts](https://github.com/abhijitkrm/shrt-ts) /
[shrt-go](https://github.com/abhijitkrm/shrt-go), tuned for maximum throughput.

* **HTTP**: custom thread-per-connection server built on
  [`httparse`](https://docs.rs/httparse) (`SERVER=mini`, default) — one read
  batch answered by a single write, HTTP pipelining supported;
  [`hyper`](https://docs.rs/hyper) fallback (`SERVER=hyper`).
* **Storage**: custom append-only log (AOF) — sharded in-memory index (256
  shards, `parking_lot` RwLock + `FxHashMap` + atomic hit counters) + batched
  `write()`/`fsync`. Reads never touch disk; writes are ~ns enqueue + one
  syscall batch per 5 ms.
* **JSON**: [`sonic-rs`](https://github.com/cloudwego/sonic-rs) (SIMD) on the
  write path; log lines are hand-serialized into a scratch buffer (zero JSON,
  zero per-row allocation on the read/bulk hot paths).
* **Allocator**: `mimalloc` global allocator; release builds use fat LTO +
  `codegen-units = 1`.
* **Codes**: 8 chars = `ALPHABET[instance]` + 7 random base62 chars
  (62⁷ ≈ 3.5T per instance). The prefix shard-marks every code — unique across
  processes with zero coordination, and a read-miss knows exactly which sibling
  log to tail. Checked against the index and retried on collision.
* **Multi-instance**: `WORKERS=N` spawns N processes that **share one port via
  SO_REUSEPORT** (the kernel load-balances connections) with per-instance log
  shards (`data-<i>.log`). Siblings are discovered and tailed **lazily on
  read-miss** — writes never pay replication cost, so write throughput scales
  ~linearly with instance count.
* **Durability**: every append reaches the OS page cache within 5 ms and is
  fsync'd every 500 ms — a process crash loses ≤5 ms of writes, a machine
  crash ≤~500 ms (tunable constants in `src/store.rs`; snapshot+truncate via
  `compact()`).
* **External KV mode** (`STORE=dragonfly|redis`): the whole corpus lives in a
  RESP-compatible store (DragonflyDB / Redis) instead of process memory — RAM
  stays flat regardless of link count. Each node keeps only a **bounded FIFO
  cache** (`CACHE` entries) + batched hit deltas (pipelined `INCRBY` every
  5 ms). Keys: `l:{code}` → `"{exp}|{created}|{url}"` (`PX` self-evicts TTLs),
  `h:{code}` → hit counter. No tailing/convergence — the KV is shared state,
  so admin mutations work on any node.

## Quickstart

```sh
cargo build --release
./target/release/shrt                    # :3000, mini server, 1 instance
WORKERS=4 ./target/release/shrt          # 4 instances sharing :3000
SERVER=hyper ./target/release/shrt       # hyper frontend
```

## API (identical to shrt-ts / shrt-go)

| route | method | description |
|---|---|---|
| `/api/shorten` | POST | `{"url","alias"?,"ttl_ms"?}` → `{"code","short_url"}` |
| `/api/shorten/bulk` | POST | `{"urls":[...]}` (≤10k) → `{"count","codes"}` |
| `/:code` | GET | 302 redirect (counts a hit) |
| `/api/stats/:code` | GET | `{"code","url","hits","created_at","expires_at"}` |
| `/api/links` | GET | `?limit&offset&sort=hits|created&q` (admin list) |
| `/api/links/:code` | PATCH/DELETE | requires `ADMIN_TOKEN` + `x-admin-token` header |
| `/api/health` `/api/metrics` | GET | health / request counters |
| `/` | GET | built-in UI (`ui/index.html`) |

## Config (env)

`PORT` (3000) · `DATA_DIR` (`data`) · `WORKERS` (1) · `SERVER` (`mini`|`hyper`)
· `SEED` (pre-generate N links at boot) · `ADMIN_TOKEN` · `CORS_ORIGIN` (`*`)
· `LINK_TTL_MS` (86400000, capped at this value)
· `STORE` (`aof`|`dragonfly`|`redis`) · `DRAGONFLY_ADDR` (`127.0.0.1:6379`)
· `CACHE` (100000, bounded hot cache entries) · `CACHE_TTL_MS` (5000,
staleness bound for cached entries)
· `KV_LAYOUT` (`key`|`hash`) — `key`: `l:{code}` string keys with per-key
`PX` expiry. `hash`: links packed as fields in `l:{code % KV_BUCKETS}` hashes
(~40% less KV memory at ~105B values); expiry is enforced on read and a
janitor reaps dead fields every `KV_SWEEP_MS` (1h)
· `KV_BUCKETS` (1000000) — keep fields/bucket under the server's
`hash-max-listpack-entries` (512 on Redis 8) so buckets stay listpack-packed
· `KV_SWEEP_MS` (3600000) — janitor interval for hash layout

Hash-layout memory win needs the server's `hash-max-listpack-value` above the
stored value size (~105B): `CONFIG SET hash-max-listpack-value 256` —
otherwise buckets convert to hashtable encoding and savings drop to ~7%.

```sh
STORE=dragonfly DRAGONFLY_ADDR=host:6379 CACHE=200000 ./target/release/shrt
SHRT_KV_ADDR=127.0.0.1:6379 cargo test --test kv_test   # live KV tests
```

## Bench

```sh
cargo run --release --bin shrt-bench     # spawns real servers, drives raw-TCP load
bash scripts/smoke.sh                    # end-to-end API smoke
bash scripts/flood.sh                    # write flood (autocannon if installed)
cargo test                               # 32 tests: store engine + API × both frontends
```

Measured on Apple Silicon (client+server colocated, 64 conns):

| scenario | shrt-rust | shrt-go | shrt-ts |
|---|---|---|---|
| redirect | **~208k req/s** | ~197k | ~133k |
| redirect, pipelined ×10 | ~409k req/s | ~457k | ~320k |
| mixed 95/5 | **~205k req/s** | ~185k | ~115k |
| shorten | **~175k req/s** | ~158k | ~72k |
| bulk ×1000 | **~4.1M rows/s** | ~2.1M | ~640k |
| redirect ×4 workers | ~184k req/s | — | — |

`STORE=dragonfly` (local Redis 8.2, same load): hot-cache reads are nearly
free — ~197k redirect / ~430k pipelined / ~205k ×4 workers — while writes pay
one round-trip (~65k shorten, ~486k rows/s bulk). Cold misses cost one
`GET` (~50–100 µs); the corpus is unbounded by RAM.
