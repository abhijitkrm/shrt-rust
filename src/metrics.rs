//! In-process request metrics: per-second ring buffer + totals, reported via
//! /api/metrics.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const N: usize = 60;

#[allow(clippy::declare_interior_mutable_const)]
static BUCKETS: [AtomicI64; N] = {
    // const-init array of atomics
    const Z: AtomicI64 = AtomicI64::new(0);
    [Z; N]
};
static CUR: AtomicI64 = AtomicI64::new(0);
static TOTAL: AtomicI64 = AtomicI64::new(0);
static STARTED: AtomicI64 = AtomicI64::new(0);

// ---- counters for the Prometheus /metrics endpoint ----
// ops: redirect, shorten, shorten_bulk, update, delete, list, stats,
// health, metrics, ui, other; plus cache hit/miss and rate-limited.
pub const OPS: [&str; 11] = [
    "redirect",
    "shorten",
    "shorten_bulk",
    "update",
    "delete",
    "list",
    "stats",
    "health",
    "metrics",
    "ui",
    "other",
];
#[allow(clippy::declare_interior_mutable_const)]
static OP_COUNTS: [AtomicI64; 11] = {
    const Z: AtomicI64 = AtomicI64::new(0);
    [Z; 11]
};
#[allow(clippy::declare_interior_mutable_const)]
static STATUS: [AtomicI64; 4] = {
    const Z: AtomicI64 = AtomicI64::new(0);
    [Z; 4]
}; // 2xx 3xx 4xx 5xx
static CACHE_HIT: AtomicI64 = AtomicI64::new(0);
static CACHE_MISS: AtomicI64 = AtomicI64::new(0);
static STORE_READS: AtomicI64 = AtomicI64::new(0);
static STORE_READ_US: AtomicI64 = AtomicI64::new(0);
static STORE_WRITES: AtomicI64 = AtomicI64::new(0);
static LINKS_TOTAL: AtomicI64 = AtomicI64::new(0);

#[inline]
pub fn op(i: usize) {
    OP_COUNTS[i].fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn status(code: u16) {
    let i = match code {
        200..=299 => 0,
        300..=399 => 1,
        400..=499 => 2,
        _ => 3,
    };
    STATUS[i].fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn cache_hit() {
    CACHE_HIT.fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn cache_miss() {
    CACHE_MISS.fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn store_read(us: i64) {
    STORE_READS.fetch_add(1, Ordering::Relaxed);
    STORE_READ_US.fetch_add(us, Ordering::Relaxed);
}
#[inline]
pub fn store_write() {
    STORE_WRITES.fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub fn links_delta(n: i64) {
    LINKS_TOTAL.fetch_add(n, Ordering::Relaxed);
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn init() {
    let ms = now_ms();
    STARTED.store(ms, Ordering::Relaxed);
    CUR.store(ms / 1000, Ordering::Relaxed);
}

#[inline]
pub fn tick() {
    TOTAL.fetch_add(1, Ordering::Relaxed);
    let s = now_ms() / 1000;
    let c = CUR.load(Ordering::Relaxed);
    let mut sec = s;
    if s != c {
        if CUR
            .compare_exchange(c, s, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let mut t = c + 1;
            while t <= s {
                BUCKETS[(t as usize) % N].store(0, Ordering::Relaxed);
                t += 1;
            }
        } else {
            sec = CUR.load(Ordering::Relaxed);
        }
    }
    BUCKETS[(sec as usize) % N].fetch_add(1, Ordering::Relaxed);
}

/// Renders {"req_s","total","uptime_s","per_second":[31]} — the last 30
/// complete seconds plus the current partial second.
pub fn snapshot() -> Vec<u8> {
    let s = now_ms() / 1000;
    let c = CUR.load(Ordering::Relaxed);
    let mut window = [0i64; 31];
    for (i, w) in window.iter_mut().enumerate() {
        let t = s - 30 + i as i64;
        if t <= c && c - t < N as i64 {
            *w = BUCKETS[(t as usize) % N].load(Ordering::Relaxed);
        }
    }
    let last5: i64 = window[25..30].iter().sum();
    let req_s = ((last5 as f64 / 5.0) * 10.0).round() / 10.0;

    let mut b = Vec::with_capacity(320);
    b.extend_from_slice(b"{\"req_s\":");
    b.extend_from_slice(format!("{req_s}").as_bytes());
    b.extend_from_slice(b",\"total\":");
    b.extend_from_slice(TOTAL.load(Ordering::Relaxed).to_string().as_bytes());
    b.extend_from_slice(b",\"uptime_s\":");
    b.extend_from_slice(
        ((now_ms() - STARTED.load(Ordering::Relaxed)) / 1000)
            .to_string()
            .as_bytes(),
    );
    b.extend_from_slice(b",\"per_second\":[");
    for (i, v) in window.iter().enumerate() {
        if i > 0 {
            b.push(b',');
        }
        b.extend_from_slice(v.to_string().as_bytes());
    }
    b.extend_from_slice(b"]}");
    b
}

/// Prometheus text exposition — /metrics endpoint.
fn pline(b: &mut Vec<u8>, m: &str, labels: &str, v: i64) {
    b.extend_from_slice(m.as_bytes());
    if !labels.is_empty() {
        b.push(b'{');
        b.extend_from_slice(labels.as_bytes());
        b.push(b'}');
    }
    b.push(b' ');
    b.extend_from_slice(v.to_string().as_bytes());
    b.push(b'\n');
}

pub fn prometheus() -> Vec<u8> {
    let mut b = Vec::with_capacity(1024);
    b.extend_from_slice(b"# HELP shrt_requests_total Requests by operation\n");
    b.extend_from_slice(b"# TYPE shrt_requests_total counter\n");
    for (i, name) in OPS.iter().enumerate() {
        pline(
            &mut b,
            "shrt_requests_total",
            &format!("op=\"{name}\""),
            OP_COUNTS[i].load(Ordering::Relaxed),
        );
    }
    b.extend_from_slice(b"# HELP shrt_responses_total Responses by status class\n");
    b.extend_from_slice(b"# TYPE shrt_responses_total counter\n");
    for (i, cls) in ["2xx", "3xx", "4xx", "5xx"].iter().enumerate() {
        pline(
            &mut b,
            "shrt_responses_total",
            &format!("class=\"{cls}\""),
            STATUS[i].load(Ordering::Relaxed),
        );
    }
    b.extend_from_slice(b"# HELP shrt_cache_lookups_total Local hot-cache lookups\n");
    b.extend_from_slice(b"# TYPE shrt_cache_lookups_total counter\n");
    pline(
        &mut b,
        "shrt_cache_lookups_total",
        "result=\"hit\"",
        CACHE_HIT.load(Ordering::Relaxed),
    );
    pline(
        &mut b,
        "shrt_cache_lookups_total",
        "result=\"miss\"",
        CACHE_MISS.load(Ordering::Relaxed),
    );
    b.extend_from_slice(
        b"# HELP shrt_store_reads_total Backing-store point reads (cache misses)\n",
    );
    b.extend_from_slice(b"# TYPE shrt_store_reads_total counter\n");
    pline(
        &mut b,
        "shrt_store_reads_total",
        "",
        STORE_READS.load(Ordering::Relaxed),
    );
    b.extend_from_slice(
        b"# HELP shrt_store_read_us_total Cumulative backing-store read latency (us)\n",
    );
    b.extend_from_slice(b"# TYPE shrt_store_read_us_total counter\n");
    pline(
        &mut b,
        "shrt_store_read_us_total",
        "",
        STORE_READ_US.load(Ordering::Relaxed),
    );
    b.extend_from_slice(b"# HELP shrt_store_writes_total Backing-store writes\n");
    b.extend_from_slice(b"# TYPE shrt_store_writes_total counter\n");
    pline(
        &mut b,
        "shrt_store_writes_total",
        "",
        STORE_WRITES.load(Ordering::Relaxed),
    );
    b.extend_from_slice(b"# HELP shrt_rate_limited_total Requests rejected by the rate limiter\n");
    b.extend_from_slice(b"# TYPE shrt_rate_limited_total counter\n");
    pline(
        &mut b,
        "shrt_rate_limited_total",
        "",
        crate::ratelimit::LIMITED.load(Ordering::Relaxed) as i64,
    );
    b.extend_from_slice(b"# HELP shrt_links_total Live links created minus deleted\n");
    b.extend_from_slice(b"# TYPE shrt_links_total gauge\n");
    pline(
        &mut b,
        "shrt_links_total",
        "",
        LINKS_TOTAL.load(Ordering::Relaxed),
    );
    b.extend_from_slice(b"# HELP shrt_uptime_seconds Process uptime\n");
    b.extend_from_slice(b"# TYPE shrt_uptime_seconds gauge\n");
    pline(
        &mut b,
        "shrt_uptime_seconds",
        "",
        (now_ms() - STARTED.load(Ordering::Relaxed)) / 1000,
    );
    b
}
