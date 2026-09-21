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
