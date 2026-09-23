//! Per-IP token-bucket rate limiter for write endpoints (abuse control on
//! a public shortener). Off by default: RATE_LIMIT=<req/s per IP> enables
//! it; RATE_LIMIT_BURST sets the bucket capacity (default = RATE_LIMIT).
//! POST /api/shorten costs 1 token; /api/shorten/bulk costs urls.len().

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const SHARDS: usize = 64;
const MAX_KEYS: usize = 1 << 12; // per shard — bounds tracked IPs to ~256k
const IDLE_MS: u64 = 60_000;

struct Bucket {
    tokens: f64,
    last_ms: u64,
}

pub struct RateLimiter {
    shards: [Mutex<HashMap<u64, Bucket>>; SHARDS],
    rate: f64,  // tokens per second
    burst: f64, // bucket capacity
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ip_key(ip: &str) -> u64 {
    // FNV-1a — cheap, no allocation
    let mut h: u64 = 0xcbf29ce484222325;
    for b in ip.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

impl RateLimiter {
    fn new() -> RateLimiter {
        let rate = std::env::var("RATE_LIMIT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            .max(0.0);
        let burst = std::env::var("RATE_LIMIT_BURST")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(rate)
            .max(1.0);
        RateLimiter {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            rate,
            burst,
        }
    }

    /// cost tokens from ip's bucket; true if allowed.
    pub fn allow(&self, ip: &str, cost: f64) -> bool {
        if self.rate <= 0.0 {
            return true; // disabled
        }
        let k = ip_key(ip);
        let mut m = self.shards[(k as usize) & (SHARDS - 1)].lock().unwrap();
        let now = now_ms();
        if m.len() >= MAX_KEYS {
            m.retain(|_, b| now - b.last_ms < IDLE_MS);
        }
        let b = m.entry(k).or_insert(Bucket {
            tokens: self.burst,
            last_ms: now,
        });
        let elapsed = (now - b.last_ms) as f64 / 1000.0;
        b.tokens = (b.tokens + elapsed * self.rate).min(self.burst);
        b.last_ms = now;
        if b.tokens >= cost {
            b.tokens -= cost;
            true
        } else {
            false
        }
    }
}

static RL: std::sync::OnceLock<RateLimiter> = std::sync::OnceLock::new();
pub fn limiter() -> &'static RateLimiter {
    RL.get_or_init(RateLimiter::new)
}

/// Total requests rejected (exported via /metrics).
pub static LIMITED: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_allows_burst_then_throttles() {
        let rl = RateLimiter {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            rate: 1.0,
            burst: 3.0,
        };
        assert!(rl.allow("1.2.3.4", 1.0));
        assert!(rl.allow("1.2.3.4", 1.0));
        assert!(rl.allow("1.2.3.4", 1.0));
        assert!(!rl.allow("1.2.3.4", 1.0));
        assert!(rl.allow("5.6.7.8", 1.0)); // different IP unaffected
    }
}
