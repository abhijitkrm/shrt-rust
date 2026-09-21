//! Integration tests for the RESP-KV backend (STORE=dragonfly|redis).
//! Gated on SHRT_KV_ADDR (e.g. SHRT_KV_ADDR=127.0.0.1:6379 cargo test --test kv_test)
//! — skipped entirely when unset or unreachable, keeping the suite hermetic.

use shrt::store::{MutResult, Store};
use std::sync::{Mutex, MutexGuard};

// tests share one Redis logical DB — serialize so flushdb actually isolates
static DB_LOCK: Mutex<()> = Mutex::new(());

fn kv() -> Option<(MutexGuard<'static, ()>, Store)> {
    let addr = std::env::var("SHRT_KV_ADDR").ok()?;
    let g = DB_LOCK.lock().unwrap();
    let st = Store::open_kv(&addr, 0, 1_000, 50).ok()?;
    shrt::kv::Kv::connect(&addr).ok()?.flushdb().ok()?;
    Some((g, st))
}

macro_rules! skip {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => {
                eprintln!("skipping: SHRT_KV_ADDR unset/unreachable");
                return;
            }
        }
    };
}

#[test]
fn shorten_resolve_alias() {
    let (_g, st) = skip!(kv());
    assert_eq!(
        st.shorten("https://a.com", Some("gh"), 0).as_deref(),
        Some("gh")
    );
    assert_eq!(st.resolve("gh").as_deref(), Some("https://a.com"));
    // alias collision rejected
    assert!(st.shorten("https://b.com", Some("gh"), 0).is_none());
    // generated codes resolve
    let c = st.shorten("https://c.com", None, 0).unwrap();
    assert_eq!(st.resolve(&c).as_deref(), Some("https://c.com"));
}

#[test]
fn cache_bounded_and_cold_miss() {
    let (_g, st) = skip!(kv());
    // CACHE=1000 -> ~3-4 per shard; generate enough codes to force eviction
    let mut codes = Vec::new();
    for i in 0..200 {
        codes.push(st.shorten(&format!("https://x{i}.com"), None, 0).unwrap());
    }
    // every code still resolves — cold misses go to the KV
    for (i, c) in codes.iter().enumerate() {
        assert_eq!(
            st.resolve(c).as_deref(),
            Some(format!("https://x{i}.com").as_str()),
            "cold miss failed for {c}"
        );
    }
}

#[test]
fn hits_batched_to_kv() {
    let (_g, st) = skip!(kv());
    st.shorten("https://a.com", Some("h"), 0);
    for _ in 0..5 {
        st.resolve("h");
    }
    st.flush(); // force the 5ms delta flush
    let s = st.stats("h").unwrap();
    assert_eq!(s.hits, 5);
}

#[test]
fn update_and_remove() {
    let (_g, st) = skip!(kv());
    st.shorten("https://a.com", Some("u"), 0);
    assert_eq!(st.update("u", "https://b.com", 0, false), MutResult::Ok);
    assert_eq!(st.resolve("u").as_deref(), Some("https://b.com"));
    assert_eq!(
        st.update("missing", "https://x.com", 0, false),
        MutResult::Missing
    );
    assert_eq!(st.remove("u"), MutResult::Ok);
    assert!(st.resolve("u").is_none());
    assert_eq!(st.remove("u"), MutResult::Missing);
}

#[test]
fn ttl_expiry() {
    let (_g, st) = skip!(kv());
    st.shorten("https://t.com", Some("ttl"), 80);
    assert_eq!(st.resolve("ttl").as_deref(), Some("https://t.com"));
    std::thread::sleep(std::time::Duration::from_millis(120));
    // PX expiry in the KV + cache expiry both make it vanish
    assert!(st.resolve("ttl").is_none());
}

#[test]
fn list_and_stats() {
    let (_g, st) = skip!(kv());
    st.shorten("https://one.com", Some("one"), 0);
    st.shorten("https://two.com", Some("two"), 0);
    st.resolve("one");
    st.flush();
    let (links, total) = st.list(10, 0, "", "");
    assert_eq!(total, 2);
    assert!(links.iter().any(|l| l.code == "one" && l.hits == 1));
    let s = st.stats("two").unwrap();
    assert_eq!(s.url, "https://two.com");
    assert!(s.created_at > 0);
}

#[test]
fn restart_survives() {
    // corpus outlives the process — a fresh handle resolves pre-existing links
    let addr = skip!(std::env::var("SHRT_KV_ADDR").ok());
    {
        let st = skip!(Store::open_kv(&addr, 0, 100, 50).ok());
        shrt::kv::Kv::connect(&addr).unwrap().flushdb().unwrap();
        st.shorten("https://stay.com", Some("stay"), 0);
        st.close();
    }
    let st2 = Store::open_kv(&addr, 1, 100, 50).unwrap();
    assert_eq!(st2.resolve("stay").as_deref(), Some("https://stay.com"));
}

#[test]
fn bulk_shorten() {
    let (_g, st) = skip!(kv());
    let urls: Vec<String> = (0..50).map(|i| format!("https://b{i}.com")).collect();
    let codes = st.shorten_many(&urls, 0);
    assert_eq!(codes.len(), 50);
    for (i, c) in codes.iter().enumerate() {
        assert_eq!(st.resolve(c).as_deref(), Some(urls[i].as_str()));
    }
}
