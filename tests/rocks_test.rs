//! Integration tests for the embedded RocksDB backend (STORE=rocksdb).
//! No external server needed — each test opens a fresh temp DB.

use shrt::store::{MutResult, Store};

fn rocks() -> Store {
    let dir = std::env::temp_dir().join(format!("shrt-rocks-test-{}", uuid()));
    Store::open_rocks(dir.to_str().unwrap(), 0, 1_000, 50).unwrap()
}

fn uuid() -> String {
    use rand::RngCore;
    let mut b = [0u8; 8];
    rand::rng().fill_bytes(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn shorten_resolve_alias() {
    let st = rocks();
    assert_eq!(
        st.shorten("https://a.com", Some("gh"), 0).as_deref(),
        Some("gh")
    );
    assert_eq!(st.resolve("gh").as_deref(), Some("https://a.com"));
    // alias collision rejected — NX semantics via get+put mutex
    assert!(st.shorten("https://b.com", Some("gh"), 0).is_none());
    let c = st.shorten("https://c.com", None, 0).unwrap();
    assert_eq!(st.resolve(&c).as_deref(), Some("https://c.com"));
}

#[test]
fn cache_bounded_and_cold_miss() {
    let st = rocks();
    let mut codes = Vec::new();
    for i in 0..200 {
        codes.push(st.shorten(&format!("https://x{i}.com"), None, 0).unwrap());
    }
    // cache holds ~3-4 entries per shard; the rest must cold-read from RocksDB
    for (i, c) in codes.iter().enumerate() {
        assert_eq!(
            st.resolve(c).as_deref(),
            Some(format!("https://x{i}.com").as_str()),
            "cold miss failed for {c}"
        );
    }
}

#[test]
fn hits_batched_to_store() {
    let st = rocks();
    st.shorten("https://a.com", Some("gh"), 0);
    for _ in 0..5 {
        st.resolve("gh");
    }
    st.flush(); // forces the 5ms delta flush deterministically
    let l = st.stats("gh").unwrap();
    assert_eq!(l.hits, 5);
}

#[test]
fn update_and_remove() {
    let st = rocks();
    st.shorten("https://a.com", Some("gh"), 0);
    st.resolve("gh"); // fill cache so we can check invalidation
    assert_eq!(st.update("gh", "https://b.com", 0, false), MutResult::Ok);
    assert_eq!(st.resolve("gh").as_deref(), Some("https://b.com"));
    assert_eq!(st.remove("gh"), MutResult::Ok);
    assert!(st.resolve("gh").is_none());
    assert_eq!(st.remove("gh"), MutResult::Missing);
}

#[test]
fn ttl_expiry() {
    let st = rocks();
    st.shorten("https://a.com", Some("gh"), 60);
    std::thread::sleep(std::time::Duration::from_millis(90));
    assert!(st.resolve("gh").is_none());
    assert!(st.stats("gh").is_none());
}

#[test]
fn list_and_stats() {
    let st = rocks();
    st.shorten("https://a.com", Some("aa"), 0);
    st.shorten("https://b.com", Some("bb"), 0);
    st.resolve("aa");
    st.flush();
    let (rows, total) = st.list(10, 0, "", "");
    assert_eq!(total, 2);
    assert_eq!(rows.len(), 2);
    let s = st.stats("aa").unwrap();
    assert_eq!(s.hits, 1);
    assert_eq!(s.url, "https://a.com");
    // query filter
    let (rows, _) = st.list(10, 0, "", "b.com");
    assert_eq!(rows.len(), 1);
}

#[test]
fn bulk_shorten() {
    let st = rocks();
    let urls: Vec<String> = (0..50).map(|i| format!("https://u{i}.com")).collect();
    let codes = st.shorten_many(&urls, 0);
    assert_eq!(codes.len(), 50);
    for (i, c) in codes.iter().enumerate() {
        assert_eq!(
            st.resolve(c).as_deref(),
            Some(format!("https://u{i}.com").as_str())
        );
    }
}

#[test]
fn restart_survives() {
    // corpus outlives the handle — reopen the same path, link still resolves
    let dir = std::env::temp_dir().join(format!("shrt-rocks-test-{}", uuid()));
    let p = dir.to_str().unwrap();
    {
        let st = Store::open_rocks(p, 0, 1_000, 50).unwrap();
        st.shorten("https://a.com", Some("gh"), 0);
        st.flush();
    }
    {
        let st = Store::open_rocks(p, 0, 0, 50).unwrap();
        assert_eq!(st.resolve("gh").as_deref(), Some("https://a.com"));
    }
}
