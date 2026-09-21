use std::fs;
use std::time::Duration;

use shrt::base62;
use shrt::store::{now_ms, MutResult, Store};

fn tmpdir() -> String {
    let dir = std::env::temp_dir().join(format!(
        "shrt-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    dir.to_string_lossy().to_string()
}

#[test]
fn base62_encode() {
    for (n, want) in [(0u64, "0"), (1, "1"), (61, "Z"), (62, "10"), (3843, "ZZ")] {
        assert_eq!(base62::encode(n), want, "encode({n})");
    }
}

#[test]
fn shorten_generates_random_8char_codes() {
    let s = Store::new(":memory:", -1).unwrap();
    let a = s.shorten("https://example.com", None, 0).unwrap();
    let b = s.shorten("https://example.org", None, 0).unwrap();
    let ok = |c: &str| c.len() == 8 && c.bytes().all(|b| b.is_ascii_alphanumeric());
    assert!(ok(&a) && ok(&b), "bad codes {a} {b}");
    assert_ne!(a, b, "duplicate codes");
}

#[test]
fn resolve_counts_hits() {
    let s = Store::new(":memory:", -1).unwrap();
    let code = s.shorten("https://example.com", None, 0).unwrap();
    for _ in 0..2 {
        assert_eq!(&*s.resolve(&code).unwrap(), "https://example.com");
    }
    let st = s.stats(&code).unwrap();
    assert_eq!(st.hits, 2);
    assert_eq!(st.url, "https://example.com");
}

#[test]
fn resolve_misses() {
    let s = Store::new(":memory:", -1).unwrap();
    assert!(s.resolve("nope").is_none());
    assert!(s.stats("nope").is_none());
}

#[test]
fn alias_collision() {
    let s = Store::new(":memory:", -1).unwrap();
    assert_eq!(
        &*s.shorten("https://a.com", Some("my-link"), 0).unwrap(),
        "my-link"
    );
    assert_eq!(&*s.resolve("my-link").unwrap(), "https://a.com");
    assert!(s.shorten("https://b.com", Some("my-link"), 0).is_none());
    let gen = s.shorten("https://c.com", None, 0).unwrap();
    assert_ne!(&*gen, "my-link");
    assert_eq!(&*s.resolve(&gen).unwrap(), "https://c.com");
}

#[test]
fn expired_links_stop_resolving() {
    let s = Store::new(":memory:", -1).unwrap();
    let code = s.shorten("https://example.com", None, 1).unwrap();
    assert!(s.resolve(&code).is_some());
    std::thread::sleep(Duration::from_millis(5));
    assert!(s.resolve(&code).is_none(), "expired link resolved");
}

#[test]
fn shorten_many_aligned() {
    let s = Store::new(":memory:", -1).unwrap();
    let urls: Vec<String> = ["https://a.com", "https://b.com", "https://c.com"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let codes = s.shorten_many(&urls, 0);
    assert_eq!(codes.len(), 3);
    let mut seen = std::collections::HashSet::new();
    for c in &codes {
        seen.insert(c.clone());
    }
    assert_eq!(seen.len(), 3, "duplicate codes");
    for (i, c) in codes.iter().enumerate() {
        assert_eq!(&*s.resolve(c).unwrap(), urls[i].as_str());
    }
}

#[test]
fn persists_across_reopen() {
    let dir = tmpdir();
    {
        let s1 = Store::new(&dir, -1).unwrap();
        let code = s1.shorten("https://example.com", None, 0).unwrap();
        s1.resolve(&code);
        s1.resolve(&code);
        s1.close();

        let s2 = Store::new(&dir, 0).unwrap(); // same instance replays own log
        assert_eq!(&*s2.resolve(&code).unwrap(), "https://example.com");
        assert_eq!(s2.stats(&code).unwrap().hits, 3);
        s2.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn codes_prefix_sharded() {
    let dir = tmpdir();
    {
        let a = Store::new(&dir, 0).unwrap();
        let b = Store::new(&dir, 1).unwrap();
        let ca = a.shorten("https://a.com", None, 0).unwrap();
        let cb = b.shorten("https://b.com", None, 0).unwrap();
        assert_ne!(ca, cb);
        assert_ne!(
            ca.as_bytes()[0],
            cb.as_bytes()[0],
            "codes not prefix-disjoint"
        );
        a.close();
        b.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn sibling_tailing_converges() {
    let dir = tmpdir();
    {
        let a = Store::new(&dir, 0).unwrap();
        let b = Store::new(&dir, 1).unwrap();
        let code = a.shorten("https://a.com", None, 0).unwrap();
        a.flush();
        b.poll_tails();
        assert_eq!(&*b.resolve(&code).unwrap(), "https://a.com");
        a.close();
        b.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn sibling_sees_hits_via_tail() {
    let dir = tmpdir();
    {
        let a = Store::new(&dir, 0).unwrap();
        let b = Store::new(&dir, 1).unwrap();
        let code = a.shorten("https://a.com", None, 0).unwrap();
        a.flush();
        b.poll_tails();
        a.resolve(&code);
        a.resolve(&code);
        a.flush();
        b.poll_tails();
        assert_eq!(b.stats(&code).unwrap().hits, 2);
        a.close();
        b.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn remove_tombstone_survives_reopen() {
    let dir = tmpdir();
    {
        let s1 = Store::new(&dir, -1).unwrap();
        let code = s1.shorten("https://example.com", None, 0).unwrap();
        assert_eq!(s1.remove(&code), MutResult::Ok);
        assert!(s1.resolve(&code).is_none());
        s1.close();

        let s2 = Store::new(&dir, 0).unwrap();
        assert!(s2.resolve(&code).is_none(), "tombstone not replayed");
        assert_eq!(s2.remove("nope"), MutResult::Missing);
        s2.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn update_persists_across_reopen() {
    let dir = tmpdir();
    {
        let s1 = Store::new(&dir, -1).unwrap();
        let code = s1.shorten("https://old.example", None, 0).unwrap();
        s1.resolve(&code); // 1 hit
        assert_eq!(
            s1.update(&code, "https://new.example", 60_000, true),
            MutResult::Ok
        );
        assert_eq!(&*s1.resolve(&code).unwrap(), "https://new.example");
        s1.close();

        let s2 = Store::new(&dir, 0).unwrap();
        let e = s2.stats(&code).unwrap();
        assert_eq!(e.url, "https://new.example");
        assert!(e.expires_at.unwrap_or(0) > now_ms(), "expiry not preserved");
        assert!(e.hits >= 1, "hits = {}", e.hits);
        s2.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn remote_owned_mutations_return_remote() {
    let dir = tmpdir();
    {
        let a = Store::new(&dir, 0).unwrap();
        let b = Store::new(&dir, 1).unwrap();
        let code = a.shorten("https://a.example", None, 0).unwrap();
        a.flush();
        b.poll_tails();
        assert_eq!(
            b.update(&code, "https://x.example", 0, false),
            MutResult::Remote
        );
        assert_eq!(b.remove(&code), MutResult::Remote);
        assert_eq!(a.remove(&code), MutResult::Ok);
        a.close();
        b.close();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn list_paginates_sorts_filters() {
    let s = Store::new(":memory:", -1).unwrap();
    let a = s.shorten("https://aaa.example", None, 0).unwrap();
    s.shorten("https://bbb.example", None, 0);
    s.shorten("https://ccc.example", None, 0);
    s.resolve(&a);
    s.resolve(&a);

    let (links, total) = s.list(50, 0, "created", "");
    assert_eq!(total, 3);
    assert_eq!(links.len(), 3);
    let (page, _) = s.list(2, 0, "created", "");
    assert_eq!(page.len(), 2);
    let (by_hits, _) = s.list(50, 0, "hits", "");
    assert_eq!(&*by_hits[0].code, &*a);
    let (filtered, ftotal) = s.list(50, 0, "created", "bbb");
    assert_eq!(ftotal, 1);
    assert_eq!(filtered[0].url, "https://bbb.example");
}

#[test]
fn compact_preserves_rows_and_truncates() {
    let dir = tmpdir();
    {
        let s = Store::new(&dir, 0).unwrap();
        let code = s.shorten("https://example.com", None, 0).unwrap();
        s.resolve(&code);
        s.compact();
        s.close();

        let snap = std::path::Path::new(&dir).join("data-0.snap");
        let meta = fs::metadata(&snap).expect("snapshot missing");
        assert!(meta.len() > 0, "snapshot empty");

        let s2 = Store::new(&dir, 0).unwrap();
        assert_eq!(&*s2.resolve(&code).unwrap(), "https://example.com");
        assert_eq!(s2.stats(&code).unwrap().hits, 2);
        s2.close();
    }
    let _ = fs::remove_dir_all(&dir);
}
