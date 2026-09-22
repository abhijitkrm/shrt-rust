//! KvStore — the external-store backend (DragonflyDB / Redis / any RESP
//! server). The corpus lives in the KV store; this process keeps only a
//! bounded hot LRU + batched hit counters — memory stays flat regardless of
//! link count.
//!
//! Keys:  l:{code} -> "{expires_ms}|{url}"   (PX set so the DB self-evicts)
//!        h:{code} -> hit counter (INCRBY, flushed in 5ms batches)
//!
//! Multi-instance note: the KV IS the shared state — no log tailing, no
//! convergence lag, and admin mutations work on any node (there is no
//! "remote instance" concept in this mode).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex as PlMutex;
use rustc_hash::FxHashMap;

use crate::base62::ALPHABET;
use crate::kv::Kv;
use crate::store::{now_ms, shard_of, track_hits, Link, MutResult, NUM_SHARDS};

const FLUSH_MS: u64 = 5;

type DirtyMap = FxHashMap<Box<str>, i64>;

struct CacheEntry {
    u: Arc<str>,
    e: i64,  // expires_at ms (0 = never)
    at: i64, // cached_at ms (staleness bound)
}

struct CacheShard {
    map: FxHashMap<Box<str>, CacheEntry>,
    order: VecDeque<Box<str>>, // oldest first
}

/// Bounded segmented FIFO — fixed memory regardless of corpus size.
pub(crate) struct Lru {
    shards: Box<[PlMutex<CacheShard>]>,
    cap_per_shard: usize,
    ttl_ms: i64, // 0 = entries never go stale in-cache (immutability still enforced on writes)
}

impl Lru {

    pub(crate) fn new(cap: usize, ttl_ms: i64) -> Lru {
        let mut v = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            v.push(PlMutex::new(CacheShard {
                map: FxHashMap::default(),
                order: VecDeque::new(),
            }));
        }
        Lru {
            shards: v.into_boxed_slice(),
            cap_per_shard: (cap / NUM_SHARDS).max(16),
            ttl_ms,
        }
    }

    pub(crate) fn get(&self, code: &str) -> Option<(Arc<str>, i64)> {
        let mut sh = self.shards[shard_of(code)].lock();
        let e = sh.map.get(code)?;
        if self.ttl_ms > 0 && now_ms() - e.at > self.ttl_ms {
            sh.map.remove(code);
            sh.order.retain(|x| x.as_ref() != code);
            return None;
        }
        if e.e != 0 && e.e <= now_ms() {
            return None; // expired link
        }
        Some((e.u.clone(), e.e))
    }

    pub(crate) fn put(&self, code: &str, u: Arc<str>, e: i64) {
        let mut sh = self.shards[shard_of(code)].lock();
        if sh.map.contains_key(code) {
            if let Some(en) = sh.map.get_mut(code) {
                en.u = u;
                en.e = e;
                en.at = now_ms();
            }
            return;
        }
        while sh.order.len() >= self.cap_per_shard {
            if let Some(old) = sh.order.pop_front() {
                sh.map.remove(old.as_ref());
            } else {
                break;
            }
        }
        let k: Box<str> = code.into();
        sh.order.push_back(k.clone());
        sh.map.insert(k, CacheEntry { u, e, at: now_ms() });
    }

    pub(crate) fn remove(&self, code: &str) {
        let mut sh = self.shards[shard_of(code)].lock();
        sh.map.remove(code);
        sh.order.retain(|x| x.as_ref() != code);
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Layout {
    /// l:{code} top-level keys — per-key PX expiry, ~120B/link in Redis.
    Key,
    /// l:{code % buckets} hashes — field-packed, ~40-60B/link when small;
    /// expiry enforced on read + janitor sweep (hash fields can't PX).
    Hash,
}

pub struct KvStore {
    kv: Arc<Kv>,
    cache: Lru,
    dirty: Box<[PlMutex<DirtyMap>]>,
    instance: i32,
    prefix: u8,
    layout: Layout,
    buckets: u32, // hash-mode bucket count (l:{code % buckets})
    stop: AtomicBool,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

fn lkey(code: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(code.len() + 2);
    k.extend_from_slice(b"l:");
    k.extend_from_slice(code.as_bytes());
    k
}
fn hkey(code: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(code.len() + 2);
    k.extend_from_slice(b"h:");
    k.extend_from_slice(code.as_bytes());
    k
}
/// hit field name inside a hash bucket ("h:{code}")
fn hfield(code: &str) -> Vec<u8> {
    let mut f = Vec::with_capacity(code.len() + 2);
    f.extend_from_slice(b"h:");
    f.extend_from_slice(code.as_bytes());
    f
}
/// hash-mode bucket key: l:{shard(code) % buckets}
fn bkey(code: &str, buckets: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(12);
    k.extend_from_slice(b"l:");
    k.extend_from_slice((shard_of(code) as u32 % buckets).to_string().as_bytes());
    k
}

/// Value codec: "{e}|{c}|{u}" — one byte pass, no JSON.
/// (legacy "{e}|{u}" decodes with c=0)
pub(crate) fn enc_val(e: i64, c: i64, u: &str) -> Vec<u8> {
    let mut v = e.to_string().into_bytes();
    v.push(b'|');
    v.extend_from_slice(c.to_string().as_bytes());
    v.push(b'|');
    v.extend_from_slice(u.as_bytes());
    v
}
pub(crate) fn dec_val(v: &[u8]) -> Option<(i64, i64, &str)> {
    let p = v.iter().position(|b| *b == b'|')?;
    let e = std::str::from_utf8(&v[..p]).ok()?.parse().ok()?;
    let rest = &v[p + 1..];
    if let Some(q) = rest.iter().position(|b| *b == b'|') {
        let c = std::str::from_utf8(&rest[..q]).ok()?.parse().ok()?;
        let u = std::str::from_utf8(&rest[q + 1..]).ok()?;
        Some((e, c, u))
    } else {
        let u = std::str::from_utf8(rest).ok()?;
        Some((e, 0, u))
    }
}

impl KvStore {
    /// addr: host:port of the RESP server. cache_entries=0 disables the cache.
    pub fn open(
        addr: &str,
        instance: i32,
        cache_entries: usize,
        cache_ttl_ms: i64,
    ) -> std::io::Result<Arc<KvStore>> {
        let kv = Kv::connect(addr)?;
        let inst = instance.clamp(0, 61);
        let mut dirty = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            dirty.push(PlMutex::new(FxHashMap::default()));
        }
        let layout = match std::env::var("KV_LAYOUT").as_deref() {
            Ok("hash") => Layout::Hash,
            _ => Layout::Key,
        };
        let buckets = std::env::var("KV_BUCKETS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000u32);
        let s = KvStore {
            kv: Arc::new(kv),
            cache: Lru::new(cache_entries.max(1), cache_ttl_ms),
            dirty: dirty.into_boxed_slice(),
            instance: inst,
            prefix: ALPHABET[inst as usize],
            layout,
            buckets,
            stop: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
        };
        let s = Arc::new(s);
        // hit-delta flush thread — same 5ms group-commit, targeting the KV
        let wk = Arc::downgrade(&s);
        let t = std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(FLUSH_MS));
            let Some(s) = wk.upgrade() else { break };
            if s.stop.load(Ordering::Relaxed) {
                break;
            }
            let _ = s.flush_hits();
        });
        s.threads.lock().unwrap().push(t);
        // hash mode: hash fields can't PX, so a janitor sweeps expired fields
        if s.layout == Layout::Hash {
            let sweep_ms = std::env::var("KV_SWEEP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3_600_000u64);
            let wk = Arc::downgrade(&s);
            let j = std::thread::spawn(move || {
                // sleep in slices so close() doesn't block on a full sweep_ms
                let mut waited = 0u64;
                loop {
                    std::thread::sleep(Duration::from_millis(50));
                    let Some(s) = wk.upgrade() else { break };
                    if s.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    waited += 50;
                    if waited >= sweep_ms {
                        waited = 0;
                        let _ = s.sweep_expired();
                    }
                }
            });
            s.threads.lock().unwrap().push(j);
        }
        Ok(s)
    }

    /// Janitor: HSCAN every l:* bucket, HDEL fields whose e is past.
    /// Bounds dead-field overhead to ~sweep interval × churn rate.
    fn sweep_expired(&self) -> std::io::Result<()> {
        let mut buckets: Vec<Vec<u8>> = Vec::new();
        self.kv.scan_each("l:*", |k| buckets.push(k))?;
        let now = now_ms();
        let mut dels: Vec<Vec<Vec<u8>>> = Vec::new();
        for b in &buckets {
            let mut dead: Vec<Vec<u8>> = Vec::new();
            self.kv.hscan_each(b, |f, v| {
                if f.starts_with(b"h:") {
                    return;
                }
                if let Some((e, _, _)) = dec_val(&v) {
                    if e != 0 && e <= now {
                        dead.push(f);
                    }
                }
            })?;
            for f in dead {
                dels.push(vec![b"HDEL".to_vec(), b.clone(), f]);
            }
        }
        if !dels.is_empty() {
            self.kv.pipe(&dels)?;
        }
        Ok(())
    }

    fn flush_hits(&self) -> std::io::Result<()> {
        match self.layout {
            Layout::Hash => {
                let mut deltas: Vec<(Vec<u8>, Vec<u8>, i64)> = Vec::new();
                for sh in self.dirty.iter() {
                    let mut d = sh.lock();
                    for (c, n) in d.drain() {
                        deltas.push((bkey(&c, self.buckets), hfield(&c), n));
                    }
                }
                self.kv.hincrby_many(&deltas)
            }
            Layout::Key => {
                let mut deltas: Vec<(String, i64)> = Vec::new();
                for sh in self.dirty.iter() {
                    let mut d = sh.lock();
                    for (c, n) in d.drain() {
                        deltas.push((format!("h:{c}"), n));
                    }
                }
                self.kv.incrby_many(&deltas)
            }
        }
    }

    /// Read the link row for `code` from whichever layout is active.
    fn kv_get(&self, code: &str) -> std::io::Result<Option<Vec<u8>>> {
        match self.layout {
            Layout::Hash => self.kv.hget(&bkey(code, self.buckets), code.as_bytes()),
            Layout::Key => self.kv.get(&lkey(code)),
        }
    }

    fn gen_code(&self) -> Box<str> {
        let mut c = String::with_capacity(8);
        c.push(self.prefix as char);
        let mut b = [0u8; 7];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut b);
        for x in b {
            c.push(ALPHABET[(x as usize) % 62] as char);
        }
        c.into_boxed_str()
    }

    fn bump(&self, code: &str) {
        if track_hits() {
            let mut d = self.dirty[shard_of(code)].lock();
            *d.entry(code.into()).or_insert(0) += 1;
        }
    }

    pub fn resolve(&self, code: &str) -> Option<Arc<str>> {
        if let Some((u, _)) = self.cache.get(code) {
            self.bump(code);
            return Some(u);
        }
        let v = self.kv_get(code).ok()??;
        let (e, _, u) = dec_val(&v)?;
        if e != 0 && e <= now_ms() {
            return None;
        }
        let ua: Arc<str> = u.into();
        self.cache.put(code, ua.clone(), e);
        self.bump(code);
        Some(ua)
    }

    /// SET NX for alias; plain SET after gen for random codes (retry on the
    /// astronomically unlikely NX fail).
    pub fn shorten(&self, url: &str, alias: Option<&str>, ttl_ms: i64) -> Option<Box<str>> {
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        match alias {
            Some(a) => {
                let ok = match self.layout {
                    Layout::Hash => self
                        .kv
                        .hsetnx(&bkey(a, self.buckets), a.as_bytes(), &enc_val(exp, now, url)),
                    Layout::Key => {
                        self.kv.set(&lkey(a), &enc_val(exp, now, url), ttl_ms, true)
                    }
                }
                .ok()?;
                if ok {
                    Some(a.into())
                } else {
                    None
                }
            }
            None => loop {
                let c = self.gen_code();
                let ok = match self.layout {
                    Layout::Hash => self
                        .kv
                        .hsetnx(&bkey(&c, self.buckets), c.as_bytes(), &enc_val(exp, now, url)),
                    Layout::Key => {
                        self.kv.set(&lkey(&c), &enc_val(exp, now, url), ttl_ms, true)
                    }
                }
                .unwrap_or(false);
                if ok {
                    return Some(c);
                }
            },
        }
    }

    /// Pipelined bulk: all SET NX in one round-trip; colliding codes (rare)
    /// are retried serially.
    pub fn shorten_many(&self, urls: &[String], ttl_ms: i64) -> Vec<Box<str>> {
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        let mut codes: Vec<Box<str>> = Vec::with_capacity(urls.len());
        let mut cmds: Vec<Vec<Vec<u8>>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = self.gen_code();
            let args: Vec<Vec<u8>> = match self.layout {
                Layout::Hash => vec![
                    b"HSETNX".to_vec(),
                    bkey(&c, self.buckets),
                    c.as_bytes().to_vec(),
                    enc_val(exp, now, u),
                ],
                Layout::Key => {
                    let px = ttl_ms.to_string();
                    let mut a = vec![b"SET".to_vec(), lkey(&c), enc_val(exp, now, u)];
                    if ttl_ms > 0 {
                        a.push(b"PX".to_vec());
                        a.push(px.into_bytes());
                    }
                    a.push(b"NX".to_vec());
                    a
                }
            };
            cmds.push(args);
            codes.push(c);
        }
        let rs = self.kv.pipe(&cmds).unwrap_or_default();
        for (i, r) in rs.iter().enumerate() {
            let ok = match self.layout {
                Layout::Hash => matches!(r, crate::kv::Resp::Int(1)),
                Layout::Key => matches!(r, crate::kv::Resp::Simple(s) if s == "OK"),
            };
            if !ok {
                // retry once serially
                if let Some(c2) = self.shorten(&urls[i], None, ttl_ms) {
                    codes[i] = c2;
                }
            }
        }
        codes
    }

    pub fn update(&self, code: &str, url: &str, ttl_ms: i64, has_ttl: bool) -> MutResult {
        let Some(v) = self.kv_get(code).ok().flatten() else {
            return MutResult::Missing;
        };
        let Some((e, c, _)) = dec_val(&v) else {
            return MutResult::Missing;
        };
        let exp = if has_ttl {
            if ttl_ms > 0 {
                now_ms() + ttl_ms
            } else {
                0
            }
        } else {
            e
        };
        let px = if exp > 0 { exp - now_ms() } else { 0 };
        let ok = match self.layout {
            Layout::Hash => self
                .kv
                .hset(&bkey(code, self.buckets), code.as_bytes(), &enc_val(exp, c, url))
                .unwrap_or(false),
            Layout::Key => self
                .kv
                .set(&lkey(code), &enc_val(exp, c, url), px, false)
                .unwrap_or(false),
        };
        if ok {
            self.cache.remove(code);
            MutResult::Ok
        } else {
            MutResult::Missing
        }
    }

    pub fn remove(&self, code: &str) -> MutResult {
        match self.layout {
            Layout::Hash => {
                let b = bkey(code, self.buckets);
                match self.kv.hdel(&b, code.as_bytes()) {
                    Ok(n) if n > 0 => {
                        let _ = self.kv.hdel(&b, &hfield(code));
                        self.cache.remove(code);
                        MutResult::Ok
                    }
                    _ => MutResult::Missing,
                }
            }
            Layout::Key => match self.kv.del(&lkey(code)) {
                Ok(n) if n > 0 => {
                    let _ = self.kv.del(&hkey(code));
                    self.cache.remove(code);
                    MutResult::Ok
                }
                _ => MutResult::Missing,
            },
        }
    }

    /// SCAN-based listing — admin path, O(corpus).
    pub fn list(&self, limit: usize, offset: usize, sort: &str, q: &str) -> (Vec<Link>, usize) {
        let mut items: Vec<Link> = Vec::new();
        if self.layout == Layout::Hash {
            // buckets -> HSCAN; fields "h:{code}" are hit counters
            let mut buckets: Vec<Vec<u8>> = Vec::new();
            let _ = self.kv.scan_each("l:*", |k| buckets.push(k));
            let mut hits: FxHashMap<String, i64> = FxHashMap::default();
            for b in &buckets {
                let mut fields: Vec<(String, Vec<u8>)> = Vec::new();
                let _ = self.kv.hscan_each(b, |f, v| {
                    if let Ok(fs) = String::from_utf8(f) {
                        fields.push((fs, v));
                    }
                });
                for (f, v) in fields {
                    if let Some(c) = f.strip_prefix("h:") {
                        hits.insert(
                            c.to_string(),
                            std::str::from_utf8(&v)
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0),
                        );
                    } else if let Some((e, c, u)) = dec_val(&v) {
                        if e != 0 && e <= now_ms() {
                            continue;
                        }
                        if !q.is_empty() && !f.contains(q) && !u.contains(q) {
                            continue;
                        }
                        items.push(Link {
                            code: f,
                            url: u.to_string(),
                            hits: 0,
                            created_at: c,
                            expires_at: if e != 0 { Some(e) } else { None },
                        });
                    }
                }
            }
            for it in &mut items {
                it.hits = hits.get(&it.code).copied().unwrap_or(0);
            }
        } else {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let _ = self.kv.scan_each("l:*", |k| keys.push(k));
        // one pipeline round-trip: GET l:k and GET h:k for every key
        let cmds: Vec<Vec<Vec<u8>>> = keys
            .iter()
            .flat_map(|k| {
                let code = &k[2..];
                [
                    vec![b"GET".to_vec(), k.clone()],
                    vec![
                        b"GET".to_vec(),
                        hkey(std::str::from_utf8(code).unwrap_or("")),
                    ],
                ]
            })
            .collect();
        let rs = self.kv.pipe(&cmds).unwrap_or_default();
        for (i, k) in keys.iter().enumerate() {
            let code = String::from_utf8_lossy(&k[2..]).to_string();
            let Some(crate::kv::Resp::Bulk(Some(v))) = rs.get(2 * i) else {
                continue;
            };
            let Some((e, c, u)) = dec_val(v) else {
                continue;
            };
            if !q.is_empty() && !code.contains(q) && !u.contains(q) {
                continue;
            }
            let hits = match rs.get(2 * i + 1) {
                Some(crate::kv::Resp::Bulk(Some(h))) => std::str::from_utf8(h)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                _ => 0,
            };
            items.push(Link {
                code,
                url: u.to_string(),
                hits,
                created_at: c,
                expires_at: if e != 0 { Some(e) } else { None },
            });
        }
        }
        if sort == "hits" {
            items.sort_by_key(|x| std::cmp::Reverse(x.hits));
        }
        let total = items.len();
        (
            items
                .into_iter()
                .skip(offset.min(total))
                .take(limit)
                .collect(),
            total,
        )
    }

    pub fn stats(&self, code: &str) -> Option<Link> {
        let (v, hv) = match self.layout {
            Layout::Hash => {
                let b = bkey(code, self.buckets);
                let rs = self
                    .kv
                    .pipe(&[
                        vec![b"HGET".to_vec(), b.clone(), code.as_bytes().to_vec()],
                        vec![b"HGET".to_vec(), b, hfield(code)],
                    ])
                    .ok()?;
                let get = |r: Option<&crate::kv::Resp>| match r {
                    Some(crate::kv::Resp::Bulk(b)) => b.clone(),
                    _ => None,
                };
                (get(rs.first()), get(rs.get(1)))
            }
            Layout::Key => (
                self.kv.get(&lkey(code)).ok()?,
                self.kv.get(&hkey(code)).ok()?,
            ),
        };
        let v = v?;
        let (e, c, u) = dec_val(&v)?;
        let mut hits = match hv {
            Some(h) => std::str::from_utf8(&h)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            _ => 0,
        };
        if let Some(n) = self.dirty[shard_of(code)].lock().get(code) {
            hits += n;
        }
        Some(Link {
            code: code.to_string(),
            url: u.to_string(),
            hits,
            created_at: c,
            expires_at: if e != 0 { Some(e) } else { None },
        })
    }

    pub fn seed(&self, urls: &[String]) -> usize {
        self.shorten_many(urls, 0);
        let _ = self.flush_hits();
        urls.len()
    }

    pub fn is_empty(&self) -> bool {
        let mut any = false;
        let _ = self.kv.scan_each("l:*", |_| any = true);
        !any
    }

    pub fn instance(&self) -> i32 {
        self.instance
    }
    pub fn persistent(&self) -> bool {
        true
    }
    pub fn flush(&self) {
        let _ = self.flush_hits();
    }
    pub fn poll_tails(&self) {}
    pub fn compact(&self) {}

    pub fn close(&self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.lock().unwrap().drain(..) {
            let _ = t.join();
        }
        let _ = self.flush_hits();
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if !self.stop.load(Ordering::Relaxed) {
            self.stop.store(true, Ordering::Relaxed);
            let _ = self.flush_hits();
        }
    }
}
