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
struct Lru {
    shards: Box<[PlMutex<CacheShard>]>,
    cap_per_shard: usize,
    ttl_ms: i64, // 0 = entries never go stale in-cache (immutability still enforced on writes)
}

impl Lru {
    fn new(cap: usize, ttl_ms: i64) -> Lru {
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

    fn get(&self, code: &str) -> Option<(Arc<str>, i64)> {
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

    fn put(&self, code: &str, u: Arc<str>, e: i64) {
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

    fn remove(&self, code: &str) {
        let mut sh = self.shards[shard_of(code)].lock();
        sh.map.remove(code);
        sh.order.retain(|x| x.as_ref() != code);
    }
}

pub struct KvStore {
    kv: Arc<Kv>,
    cache: Lru,
    dirty: Box<[PlMutex<DirtyMap>]>,
    instance: i32,
    prefix: u8,
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

/// Value codec: "{e}|{c}|{u}" — one byte pass, no JSON.
/// (legacy "{e}|{u}" decodes with c=0)
fn enc_val(e: i64, c: i64, u: &str) -> Vec<u8> {
    let mut v = e.to_string().into_bytes();
    v.push(b'|');
    v.extend_from_slice(c.to_string().as_bytes());
    v.push(b'|');
    v.extend_from_slice(u.as_bytes());
    v
}
fn dec_val(v: &[u8]) -> Option<(i64, i64, &str)> {
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
        let s = KvStore {
            kv: Arc::new(kv),
            cache: Lru::new(cache_entries.max(1), cache_ttl_ms),
            dirty: dirty.into_boxed_slice(),
            instance: inst,
            prefix: ALPHABET[inst as usize],
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
        Ok(s)
    }

    fn flush_hits(&self) -> std::io::Result<()> {
        let mut deltas: Vec<(String, i64)> = Vec::new();
        for sh in self.dirty.iter() {
            let mut d = sh.lock();
            for (c, n) in d.drain() {
                deltas.push((format!("h:{c}"), n));
            }
        }
        self.kv.incrby_many(&deltas)
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
        let v = self.kv.get(&lkey(code)).ok()??;
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
                let ok = self
                    .kv
                    .set(&lkey(a), &enc_val(exp, now, url), ttl_ms, true)
                    .ok()?;
                if ok {
                    Some(a.into())
                } else {
                    None
                }
            }
            None => loop {
                let c = self.gen_code();
                let ok = self
                    .kv
                    .set(&lkey(&c), &enc_val(exp, now, url), ttl_ms, true)
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
            let px = ttl_ms.to_string();
            let mut args: Vec<Vec<u8>> = vec![b"SET".to_vec(), lkey(&c), enc_val(exp, now, u)];
            if ttl_ms > 0 {
                args.push(b"PX".to_vec());
                args.push(px.into_bytes());
            }
            args.push(b"NX".to_vec());
            cmds.push(args);
            codes.push(c);
        }
        let rs = self.kv.pipe(&cmds).unwrap_or_default();
        for (i, r) in rs.iter().enumerate() {
            let ok = matches!(r, crate::kv::Resp::Simple(s) if s == "OK");
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
        let Some(v) = self.kv.get(&lkey(code)).ok().flatten() else {
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
        match self.kv.set(&lkey(code), &enc_val(exp, c, url), px, false) {
            Ok(true) => {
                self.cache.remove(code);
                MutResult::Ok
            }
            _ => MutResult::Missing,
        }
    }

    pub fn remove(&self, code: &str) -> MutResult {
        match self.kv.del(&lkey(code)) {
            Ok(n) if n > 0 => {
                let _ = self.kv.del(&hkey(code));
                self.cache.remove(code);
                MutResult::Ok
            }
            _ => MutResult::Missing,
        }
    }

    /// SCAN-based listing — admin path, O(corpus).
    pub fn list(&self, limit: usize, offset: usize, sort: &str, q: &str) -> (Vec<Link>, usize) {
        let mut items: Vec<Link> = Vec::new();
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
        let v = self.kv.get(&lkey(code)).ok()??;
        let (e, c, u) = dec_val(&v)?;
        let mut hits = match self.kv.get(&hkey(code)) {
            Ok(Some(h)) => std::str::from_utf8(&h)
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
