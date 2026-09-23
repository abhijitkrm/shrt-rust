//! RocksDB-backed store — disk-native corpus, ~0 required RAM.
//! Links: default CF `code -> {e}|{c}|{u}` (same codec as KV mode).
//! Hits: `hits` CF as u64 merge operands (no read-modify-write).
//! Expiry: read-path check + compaction filter drops dead keys for free
//! (plus a periodic compact_range so reclamation doesn't wait on organic
//! compaction). NX-create uses a mutex around get+put — RocksDB has no
//! insert-if-absent, and an embedded DB is only written by this process.
use crate::store::{Link, MutResult};
use crate::store_kv::{dec_val, enc_val, Lru};
use parking_lot::Mutex as PlMutex;
use rocksdb::compaction_filter::Decision;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MergeOperands,
    MultiThreaded, Options, WriteBatch,
};
use rustc_hash::FxHashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const NUM_SHARDS: usize = 16;
const FLUSH_MS: u64 = 5;
const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const HITS_CF: &str = "hits";

type Db = DBWithThreadMode<MultiThreaded>;
type DirtyMap = FxHashMap<String, i64>;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn shard_of(code: &str) -> usize {
    let mut h: u64 = 5381;
    for &b in code.as_bytes() {
        h = h.wrapping_mul(33) ^ b as u64;
    }
    h as usize % NUM_SHARDS
}

/// hits CF merge: u64 little-endian add.
fn hits_merge(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut n: u64 = existing
        .and_then(|v| <[u8; 8]>::try_from(v).ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0);
    for op in operands.iter() {
        if let Ok(b) = <[u8; 8]>::try_from(op) {
            n = n.wrapping_add(u64::from_le_bytes(b));
        }
    }
    Some(n.to_le_bytes().to_vec())
}

/// default CF compaction filter: drop keys whose embedded expiry is past.
fn expired_filter(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    let Ok(s) = std::str::from_utf8(value) else {
        return Decision::Keep;
    };
    let Some(end) = s.find('|') else {
        return Decision::Keep;
    };
    let Ok(e) = s[..end].parse::<i64>() else {
        return Decision::Keep;
    };
    if e != 0 && e <= now_ms() {
        Decision::Remove
    } else {
        Decision::Keep
    }
}

pub struct RocksStore {
    db: Db,
    cache: Lru,
    dirty: Box<[PlMutex<DirtyMap>]>,
    instance: i32,
    prefix: u8,
    alias_lock: Mutex<()>, // serializes get+put for NX-create
    stop: AtomicBool,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl RocksStore {
    /// path: RocksDB dir (default DATA_DIR/rocksdb via env dispatch).
    /// cache_entries=0 disables the read-through cache.
    pub fn open(
        path: &str,
        instance: i32,
        cache_entries: usize,
        cache_ttl_ms: i64,
    ) -> io::Result<Arc<RocksStore>> {
        let mut bbo = BlockBasedOptions::default();
        bbo.set_bloom_filter(10.0, true); // ~10 bits/key, negative lookups stay in RAM
        bbo.set_block_cache(&rocksdb::Cache::new_lru_cache(256 << 20));

        let mut links_opts = Options::default();
        links_opts.set_block_based_table_factory(&bbo);
        links_opts.set_compaction_filter("expired", expired_filter);
        links_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let mut hits_opts = Options::default();
        hits_opts.set_merge_operator_associative("hits_add", hits_merge);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(256 << 20);
        db_opts.set_max_write_buffer_number(4);
        db_opts.set_target_file_size_base(256 << 20);
        db_opts.increase_parallelism(4);

        let db = Db::open_cf_descriptors(
            &db_opts,
            path,
            vec![
                ColumnFamilyDescriptor::new("default", links_opts),
                ColumnFamilyDescriptor::new(HITS_CF, hits_opts),
            ],
        )
        .map_err(io::Error::other)?;
        db.cf_handle(HITS_CF)
            .ok_or_else(|| io::Error::other("hits cf"))?;

        let inst = if instance < 0 {
            (std::process::id() % 62) as i32
        } else {
            instance.clamp(0, 61)
        };
        let mut dirty = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            dirty.push(PlMutex::new(FxHashMap::default()));
        }
        let s = Arc::new(RocksStore {
            db,
            cache: Lru::new(cache_entries.max(1), cache_ttl_ms),
            dirty: dirty.into_boxed_slice(),
            instance: inst,
            prefix: ALPHABET[inst as usize],
            alias_lock: Mutex::new(()),
            stop: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
        });

        // hit-delta flush: batched merge_cf every 5ms — same group-commit as KV
        {
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
        }
        // periodic full compaction — runs the expiry filter, reclaims dead keys
        {
            let sweep_ms = std::env::var("SWEEP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3_600_000u64);
            let wk = Arc::downgrade(&s);
            let j = std::thread::spawn(move || {
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
                        s.db.compact_range(None::<&[u8]>, None::<&[u8]>);
                    }
                }
            });
            s.threads.lock().unwrap().push(j);
        }
        Ok(s)
    }

    fn bump(&self, code: &str) {
        *self.dirty[shard_of(code)]
            .lock()
            .entry(code.into())
            .or_default() += 1;
    }

    fn flush_hits(&self) -> io::Result<()> {
        let mut b = WriteBatch::default();
        let mut any = false;
        for sh in self.dirty.iter() {
            let mut d = sh.lock();
            let cf = self.db.cf_handle(HITS_CF).unwrap();
            for (c, n) in d.drain() {
                b.merge_cf(&cf, c.as_bytes(), n.to_le_bytes());
                any = true;
            }
        }
        if any {
            self.db.write(b).map_err(io::Error::other)?;
        }
        Ok(())
    }

    fn hits_of(&self, code: &str) -> i64 {
        let kv = self
            .db
            .get_cf(&self.db.cf_handle(HITS_CF).unwrap(), code.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0) as i64;
        kv + self
            .dirty
            .get(shard_of(code))
            .map(|d| d.lock().get(code).copied().unwrap_or(0))
            .unwrap_or(0)
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

    /// decode a stored row, dropping it if past expiry
    fn row(&self, code: &str) -> io::Result<Option<(i64, i64, String)>> {
        let Some(v) = self.db.get(code.as_bytes()).map_err(io::Error::other)? else {
            return Ok(None);
        };
        let Some((e, c, u)) = dec_val(&v) else {
            return Ok(None);
        };
        if e != 0 && e <= now_ms() {
            let _ = self.db.delete(code.as_bytes()); // lazy reap
            return Ok(None);
        }
        Ok(Some((e, c, u.to_string())))
    }

    pub fn resolve(&self, code: &str) -> Option<Arc<str>> {
        if let Some((u, _)) = self.cache.get(code) {
            crate::metrics::cache_hit();
            self.bump(code);
            return Some(u);
        }
        crate::metrics::cache_miss();
        let t0 = std::time::Instant::now();
        let r = self.row(code);
        crate::metrics::store_read(t0.elapsed().as_micros() as i64);
        let (e, _, u) = r.ok()??;
        let ua: Arc<str> = u.into();
        self.cache.put(code, ua.clone(), e);
        self.bump(code);
        Some(ua)
    }

    /// get+put under alias_lock — RocksDB has no insert-if-absent.
    fn put_nx(&self, code: &str, v: &[u8]) -> io::Result<bool> {
        let _g = self.alias_lock.lock().unwrap();
        if self
            .db
            .get(code.as_bytes())
            .map_err(io::Error::other)?
            .is_some()
        {
            return Ok(false);
        }
        self.db.put(code.as_bytes(), v).map_err(io::Error::other)?;
        Ok(true)
    }

    pub fn shorten(&self, url: &str, alias: Option<&str>, ttl_ms: i64) -> Option<Box<str>> {
        crate::metrics::store_write();
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        match alias {
            Some(a) => self
                .put_nx(a, &enc_val(exp, now, url))
                .ok()?
                .then(|| a.into()),
            None => loop {
                let c = self.gen_code();
                if self.put_nx(&c, &enc_val(exp, now, url)).unwrap_or(false) {
                    return Some(c);
                }
            },
        }
    }

    pub fn shorten_many(&self, urls: &[String], ttl_ms: i64) -> Vec<Box<str>> {
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        let mut codes: Vec<Box<str>> = Vec::with_capacity(urls.len());
        let mut b = WriteBatch::default();
        for u in urls {
            let c = self.gen_code();
            b.put(c.as_bytes(), enc_val(exp, now, u));
            codes.push(c);
        }
        if self.db.write(b).is_err() {
            for i in 0..codes.len() {
                let c = codes[i].clone();
                if self.put_nx(&c, &enc_val(exp, now, &urls[i])).is_err() {
                    if let Some(c2) = self.shorten(&urls[i], None, ttl_ms) {
                        codes[i] = c2;
                    }
                }
            }
        }
        codes
    }

    pub fn update(&self, code: &str, url: &str, ttl_ms: i64, has_ttl: bool) -> MutResult {
        let Ok(Some((e, c, _))) = self.row(code) else {
            return MutResult::Missing;
        };
        let exp = if has_ttl { now_ms() + ttl_ms.max(0) } else { e };
        match self.db.put(code.as_bytes(), enc_val(exp, c, url)) {
            Ok(()) => {
                self.cache.remove(code);
                MutResult::Ok
            }
            Err(_) => MutResult::Missing,
        }
    }

    pub fn remove(&self, code: &str) -> MutResult {
        // delete is an unconditional tombstone — check existence first
        let Ok(Some(_)) = self.row(code) else {
            return MutResult::Missing;
        };
        let mut b = WriteBatch::default();
        b.delete(code.as_bytes());
        b.delete_cf(&self.db.cf_handle(HITS_CF).unwrap(), code.as_bytes());
        match self.db.write(b) {
            Ok(()) => {
                self.cache.remove(code);
                MutResult::Ok
            }
            Err(_) => MutResult::Missing,
        }
    }

    /// admin listing — iterator over the corpus.
    pub fn list(&self, limit: usize, offset: usize, sort: &str, q: &str) -> (Vec<Link>, usize) {
        let _ = self.flush_hits();
        let mut items: Vec<Link> = Vec::new();
        for r in self.db.iterator(IteratorMode::Start) {
            let Ok((k, v)) = r else { continue };
            let Ok(code) = std::str::from_utf8(&k) else {
                continue;
            };
            let Some((e, c, u)) = dec_val(&v) else {
                continue;
            };
            if e != 0 && e <= now_ms() {
                continue;
            }
            if !q.is_empty() && !code.contains(q) && !u.contains(q) {
                continue;
            }
            items.push(Link {
                code: code.to_string(),
                url: u.to_string(),
                hits: self.hits_of(code),
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
        let (e, c, u) = self.row(code).ok()??;
        Some(Link {
            code: code.to_string(),
            url: u,
            hits: self.hits_of(code),
            created_at: c,
            expires_at: if e != 0 { Some(e) } else { None },
        })
    }

    pub fn seed(&self, urls: &[String]) -> usize {
        self.shorten_many(urls, 0);
        let _ = self.flush_hits();
        urls.len()
    }

    /// /api/health probe: one point lookup proves the DB is open & readable.
    pub fn healthy(&self) -> bool {
        self.db.get(b"").is_ok()
    }

    pub fn is_empty(&self) -> bool {
        self.db.iterator(IteratorMode::Start).next().is_none()
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
    pub fn compact(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }

    pub fn close(&self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.lock().unwrap().drain(..) {
            let _ = t.join();
        }
        let _ = self.flush_hits();
    }
}
