//! In-memory KV + append-only-log persistence.
//!
//! - reads: pure map lookup (no disk, no SQL)
//! - writes: map set + buffered append; flush batch every FLUSH_MS (<=5ms
//!   loss window), fsync every FSYNC_MS
//! - multi-instance: per-instance log shards (data-<i>.log). Generated codes
//!   are 8 chars: ALPHABET[instance] + 7 random base62 chars — the prefix
//!   keeps codes unique across instances with zero coordination and lets a
//!   read-miss tail exactly the owning shard's log.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use rand::Rng;
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::aof::{self, Aof, TailReader};
use crate::base62::ALPHABET;
use crate::codec::{del_line, hit_line_into, parse_op, row_line, row_line_into, Op};

const FLUSH_MS: u64 = 5;
const FSYNC_MS: u64 = 500;
const TAIL_MIN_INTERVAL: i64 = 200; // ms, rate limit for lazy on-miss tail polls
const FLUSH_BYTES: usize = 256 << 10;
const CODE_LEN: usize = 8; // 1 instance-prefix char + 7 random base62 chars
const MAX_INSTANCES: usize = 62; // prefix char space
const MAX_STRAY_HITS: usize = 10_000;
const NUM_SHARDS: usize = 256;

fn tail_ms() -> u64 {
    std::env::var("TAIL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn track_hits() -> bool {
    std::env::var("HITS").map(|v| v != "0").unwrap_or(true)
}

#[inline]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Public view of a stored entry.
#[derive(Serialize)]
pub struct Link {
    pub code: String,
    pub url: String,
    pub hits: i64,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

struct Entry {
    u: Arc<str>,
    a: i64, // created_at ms
    e: i64, // expires_at ms, 0 = never
    i: i32, // owning instance
    h: AtomicI64,
    oh: AtomicI64, // own hits only (what snapshots persist)
}

#[derive(Default)]
struct StrayHit {
    t: i64, // total deltas seen for a code whose row hasn't arrived
    o: i64, // of those, deltas this instance logged
}

type StrayMap = FxHashMap<Box<str>, StrayHit>;

struct Shard {
    data: RwLock<FxHashMap<Box<str>, Entry>>,
    dirty: Mutex<FxHashMap<Box<str>, i64>>,
}

/// Outcome of update/remove.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MutResult {
    Ok,
    Missing,
    Remote,
}

#[derive(Default)]
struct Tail {
    readers: FxHashMap<String, TailReader>,
    stray: StrayMap,
    last_poll: i64,
}

struct Inner {
    shards: Vec<Shard>,
    tail: Mutex<Tail>,
    aof: Option<Mutex<Aof>>,
    instance: i32,
    prefix: u8,
    dir: PathBuf,
    own_name: String,
    lock_path: Option<PathBuf>,
    gate: RwLock<()>, // read: mutations; write: compact (excludes them)
    stop: AtomicBool,
    closed: AtomicBool,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

#[inline]
fn shard_of(code: &str) -> usize {
    // FNV-1a — cheap and well-spread
    let mut h: u32 = 2166136261;
    for &b in code.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    (h as usize) & (NUM_SHARDS - 1)
}

fn alpha_idx(c: u8) -> i32 {
    let mut t = -1;
    for (i, &x) in ALPHABET.iter().enumerate() {
        if x == c {
            t = i as i32;
            break;
        }
    }
    t
}

fn snap_name(i: i32) -> String {
    format!("data-{i}.snap")
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

/// Fold one parsed log line into the index. `stray` is the caller-held
/// stray-hit map (borrowed from the tail lock).
fn apply(inner: &Inner, o: &Op, stray: &mut StrayMap) {
    if !o.x.is_empty() {
        inner.shards[shard_of(&o.x)]
            .data
            .write()
            .remove(o.x.as_str());
        stray.remove(o.x.as_str());
        return;
    }
    if !o.h.is_empty() {
        let own = inner.instance as i64 == o.i;
        let sh = &inner.shards[shard_of(&o.h)];
        {
            let g = sh.data.read();
            if let Some(e) = g.get(o.h.as_str()) {
                e.h.fetch_add(o.d, Ordering::Relaxed);
                if own {
                    e.oh.fetch_add(o.d, Ordering::Relaxed);
                }
                return;
            }
        }
        if stray.len() >= MAX_STRAY_HITS {
            if let Some(k) = stray.keys().next().cloned() {
                stray.remove(&k);
            }
        }
        let st = stray.entry(o.h.clone().into_boxed_str()).or_default();
        st.t += o.d;
        if own {
            st.o += o.d;
        }
        return;
    }
    if o.c.is_empty() {
        return;
    }
    let (st_t, st_o) = match stray.remove(o.c.as_str()) {
        Some(s) => (s.t, s.o),
        None => (0, 0),
    };
    let e = Entry {
        u: Arc::from(o.u.as_str()),
        a: o.a,
        e: if o.has_e { o.e } else { 0 },
        i: o.i as i32,
        h: AtomicI64::new(o.n + st_t),
        oh: AtomicI64::new(o.n + st_o),
    };
    inner.shards[shard_of(&o.c)]
        .data
        .write()
        .insert(o.c.clone().into_boxed_str(), e);
}

fn apply_line(inner: &Inner, line: &[u8], stray: &mut StrayMap) {
    let mut o = Op::default();
    if parse_op(line, &mut o) {
        apply(inner, &o, stray);
    }
}

/// Discover new sibling shards and pull newly appended lines into `tail`.
fn poll_locked(inner: &Inner, tail: &mut Tail) {
    for f in aof::shard_files(&inner.dir, &inner.own_name) {
        if !tail.readers.contains_key(&f) {
            tail.readers
                .insert(f.clone(), TailReader::from_start(inner.dir.join(&f)));
        }
    }
    let Tail { readers, stray, .. } = tail;
    for t in readers.values_mut() {
        t.read_new(|l| apply_line(inner, l, stray));
    }
    tail.last_poll = now_ms();
}

impl Store {
    /// dir ":memory:" disables persistence. instance < 0 auto-claims the
    /// lowest free instance id via lock files.
    pub fn new(dir: &str, instance: i32) -> std::io::Result<Store> {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Shard {
                data: RwLock::new(FxHashMap::default()),
                dirty: Mutex::new(FxHashMap::default()),
            });
        }
        let mut inner = Inner {
            shards,
            tail: Mutex::new(Tail::default()),
            aof: None,
            instance: 0,
            prefix: ALPHABET[0],
            dir: PathBuf::new(),
            own_name: String::new(),
            lock_path: None,
            gate: RwLock::new(()),
            stop: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
        };

        if dir != ":memory:" {
            let dirp = PathBuf::from(dir);
            let (id, lock) = if instance >= 0 {
                (instance, None)
            } else {
                let (id, lock) = aof::claim_instance(&dirp)?;
                (id, Some(lock))
            };
            if id as usize >= MAX_INSTANCES {
                if let Some(l) = &lock {
                    let _ = std::fs::remove_file(l);
                }
                return Err(std::io::Error::other(format!(
                    "instance {id} >= max {MAX_INSTANCES}"
                )));
            }
            inner.instance = id;
            inner.prefix = ALPHABET[id as usize];
            inner.dir = dirp;
            inner.own_name = format!("data-{id}.log");
            inner.lock_path = lock;
            inner.aof = Some(Mutex::new(Aof::new(&inner.dir, &inner.own_name)?));
        }

        let store = Store {
            inner: Arc::new(inner),
        };
        if store.inner.aof.is_some() {
            store.load_all();
        }

        // flush thread
        {
            let wk = store.inner.clone();
            let h = std::thread::spawn(move || loop {
                std::thread::sleep(Duration::from_millis(FLUSH_MS));
                if wk.stop.load(Ordering::Relaxed) {
                    break;
                }
                wk.flush_public();
            });
            store.inner.threads.lock().push(h);
        }
        if store.inner.aof.is_some() {
            // fsync thread (checks stop every 50ms so close() stays snappy)
            let wk = store.inner.clone();
            let h = std::thread::spawn(move || loop {
                for _ in 0..(FSYNC_MS / 50) {
                    std::thread::sleep(Duration::from_millis(50));
                    if wk.stop.load(Ordering::Relaxed) {
                        return;
                    }
                }
                if let Some(a) = &wk.aof {
                    a.lock().sync();
                }
            });
            store.inner.threads.lock().push(h);

            let tms = tail_ms();
            if tms > 0 {
                let wk = store.inner.clone();
                let h = std::thread::spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(tms));
                    if wk.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    wk.poll_tails_pub();
                });
                store.inner.threads.lock().push(h);
            }
        }
        Ok(store)
    }

    pub fn instance(&self) -> i32 {
        self.inner.instance
    }

    /// Replay own snapshot + own log + all sibling logs present at boot.
    fn load_all(&self) {
        let inner = &self.inner;
        let mut tail = inner.tail.lock();
        let Tail { readers, stray, .. } = &mut *tail;
        aof::replay_file(&inner.dir.join(snap_name(inner.instance)), |l| {
            apply_line(inner, l, &mut *stray)
        });
        aof::replay_file(&inner.dir.join(&inner.own_name), |l| {
            apply_line(inner, l, &mut *stray)
        });
        for f in aof::shard_files(&inner.dir, &inner.own_name) {
            let snap = f.trim_end_matches(".log").to_string() + ".snap";
            aof::replay_file(&inner.dir.join(snap), |l| apply_line(inner, l, &mut *stray));
            readers.insert(f.clone(), TailReader::from_start(inner.dir.join(&f)));
        }
    }

    /// Pull newly appended lines from sibling logs and discover new shards.
    pub fn poll_tails(&self) {
        if self.inner.aof.is_none() {
            return;
        }
        let mut tail = self.inner.tail.lock();
        poll_locked(&self.inner, &mut tail);
    }

    /// Tail the shard that owns code's prefix (generated codes only), then a
    /// rate-limited full poll for aliases/strays. Returns when the index may
    /// have been updated.
    fn tail_missed(&self, code: &str) {
        let inner = &self.inner;
        if code.is_empty() || inner.aof.is_none() {
            return;
        }
        let mut tail = inner.tail.lock();
        let owner = alpha_idx(code.as_bytes()[0]);
        if owner >= 0 && owner != inner.instance {
            let name = format!("data-{owner}.log");
            let Tail { readers, stray, .. } = &mut *tail;
            let t = readers
                .entry(name.clone())
                .or_insert_with(|| TailReader::from_start(inner.dir.join(&name)));
            t.read_new(|l| apply_line(inner, l, stray));
            tail.last_poll = now_ms();
            return;
        }
        // alias or unknown-prefix code: rate-limited full poll
        if now_ms() - tail.last_poll >= TAIL_MIN_INTERVAL {
            poll_locked(inner, &mut tail);
        }
    }

    fn rand_suffix() -> [u8; CODE_LEN - 1] {
        let mut buf = [0u8; CODE_LEN - 1];
        rand::rng().fill(&mut buf);
        let mut sb = [0u8; CODE_LEN - 1];
        for k in 0..CODE_LEN - 1 {
            sb[k] = ALPHABET[(buf[k] as usize) % MAX_INSTANCES];
        }
        sb
    }

    fn gen_code(&self) -> Box<str> {
        let mut c = String::with_capacity(CODE_LEN);
        c.push(self.inner.prefix as char);
        for b in Self::rand_suffix() {
            c.push(b as char);
        }
        c.into_boxed_str()
    }

    fn new_entry(&self, url: &str, now: i64, exp: i64) -> Entry {
        Entry {
            u: Arc::from(url),
            a: now,
            e: exp,
            i: self.inner.instance,
            h: AtomicI64::new(0),
            oh: AtomicI64::new(0),
        }
    }

    /// Create a link; returns the code, or None if the alias is taken.
    pub fn shorten(&self, url: &str, alias: Option<&str>, ttl_ms: i64) -> Option<Box<str>> {
        let inner = &self.inner;
        let _g = inner.gate.read();
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        let code: Box<str> = match alias {
            Some(a) => {
                let code: Box<str> = a.into();
                let sh = &inner.shards[shard_of(&code)];
                let mut g = sh.data.write();
                if g.contains_key(&*code) {
                    return None;
                }
                g.insert(code.clone(), self.new_entry(url, now, exp));
                code
            }
            None => loop {
                let cb = self.gen_code();
                let sh = &inner.shards[shard_of(&cb)];
                let mut g = sh.data.write();
                if g.contains_key(&*cb) {
                    continue;
                }
                g.insert(cb.clone(), self.new_entry(url, now, exp));
                break cb;
            },
        };
        if let Some(a) = &inner.aof {
            let mut a = a.lock();
            a.push(&row_line(&code, url, now, exp, inner.instance as i64, 0));
            if a.pending_bytes() > FLUSH_BYTES {
                a.flush();
            }
        }
        Some(code)
    }

    /// Bulk create; returns codes aligned with input order.
    pub fn shorten_many(&self, urls: &[String], ttl_ms: i64) -> Vec<Box<str>> {
        let inner = &self.inner;
        let _g = inner.gate.read();
        let now = now_ms();
        let exp = if ttl_ms > 0 { now + ttl_ms } else { 0 };
        let mut codes: Vec<Box<str>> = Vec::with_capacity(urls.len());
        let mut rnd = vec![0u8; urls.len() * (CODE_LEN - 1)]; // one RNG call
        rand::rng().fill(&mut rnd[..]);
        // all rows encoded into one scratch buffer -> single log write below
        let mut lines: Vec<u8> = Vec::with_capacity(urls.len() * 48);
        for (i, u) in urls.iter().enumerate() {
            let mut c = String::with_capacity(CODE_LEN);
            c.push(inner.prefix as char);
            for k in 0..CODE_LEN - 1 {
                c.push(ALPHABET[(rnd[i * (CODE_LEN - 1) + k] as usize) % MAX_INSTANCES] as char);
            }
            let mut cb: Box<str> = c.into_boxed_str();
            loop {
                let sh = &inner.shards[shard_of(&cb)];
                let mut g = sh.data.write();
                if !g.contains_key(&*cb) {
                    g.insert(cb.clone(), self.new_entry(u, now, exp));
                    break;
                }
                drop(g);
                cb = self.gen_code();
            }
            if inner.aof.is_some() {
                row_line_into(&mut lines, &cb, u, now, exp, inner.instance as i64, 0);
                lines.push(b'\n');
            }
            codes.push(cb);
        }
        if let Some(a) = &inner.aof {
            let mut a = a.lock();
            a.push_raw(&lines);
            if a.pending_bytes() > FLUSH_BYTES {
                a.flush();
            }
        }
        codes
    }

    /// Returns the target url, or None for miss/expired. Counts a hit on success.
    pub fn resolve(&self, code: &str) -> Option<Arc<str>> {
        let inner = &self.inner;
        let sh = &inner.shards[shard_of(code)];
        let guard = sh.data.read();
        let mut entry_ok = false;
        if let Some(e) = guard.get(code) {
            if e.e == 0 || e.e > now_ms() {
                if track_hits() {
                    e.h.fetch_add(1, Ordering::Relaxed);
                    e.oh.fetch_add(1, Ordering::Relaxed);
                }
                entry_ok = true;
            } else {
                return None; // expired
            }
        }
        if entry_ok {
            let url = guard.get(code).map(|e| e.u.clone());
            drop(guard);
            if track_hits() {
                let mut d = sh.dirty.lock();
                match d.get_mut(code) {
                    Some(v) => *v += 1,
                    None => {
                        d.insert(code.into(), 1);
                    }
                }
            }
            return url;
        }
        drop(guard);
        inner.aof.as_ref()?;
        // maybe a sibling wrote it and we haven't tailed yet
        self.tail_missed(code);
        let g = sh.data.read();
        match g.get(code) {
            Some(e) if e.e == 0 || e.e > now_ms() => {
                if track_hits() {
                    e.h.fetch_add(1, Ordering::Relaxed);
                    e.oh.fetch_add(1, Ordering::Relaxed);
                }
                let url = e.u.clone();
                drop(g);
                if track_hits() {
                    let mut d = sh.dirty.lock();
                    match d.get_mut(code) {
                        Some(v) => *v += 1,
                        None => {
                            d.insert(code.into(), 1);
                        }
                    }
                }
                Some(url)
            }
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.shards.iter().all(|sh| sh.data.read().is_empty())
    }

    /// Update url/ttl in place. Durable only on the owning instance.
    /// has_ttl=false keeps the existing expiry; otherwise ttl_ms>0 sets
    /// now+ttl_ms and ttl_ms<=0 clears expiry.
    pub fn update(&self, code: &str, url: &str, ttl_ms: i64, has_ttl: bool) -> MutResult {
        let inner = &self.inner;
        let _g = inner.gate.read();
        let sh = &inner.shards[shard_of(code)];
        let (a, exp) = {
            let mut g = sh.data.write();
            let Some(e) = g.get_mut(code) else {
                return MutResult::Missing;
            };
            if e.i != inner.instance {
                return MutResult::Remote;
            }
            let exp = if has_ttl {
                if ttl_ms > 0 {
                    now_ms() + ttl_ms
                } else {
                    0
                }
            } else {
                e.e
            };
            e.u = Arc::from(url);
            e.e = exp;
            (e.a, exp)
        };
        if let Some(aof) = &inner.aof {
            aof.lock()
                .push(&row_line(code, url, a, exp, inner.instance as i64, 0));
        }
        MutResult::Ok
    }

    /// Delete a link. Same owner rule as update().
    pub fn remove(&self, code: &str) -> MutResult {
        let inner = &self.inner;
        let _g = inner.gate.read();
        let sh = &inner.shards[shard_of(code)];
        {
            let mut g = sh.data.write();
            let Some(e) = g.get(code) else {
                return MutResult::Missing;
            };
            if e.i != inner.instance {
                return MutResult::Remote;
            }
            g.remove(code);
        }
        if let Some(aof) = &inner.aof {
            aof.lock().push(&del_line(code));
        }
        MutResult::Ok
    }

    /// O(n) scan for UI listing — admin path, not the hot path.
    pub fn list(&self, limit: usize, offset: usize, sort: &str, q: &str) -> (Vec<Link>, usize) {
        let mut items: Vec<Link> = Vec::new();
        for sh in &self.inner.shards {
            let g = sh.data.read();
            for (code, e) in g.iter() {
                if !q.is_empty() && !code.contains(q) && !e.u.contains(q) {
                    continue;
                }
                items.push(Link {
                    code: code.to_string(),
                    url: e.u.to_string(),
                    hits: e.h.load(Ordering::Relaxed),
                    created_at: e.a,
                    expires_at: if e.e != 0 { Some(e.e) } else { None },
                });
            }
        }
        if sort == "hits" {
            items.sort_by_key(|x| std::cmp::Reverse(x.hits));
        } else {
            items.sort_by_key(|x| std::cmp::Reverse(x.created_at));
        }
        let total = items.len();
        let out: Vec<Link> = items
            .into_iter()
            .skip(offset.min(total))
            .take(limit)
            .collect();
        (out, total)
    }

    pub fn stats(&self, code: &str) -> Option<Link> {
        let sh = &self.inner.shards[shard_of(code)];
        let g = sh.data.read();
        g.get(code).map(|e| Link {
            code: code.to_string(),
            url: e.u.to_string(),
            hits: e.h.load(Ordering::Relaxed),
            created_at: e.a,
            expires_at: if e.e != 0 { Some(e.e) } else { None },
        })
    }

    /// Bulk-insert urls through the normal write path. Returns count.
    pub fn seed(&self, urls: &[String]) -> usize {
        self.shorten_many(urls, 0);
        self.flush();
        urls.len()
    }

    /// Persist hit deltas + all queued rows; one write + fsync boundary.
    pub fn flush(&self) {
        self.inner.flush_public();
    }

    /// Rewrite own rows as a compact snapshot, then truncate own log.
    pub fn compact(&self) {
        let inner = &self.inner;
        if inner.aof.is_none() {
            return;
        }
        let _g = inner.gate.write();
        inner.flush_locked();
        let snap_path = inner.dir.join(snap_name(inner.instance));
        let tmp = snap_path.with_extension("snap.tmp");
        if let Ok(mut f) = std::fs::File::create(&tmp) {
            use std::io::Write;
            for sh in &inner.shards {
                let g = sh.data.read();
                for (code, e) in g.iter() {
                    if e.i == inner.instance {
                        let _ = f.write_all(&row_line(
                            code,
                            &e.u,
                            e.a,
                            e.e,
                            e.i as i64,
                            e.oh.load(Ordering::Relaxed),
                        ));
                        let _ = f.write_all(b"\n");
                    }
                }
            }
            let _ = f.sync_all();
            let _ = std::fs::rename(&tmp, &snap_path);
        }
        if let Some(a) = &inner.aof {
            a.lock().truncate();
        }
    }

    /// Stop timers, flush, fsync, and release the instance lock.
    pub fn close(&self) {
        let inner = &self.inner;
        if inner.closed.swap(true, Ordering::Relaxed) {
            return;
        }
        inner.stop.store(true, Ordering::Relaxed);
        let mut threads = inner.threads.lock();
        while let Some(h) = threads.pop() {
            let _ = h.join();
        }
        drop(threads);
        inner.flush_public();
        if let Some(a) = &inner.aof {
            a.lock().close();
        }
        if let Some(l) = &inner.lock_path {
            let _ = std::fs::remove_file(l);
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.close();
    }
}

impl Inner {
    fn flush_public(&self) {
        let _g = self.gate.read();
        self.flush_locked();
    }

    /// Flush for callers already holding the gate.
    fn flush_locked(&self) {
        let Some(a) = &self.aof else { return };
        let mut lines: Vec<u8> = Vec::with_capacity(256);
        for sh in &self.shards {
            let mut dirty = sh.dirty.lock();
            if dirty.is_empty() {
                continue;
            }
            for (code, d) in dirty.iter() {
                hit_line_into(&mut lines, code, *d, self.instance as i64);
                lines.push(b'\n');
            }
            dirty.clear();
        }
        let mut aof = a.lock();
        aof.push_raw(&lines);
        aof.flush();
    }

    fn poll_tails_pub(&self) {
        if self.aof.is_none() {
            return;
        }
        let mut tail = self.tail.lock();
        poll_locked(self, &mut tail);
    }
}
