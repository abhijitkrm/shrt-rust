//! Append-only-log persistence: buffered line writes, batched fsync, replay,
//! sibling-log tailing, and instance-id claiming via lock files.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const NL: u8 = b'\n';

/// Append-only log: queued line writes flushed in one syscall batch.
pub struct Aof {
    buf: Vec<u8>,
    f: File,
    pub path: PathBuf,
}

impl Aof {
    pub fn new(dir: &Path, name: &str) -> std::io::Result<Aof> {
        fs::create_dir_all(dir)?;
        let path = dir.join(name);
        let f = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Aof {
            buf: Vec::with_capacity(1 << 16),
            f,
            path,
        })
    }

    /// Queue one line (newline added) for the next flush.
    #[inline]
    pub fn push(&mut self, line: &[u8]) {
        self.buf.extend_from_slice(line);
        self.buf.push(NL);
    }

    /// Queue raw bytes verbatim for the next flush (caller supplies newlines).
    #[inline]
    pub fn push_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn pending_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Append all queued lines in one write (page cache only; sync() fsyncs).
    pub fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let _ = self.f.write_all(&self.buf);
        self.buf.clear();
    }

    /// fsync the log file.
    pub fn sync(&mut self) {
        self.flush();
        let _ = self.f.sync_all();
    }

    /// Discard log contents (after a snapshot was written).
    pub fn truncate(&mut self) {
        self.flush();
        if let Ok(f) = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)
        {
            self.f = f;
        }
    }

    pub fn close(&mut self) {
        self.flush();
        let _ = self.f.sync_all();
    }
}

/// Read `path` and invoke cb for each complete line (torn tail ignored).
pub fn replay_file(path: &Path, mut cb: impl FnMut(&[u8])) {
    let Ok(buf) = fs::read(path) else { return };
    let mut start = 0;
    for (i, &b) in buf.iter().enumerate() {
        if b == NL {
            if i > start {
                cb(&buf[start..i]);
            }
            start = i + 1;
        }
    }
}

/// Tracks a sibling log's read offset; read_new returns new complete lines.
pub struct TailReader {
    pub path: PathBuf,
    offset: u64,
    leftover: Vec<u8>,
}

impl TailReader {
    pub fn from_start(path: PathBuf) -> TailReader {
        TailReader {
            path,
            offset: 0,
            leftover: Vec::new(),
        }
    }

    /// Invoke cb for each complete line appended since the last call. If the
    /// file shrank (compacted/replaced), rescans from the start — row applies
    /// are idempotent and snapshot rows were already merged.
    pub fn read_new(&mut self, mut cb: impl FnMut(&[u8])) {
        let size = file_size(&self.path);
        if size < self.offset {
            self.offset = 0;
            self.leftover.clear();
        }
        if size <= self.offset {
            return;
        }
        let Ok(mut f) = File::open(&self.path) else {
            return;
        };
        let mut buf = vec![0u8; (size - self.offset) as usize];
        let _ = f.seek(SeekFrom::Start(self.offset));
        let n = read_full(&mut f, &mut buf);
        buf.truncate(n);
        self.offset += n as u64;

        if self.leftover.is_empty() {
            let mut start = 0usize;
            for i in 0..buf.len() {
                if buf[i] == NL {
                    if i > start {
                        cb(&buf[start..i]);
                    }
                    start = i + 1;
                }
            }
            self.leftover.extend_from_slice(&buf[start..]);
        } else {
            self.leftover.extend_from_slice(&buf);
            let mut start = 0usize;
            for i in 0..self.leftover.len() {
                if self.leftover[i] == NL {
                    if i > start {
                        cb(&self.leftover[start..i]);
                    }
                    start = i + 1;
                }
            }
            self.leftover.drain(..start);
        }
    }
}

fn read_full(f: &mut File, mut buf: &mut [u8]) -> usize {
    let mut total = 0;
    while !buf.is_empty() {
        match f.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                buf = &mut buf[n..];
            }
            Err(_) => break,
        }
    }
    total
}

/// Claim the lowest free instance index via lock files in dir; stale locks
/// (dead pid) are stolen. Returns (id, lock path) — caller removes the file
/// on shutdown.
pub fn claim_instance(dir: &Path) -> std::io::Result<(i32, PathBuf)> {
    fs::create_dir_all(dir)?;
    for i in 0..1024 {
        let lock = dir.join(format!("instance-{i}.lock"));
        if try_lock(&lock) {
            return Ok((i, lock));
        }
        // Lock exists: steal it if the holder is dead.
        let Ok(s) = fs::read_to_string(&lock) else {
            continue;
        };
        let Ok(pid) = s.trim().parse::<i32>() else {
            continue;
        };
        if pid <= 0 || pid_alive(pid) {
            continue;
        }
        let _ = fs::remove_file(&lock);
        if try_lock(&lock) {
            return Ok((i, lock));
        }
    }
    Err(std::io::Error::other("no free instance id"))
}

fn pid_alive(pid: i32) -> bool {
    unsafe {
        libc::kill(pid, 0) == 0
            || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

fn try_lock(lock: &Path) -> bool {
    match OpenOptions::new().write(true).create_new(true).open(lock) {
        Ok(mut f) => {
            let _ = f.write_all(std::process::id().to_string().as_bytes());
            true
        }
        Err(_) => false,
    }
}

/// List shard log names (data-*.log) present in dir, excluding `own`.
pub fn shard_files(dir: &Path, own: &str) -> Vec<String> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<String> = rd
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with("data-") && n.ends_with(".log") && n != own)
        .collect();
    out.sort();
    out
}

fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}
