//! Minimal RESP (Redis/Dragonfly wire protocol) client — zero extra deps.
//! Small blocking connection pool; pipelined writes for batched commands.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;

pub enum Resp {
    Simple(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Arr(Vec<Resp>),
    Err(String),
}

pub struct Kv {
    addr: String,
    pool: Mutex<Vec<TcpStream>>,
}

struct Conn {
    s: TcpStream,
    r: BufReader<TcpStream>,
}

impl Kv {
    pub fn connect(addr: &str) -> std::io::Result<Kv> {
        let s = TcpStream::connect(addr)?;
        let kv = Kv {
            addr: addr.to_string(),
            pool: Mutex::new(vec![s]),
        };
        Ok(kv)
    }

    fn take(&self) -> std::io::Result<Conn> {
        if let Some(s) = self.pool.lock().unwrap().pop() {
            let r = BufReader::with_capacity(64 << 10, s.try_clone()?);
            return Ok(Conn { s, r });
        }
        let s = TcpStream::connect(&self.addr)?;
        s.set_nodelay(true)?;
        let r = BufReader::with_capacity(64 << 10, s.try_clone()?);
        Ok(Conn { s, r })
    }

    fn give(&self, c: Conn) {
        self.pool.lock().unwrap().push(c.s);
    }

    /// Run one command; returns the parsed reply. On I/O error the
    /// connection is dropped instead of returned to the pool.
    pub fn cmd(&self, args: &[&[u8]]) -> std::io::Result<Resp> {
        let mut c = self.take()?;
        match round(&mut c, args) {
            Ok(r) => {
                self.give(c);
                Ok(r)
            }
            Err(e) => Err(e), // drop conn
        }
    }

    /// Run a batch of commands over one connection (pipelined).
    pub fn pipe(&self, cmds: &[Vec<Vec<u8>>]) -> std::io::Result<Vec<Resp>> {
        let mut c = self.take()?;
        let res = (|| {
            let mut out = Vec::with_capacity(cmds.iter().map(|c| c.len() + 4).sum::<usize>() + 16);
            for a in cmds {
                write_cmd(&mut out, a);
            }
            c.s.write_all(&out)?;
            let mut rs = Vec::with_capacity(cmds.len());
            for _ in cmds {
                rs.push(read_resp(&mut c.r)?);
            }
            Ok(rs)
        })();
        match res {
            Ok(rs) => {
                self.give(c);
                Ok(rs)
            }
            Err(e) => Err(e),
        }
    }

    // ---- typed helpers ----

    pub fn get(&self, k: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        match self.cmd(&[b"GET", k])? {
            Resp::Bulk(b) => Ok(b),
            _ => Ok(None),
        }
    }

    /// SET k v [PX ms] [NX] — returns true if the write was applied.
    pub fn set(&self, k: &[u8], v: &[u8], px_ms: i64, nx: bool) -> std::io::Result<bool> {
        let mut args: Vec<&[u8]> = vec![b"SET", k, v];
        let px = px_ms.to_string();
        if px_ms > 0 {
            args.push(b"PX");
            args.push(px.as_bytes());
        }
        if nx {
            args.push(b"NX");
        }
        match self.cmd(&args)? {
            Resp::Simple(s) => Ok(s == "OK"),
            Resp::Bulk(None) => Ok(false), // NX not satisfied
            Resp::Err(e) => Err(std::io::Error::other(e)),
            _ => Ok(false),
        }
    }

    pub fn del(&self, k: &[u8]) -> std::io::Result<i64> {
        match self.cmd(&[b"DEL", k])? {
            Resp::Int(n) => Ok(n),
            _ => Ok(0),
        }
    }

    /// Batched INCRBY — one pipeline round-trip for all deltas.
    pub fn incrby_many(&self, deltas: &[(String, i64)]) -> std::io::Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let cmds: Vec<Vec<Vec<u8>>> = deltas
            .iter()
            .map(|(k, d)| {
                vec![
                    b"INCRBY".to_vec(),
                    k.as_bytes().to_vec(),
                    d.to_string().into_bytes(),
                ]
            })
            .collect();
        self.pipe(&cmds)?;
        Ok(())
    }

    /// HGET — hash field read.
    pub fn hget(&self, k: &[u8], f: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        match self.cmd(&[b"HGET", k, f])? {
            Resp::Bulk(b) => Ok(b),
            _ => Ok(None),
        }
    }

    /// HSET — returns true if the field was newly created.
    pub fn hset(&self, k: &[u8], f: &[u8], v: &[u8]) -> std::io::Result<bool> {
        match self.cmd(&[b"HSET", k, f, v])? {
            Resp::Int(_) => Ok(true),
            _ => Ok(false),
        }
    }

    /// HSETNX — true only if the field did not exist.
    pub fn hsetnx(&self, k: &[u8], f: &[u8], v: &[u8]) -> std::io::Result<bool> {
        match self.cmd(&[b"HSETNX", k, f, v])? {
            Resp::Int(n) => Ok(n == 1),
            _ => Ok(false),
        }
    }

    /// HDEL — fields removed.
    pub fn hdel(&self, k: &[u8], f: &[u8]) -> std::io::Result<i64> {
        match self.cmd(&[b"HDEL", k, f])? {
            Resp::Int(n) => Ok(n),
            _ => Ok(0),
        }
    }

    /// Batched HINCRBY — one pipeline round-trip for all deltas.
    /// deltas: (hash_key, field, delta)
    pub fn hincrby_many(&self, deltas: &[(Vec<u8>, Vec<u8>, i64)]) -> std::io::Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let cmds: Vec<Vec<Vec<u8>>> = deltas
            .iter()
            .map(|(k, f, d)| {
                vec![
                    b"HINCRBY".to_vec(),
                    k.clone(),
                    f.clone(),
                    d.to_string().into_bytes(),
                ]
            })
            .collect();
        self.pipe(&cmds)?;
        Ok(())
    }

    /// HSCAN all fields of a hash; cb(field, value).
    pub fn hscan_each(&self, key: &[u8], mut cb: impl FnMut(Vec<u8>, Vec<u8>)) -> std::io::Result<()> {
        let mut cursor = b"0".to_vec();
        loop {
            match self.cmd(&[b"HSCAN", key, &cursor, b"COUNT", b"1000"])? {
                Resp::Arr(a) if a.len() == 2 => {
                    cursor = match &a[0] {
                        Resp::Bulk(Some(b)) => b.clone(),
                        Resp::Simple(s) => s.as_bytes().to_vec(),
                        _ => break,
                    };
                    if let Resp::Arr(items) = &a[1] {
                        for pair in items.as_chunks::<2>().0 {
                            if let (Resp::Bulk(Some(f)), Resp::Bulk(Some(v))) =
                                (&pair[0], &pair[1])
                            {
                                cb(f.clone(), v.clone());
                            }
                        }
                    }
                    if cursor == b"0" {
                        return Ok(());
                    }
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// SCAN all keys matching `pat`, invoking cb per key. Admin path.
    pub fn scan_each(&self, pat: &str, mut cb: impl FnMut(Vec<u8>)) -> std::io::Result<()> {
        let mut cursor = b"0".to_vec();
        loop {
            match self.cmd(&[b"SCAN", &cursor, b"MATCH", pat.as_bytes(), b"COUNT", b"500"])? {
                Resp::Arr(a) if a.len() == 2 => {
                    cursor = match &a[0] {
                        Resp::Bulk(Some(b)) => b.clone(),
                        Resp::Simple(s) => s.as_bytes().to_vec(),
                        _ => break,
                    };
                    if let Resp::Arr(keys) = &a[1] {
                        for k in keys {
                            if let Resp::Bulk(Some(b)) = k {
                                cb(b.clone());
                            }
                        }
                    }
                    if cursor == b"0" {
                        return Ok(());
                    }
                }
                _ => break,
            }
        }
        Ok(())
    }

    pub fn select_db(&self, n: i64) -> std::io::Result<()> {
        let d = n.to_string();
        self.cmd(&[b"SELECT", d.as_bytes()]).map(|_| ())
    }

    pub fn flushdb(&self) -> std::io::Result<()> {
        self.cmd(&[b"FLUSHDB"]).map(|_| ())
    }
}

fn write_cmd(out: &mut Vec<u8>, args: &[Vec<u8>]) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        out.extend_from_slice(a.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

fn round(c: &mut Conn, args: &[&[u8]]) -> std::io::Result<Resp> {
    let mut out = Vec::with_capacity(64 + args.len() * 8);
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        out.extend_from_slice(a.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    c.s.write_all(&out)?;
    read_resp(&mut c.r)
}

fn read_line(r: &mut BufReader<TcpStream>) -> std::io::Result<String> {
    let mut s = String::new();
    r.read_line(&mut s)?;
    Ok(s.trim_end().to_string())
}

fn read_exact(r: &mut BufReader<TcpStream>, n: usize) -> std::io::Result<Vec<u8>> {
    let mut b = vec![0u8; n + 2]; // value + CRLF
    r.read_exact(&mut b)?;
    b.truncate(n);
    Ok(b)
}

fn read_resp(r: &mut BufReader<TcpStream>) -> std::io::Result<Resp> {
    let mut t = [0u8; 1];
    r.read_exact(&mut t)?;
    match t[0] {
        b'+' => Ok(Resp::Simple(read_line(r)?)),
        b'-' => Ok(Resp::Err(read_line(r)?)),
        b':' => Ok(Resp::Int(read_line(r)?.parse().unwrap_or(0))),
        b'$' => {
            let n: i64 = read_line(r)?.parse().unwrap_or(-1);
            if n < 0 {
                return Ok(Resp::Bulk(None));
            }
            Ok(Resp::Bulk(Some(read_exact(r, n as usize)?)))
        }
        b'*' => {
            let n: i64 = read_line(r)?.parse().unwrap_or(0);
            let mut items = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n {
                items.push(read_resp(r)?);
            }
            Ok(Resp::Arr(items))
        }
        _ => Err(std::io::Error::other("bad resp type")),
    }
}
