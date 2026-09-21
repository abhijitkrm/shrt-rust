//! Log line codec: serialization and parsing for the AOF row format.
//! Formats (byte-compatible with shrt-ts / shrt-go):
//!   row:       {"c":"<code>","u":"<esc url>","a":<ms>,"e":<ms|null>,"i":<inst>,"n":<hits>}
//!   hit delta: {"h":"<code>","d":<n>,"i":<inst>}
//!   tombstone: {"x":"<code>"}

/// Append u JSON-string-escaped (like JSON.stringify without quotes).
pub fn esc(dst: &mut Vec<u8>, u: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &c in u.as_bytes() {
        match c {
            b'"' | b'\\' => dst.extend_from_slice(&[b'\\', c]),
            b'\n' => dst.extend_from_slice(b"\\n"),
            b'\r' => dst.extend_from_slice(b"\\r"),
            b'\t' => dst.extend_from_slice(b"\\t"),
            0x08 => dst.extend_from_slice(b"\\b"),
            0x0c => dst.extend_from_slice(b"\\f"),
            _ if c < 0x20 => dst.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(c >> 4) as usize],
                HEX[(c & 0xf) as usize],
            ]),
            _ => dst.push(c),
        }
    }
}

/// Push n's decimal digits without allocating.
#[inline]
fn itoa(b: &mut Vec<u8>, n: i64) {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut v = n.unsigned_abs();
    loop {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if n < 0 {
        i -= 1;
        tmp[i] = b'-';
    }
    b.extend_from_slice(&tmp[i..]);
}

/// Serialize a link row directly into dst (hot path: no allocation).
pub fn row_line_into(b: &mut Vec<u8>, c: &str, u: &str, a: i64, e: i64, i: i64, n: i64) {
    b.extend_from_slice(b"{\"c\":\"");
    b.extend_from_slice(c.as_bytes());
    b.extend_from_slice(b"\",\"u\":\"");
    esc(b, u);
    b.extend_from_slice(b"\",\"a\":");
    itoa(b, a);
    b.extend_from_slice(b",\"e\":");
    if e == 0 {
        b.extend_from_slice(b"null");
    } else {
        itoa(b, e);
    }
    b.extend_from_slice(b",\"i\":");
    itoa(b, i);
    b.extend_from_slice(b",\"n\":");
    itoa(b, n);
    b.push(b'}');
}

pub fn row_line(c: &str, u: &str, a: i64, e: i64, i: i64, n: i64) -> Vec<u8> {
    let mut b = Vec::with_capacity(u.len() + c.len() + 48);
    row_line_into(&mut b, c, u, a, e, i, n);
    b
}

/// Serialize a hit delta directly into dst.
pub fn hit_line_into(b: &mut Vec<u8>, c: &str, d: i64, i: i64) {
    b.extend_from_slice(b"{\"h\":\"");
    b.extend_from_slice(c.as_bytes());
    b.extend_from_slice(b"\",\"d\":");
    itoa(b, d);
    b.extend_from_slice(b",\"i\":");
    itoa(b, i);
    b.push(b'}');
}

pub fn hit_line(c: &str, d: i64, i: i64) -> Vec<u8> {
    let mut b = Vec::with_capacity(c.len() + 24);
    hit_line_into(&mut b, c, d, i);
    b
}

pub fn del_line(c: &str) -> Vec<u8> {
    let mut b = Vec::with_capacity(c.len() + 8);
    b.extend_from_slice(b"{\"x\":\"");
    b.extend_from_slice(c.as_bytes());
    b.extend_from_slice(b"\"}");
    b
}

/// One parsed log line: row {"c","u","a","e","i","n"}, hit delta {"h","d","i"},
/// or tombstone {"x"}.
#[derive(Default)]
pub struct Op {
    pub c: String,
    pub u: String,
    pub h: String,
    pub x: String,
    pub a: i64,
    pub e: i64,
    pub i: i64,
    pub n: i64,
    pub d: i64,
    pub has_e: bool,
}

/// Decode one log line. Machine-generated input, but tolerates field
/// reordering and escapes in string values.
pub fn parse_op(b: &[u8], o: &mut Op) -> bool {
    *o = Op::default();
    let mut pos = 0usize;
    while pos < b.len() {
        while pos < b.len() && b[pos] != b'"' {
            pos += 1;
        }
        if pos >= b.len() {
            break;
        }
        pos += 1;
        let ks = pos;
        while pos < b.len() && b[pos] != b'"' {
            pos += 1;
        }
        if pos >= b.len() {
            return false;
        }
        let key = &b[ks..pos];
        pos += 1;
        while pos < b.len() && b[pos] != b':' {
            pos += 1;
        }
        if pos >= b.len() {
            return false;
        }
        pos += 1;
        while pos < b.len() && (b[pos] == b' ' || b[pos] == b'\t') {
            pos += 1;
        }
        if pos >= b.len() {
            return false;
        }
        if b[pos] == b'"' {
            pos += 1;
            let vs = pos;
            let mut escapes = false;
            while pos < b.len() {
                if b[pos] == b'\\' {
                    escapes = true;
                    pos += 2;
                    continue;
                }
                if b[pos] == b'"' {
                    break;
                }
                pos += 1;
            }
            let val = &b[vs..pos.min(b.len())];
            let s = if escapes {
                unescape(val)
            } else {
                String::from_utf8_lossy(val).into_owned()
            };
            pos += 1;
            match key.first() {
                Some(b'c') => o.c = s,
                Some(b'u') => o.u = s,
                Some(b'h') => o.h = s,
                Some(b'x') => o.x = s,
                _ => {}
            }
        } else {
            let vs = pos;
            while pos < b.len() && b[pos] != b',' && b[pos] != b'}' {
                pos += 1;
            }
            let num = std::str::from_utf8(&b[vs..pos]).unwrap_or("").trim();
            match key.first() {
                Some(b'a') => o.a = num.parse().unwrap_or(0),
                Some(b'e') => {
                    if num != "null" {
                        o.e = num.parse().unwrap_or(0);
                        o.has_e = true;
                    }
                }
                Some(b'i') => o.i = num.parse().unwrap_or(0),
                Some(b'n') => o.n = num.parse().unwrap_or(0),
                Some(b'd') => o.d = num.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    !o.c.is_empty() || !o.h.is_empty() || !o.x.is_empty()
}

fn unescape(b: &[u8]) -> String {
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' || i + 1 >= b.len() {
            out.push(b[i]);
            i += 1;
            continue;
        }
        i += 1;
        match b[i] {
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'u' => {
                if i + 4 < b.len() {
                    let ok = std::str::from_utf8(&b[i + 1..i + 5])
                        .ok()
                        .and_then(|h| u32::from_str_radix(h, 16).ok())
                        .and_then(char::from_u32);
                    if let Some(ch) = ok {
                        let mut tmp = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                        i += 4;
                        i += 1;
                        continue;
                    }
                }
                out.push(b'u');
            }
            c => out.push(c), // covers \" \\ \/
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
