//! Transport-agnostic request handler shared by the mini and hyper frontends.

use std::sync::{Arc, OnceLock};

use serde::Deserialize;

use crate::metrics;
use crate::store::{MutResult, Store};

pub const MAX_BODY: usize = 4096;
pub const MAX_BULK_BODY: usize = 1 << 20;
pub const MAX_BULK_URLS: usize = 10_000;
pub const MAX_LIST_LIMIT: i64 = 1000;

/// LINK_TTL_MS — default AND max link lifetime (env LINK_TTL_MS, 1 day).
pub fn link_ttl_ms() -> i64 {
    static V: OnceLock<i64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("LINK_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86_400_000)
    })
}

/// CORS_ORIGIN — Access-Control-Allow-Origin value (env CORS_ORIGIN).
pub fn cors_origin() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    V.get_or_init(|| std::env::var("CORS_ORIGIN").unwrap_or_else(|_| "*".into()))
}

/// Links are immutable for the public API. PATCH/DELETE exist only when
/// ADMIN_TOKEN is set, and require the x-admin-token header.
fn admin_ok(token: &str) -> bool {
    match std::env::var("ADMIN_TOKEN") {
        Ok(t) => !t.is_empty() && t == token,
        Err(_) => false,
    }
}

/// Transport-agnostic response.
pub struct Reply {
    pub status: u16,
    pub location: Option<Arc<str>>,
    pub body: Vec<u8>,
    pub ctype: Option<&'static str>,
}

impl Reply {
    fn new(status: u16, body: impl Into<Vec<u8>>) -> Reply {
        Reply {
            status,
            location: None,
            body: body.into(),
            ctype: None,
        }
    }
}

fn bad(err: &str) -> Reply {
    Reply::new(400, format!("{{\"error\":\"{err}\"}}"))
}

fn not_found() -> Reply {
    Reply::new(404, "{\"error\":\"not found\"}")
}

// ---------- UI ----------

/// ui/index.html loaded once; None when absent.
pub fn ui_html() -> Option<&'static [u8]> {
    static V: OnceLock<Option<Vec<u8>>> = OnceLock::new();
    V.get_or_init(|| std::fs::read("ui/index.html").ok())
        .as_deref()
}

// ---------- validation ----------

fn code_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

fn is_valid_url(raw: &str) -> bool {
    if raw.is_empty() || raw.len() > 2048 {
        return false;
    }
    match url::Url::parse(raw) {
        Ok(u) => {
            (u.scheme().eq_ignore_ascii_case("http") || u.scheme().eq_ignore_ascii_case("https"))
                && u.host_str().map(|h| !h.is_empty()).unwrap_or(false)
        }
        Err(_) => false,
    }
}

// ---------- request bodies ----------

#[derive(Deserialize)]
struct ShortenReq {
    url: Option<String>,
    alias: Option<String>,
    ttl_ms: Option<f64>,
}

fn shorten_one(st: &Store, p: &ShortenReq) -> Reply {
    let Some(u) = &p.url else {
        return bad("invalid url");
    };
    if !is_valid_url(u) {
        return bad("invalid url");
    }
    if let Some(a) = &p.alias {
        if !code_ok(a) {
            return bad("invalid alias");
        }
    }
    let mut ttl = link_ttl_ms();
    if let Some(t) = p.ttl_ms {
        if t <= 0.0 {
            return bad("invalid ttl_ms");
        }
        ttl = (t as i64).min(link_ttl_ms());
    }
    match st.shorten(u, p.alias.as_deref(), ttl) {
        None => Reply::new(409, "{\"error\":\"alias taken\"}"),
        Some(code) => {
            let mut b = Vec::with_capacity(code.len() + 32);
            b.extend_from_slice(b"{\"code\":\"");
            b.extend_from_slice(code.as_bytes());
            b.extend_from_slice(b"\",\"short_url\":\"/");
            b.extend_from_slice(code.as_bytes());
            b.extend_from_slice(b"\"}");
            Reply::new(201, b)
        }
    }
}

/// bulk fast-path: prefix + length + no chars that could break the log line
fn ok_bulk_url(u: &str) -> bool {
    u.len() > 7
        && u.len() <= 2048
        && (u.starts_with("http://") || u.starts_with("https://"))
        && !u
            .bytes()
            .any(|c| c == b'"' || c == b'\\' || c == b'\n' || c == b'\r')
}

#[derive(Deserialize)]
struct BulkReq {
    urls: Option<Vec<String>>,
}

fn shorten_bulk(st: &Store, p: &BulkReq) -> Reply {
    let Some(urls) = &p.urls else {
        return bad(&format!(
            "urls must be 1-{MAX_BULK_URLS} valid http(s) urls"
        ));
    };
    if urls.is_empty() || urls.len() > MAX_BULK_URLS || !urls.iter().all(|u| ok_bulk_url(u)) {
        return bad(&format!(
            "urls must be 1-{MAX_BULK_URLS} valid http(s) urls"
        ));
    }
    let codes = st.shorten_many(urls, link_ttl_ms());
    let mut b = Vec::with_capacity(codes.len() * 10 + 24);
    b.extend_from_slice(b"{\"count\":");
    b.extend_from_slice(codes.len().to_string().as_bytes());
    b.extend_from_slice(b",\"codes\":[");
    for (i, c) in codes.iter().enumerate() {
        if i > 0 {
            b.push(b',');
        }
        b.push(b'"');
        b.extend_from_slice(c.as_bytes());
        b.push(b'"');
    }
    b.extend_from_slice(b"]}");
    Reply::new(201, b)
}

#[derive(Deserialize)]
struct PatchReq {
    url: Option<String>,
    ttl_ms: Option<f64>,
}

// ---------- handler ----------

/// The transport-agnostic request handler. `path` includes the query string
/// ("...?..."); `body` is the raw request body (empty when absent).
pub fn handle(st: &Store, method: &str, path: &str, body: &[u8], admin_token: &str) -> Reply {
    let (pathname, query) = match path.find('?') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (path, ""),
    };

    metrics::tick();

    if method == "OPTIONS" {
        return Reply {
            status: 204,
            location: None,
            body: Vec::new(),
            ctype: None,
        };
    }

    match method {
        "GET" => {
            if pathname == "/api/health" {
                return Reply::new(200, "{\"ok\":true}");
            }
            if pathname == "/api/metrics" {
                return Reply::new(200, metrics::snapshot());
            }
            if pathname == "/" {
                return match ui_html() {
                    Some(html) => Reply {
                        status: 200,
                        location: None,
                        body: html.to_vec(),
                        ctype: Some("text/html; charset=utf-8"),
                    },
                    None => not_found(),
                };
            }
            if pathname == "/api/links" {
                let p = url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect::<std::collections::HashMap<String, String>>();
                let mut limit: i64 = 50;
                if let Some(v) = p.get("limit") {
                    if let Ok(n) = v.parse::<i64>() {
                        if n != 0 {
                            limit = n;
                        }
                    }
                }
                limit = limit.clamp(1, MAX_LIST_LIMIT);
                let mut offset: i64 = 0;
                if let Some(v) = p.get("offset") {
                    if let Ok(n) = v.parse::<i64>() {
                        if n > 0 {
                            offset = n;
                        }
                    }
                }
                let sort = if p.get("sort").map(String::as_str) == Some("hits") {
                    "hits"
                } else {
                    "created"
                };
                let q = p.get("q").cloned().unwrap_or_default();
                let (links, total) = st.list(limit as usize, offset as usize, sort, &q);
                #[derive(serde::Serialize)]
                struct ListResp<'a> {
                    links: &'a [crate::store::Link],
                    total: usize,
                }
                return Reply::new(
                    200,
                    sonic_rs::to_vec(&ListResp {
                        links: &links,
                        total,
                    })
                    .unwrap_or_default(),
                );
            }
            if let Some(code) = pathname.strip_prefix("/api/stats/") {
                return match st.stats(code) {
                    Some(link) => Reply::new(200, sonic_rs::to_vec(&link).unwrap_or_default()),
                    None => not_found(),
                };
            }
            let code = &pathname[1..];
            if code_ok(code) {
                if let Some(target) = st.resolve(code) {
                    return Reply {
                        status: 302,
                        location: Some(target),
                        body: Vec::new(),
                        ctype: None,
                    };
                }
            }
            not_found()
        }
        "POST" => {
            if pathname != "/api/shorten" && pathname != "/api/shorten/bulk" {
                return not_found();
            }
            if pathname == "/api/shorten" {
                return match sonic_rs::from_slice::<ShortenReq>(body) {
                    Ok(p) => shorten_one(st, &p),
                    Err(_) => bad("invalid json"),
                };
            }
            match sonic_rs::from_slice::<BulkReq>(body) {
                Ok(p) => shorten_bulk(st, &p),
                Err(_) => bad("invalid json"),
            }
        }
        "PATCH" | "DELETE" => {
            if !pathname.starts_with("/api/links/") || !admin_ok(admin_token) {
                return not_found();
            }
            let code = &pathname["/api/links/".len()..];
            if !code_ok(code) {
                return bad("invalid code");
            }
            if method == "DELETE" {
                return match st.remove(code) {
                    MutResult::Ok => Reply {
                        status: 204,
                        location: None,
                        body: Vec::new(),
                        ctype: None,
                    },
                    MutResult::Missing => not_found(),
                    MutResult::Remote => {
                        Reply::new(409, "{\"error\":\"owned by another instance\"}")
                    }
                };
            }
            let p = match sonic_rs::from_slice::<PatchReq>(body) {
                Ok(p) => p,
                Err(_) => return bad("invalid json"),
            };
            if let Some(u) = &p.url {
                if !is_valid_url(u) {
                    return bad("invalid url");
                }
                let (ttl, has_ttl) = match p.ttl_ms {
                    Some(t) => ((t as i64).min(link_ttl_ms()), true),
                    None => (0, false),
                };
                return match st.update(code, u, ttl, has_ttl) {
                    MutResult::Ok => Reply::new(200, "{\"ok\":true}"),
                    MutResult::Missing => not_found(),
                    MutResult::Remote => {
                        Reply::new(409, "{\"error\":\"owned by another instance\"}")
                    }
                };
            }
            bad("nothing to update")
        }
        _ => not_found(),
    }
}
