//! API suite — runs against both HTTP frontends (mini + hyper).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;

use shrt::server_hyper;
use shrt::server_mini;
use shrt::store::Store;

// ADMIN_TOKEN is process-global; serialize tests that mutate it.
static ADMIN_LOCK: Mutex<()> = Mutex::new(());

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn header(&self, k: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(h, _)| h.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("json body")
    }
}

/// One request per fresh connection (Connection: close) — simple and exact.
fn req(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: Option<&[u8]>) -> Resp {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let mut r = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
    let b = body.unwrap_or(b"");
    for (k, v) in headers {
        r.push_str(&format!("{k}: {v}\r\n"));
    }
    if !b.is_empty() {
        r.push_str(&format!("content-length: {}\r\n", b.len()));
    }
    r.push_str("\r\n");
    s.write_all(r.as_bytes()).unwrap();
    s.write_all(b).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header end");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let body = raw[split + 4..].to_vec();
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    Resp {
        status,
        headers,
        body,
    }
}

fn get(port: u16, path: &str) -> Resp {
    req(port, "GET", path, &[], None)
}

fn post(port: u16, path: &str, body: &str) -> Resp {
    req(
        port,
        "POST",
        path,
        &[("content-type", "application/json")],
        Some(body.as_bytes()),
    )
}

fn shorten(port: u16, url: &str) -> String {
    let r = post(port, "/api/shorten", &format!("{{\"url\":\"{url}\"}}"));
    assert_eq!(r.status, 201, "shorten {url} -> {}", r.status);
    r.json()["code"].as_str().unwrap().to_string()
}

fn shorten_body(port: u16, body: &str) -> String {
    let r = post(port, "/api/shorten", body);
    assert_eq!(r.status, 201);
    r.json()["code"].as_str().unwrap().to_string()
}

/// Boot the handler on each frontend and run fn against it.
fn run_suite(f: impl Fn(u16)) {
    for srv in ["mini", "hyper"] {
        let st = Store::new(":memory:", -1).unwrap();
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = ln.local_addr().unwrap().port();
        match srv {
            "mini" => {
                std::thread::spawn(move || {
                    let _ = server_mini::serve(ln, st);
                });
            }
            _ => {
                std::thread::spawn(move || {
                    let _ = server_hyper::serve(ln, st);
                });
            }
        }
        // wait for accept loop
        for _ in 0..100 {
            if get(port, "/api/health").status == 200 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        f(port);
    }
}

#[test]
fn health() {
    run_suite(|port| {
        let r = get(port, "/api/health");
        assert_eq!(r.status, 200);
        assert_eq!(r.json()["ok"], true);
    });
}

#[test]
fn metrics() {
    run_suite(|port| {
        let r = get(port, "/api/metrics");
        assert_eq!(r.status, 200);
        let m = r.json();
        assert!(m["req_s"].is_f64() || m["req_s"].is_u64() || m["req_s"].is_i64());
        assert!(m["total"].as_i64().unwrap_or(0) >= 1, "total not counting");
        assert_eq!(m["per_second"].as_array().unwrap().len(), 31);
    });
}

#[test]
fn prometheus_metrics() {
    run_suite(|port| {
        shorten(port, "https://prom.example");
        let r = get(port, "/metrics");
        assert_eq!(r.status, 200);
        assert_eq!(
            r.header("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = String::from_utf8_lossy(&r.body);
        assert!(
            body.contains("# TYPE shrt_requests_total counter"),
            "{body}"
        );
        assert!(
            body.contains("shrt_requests_total{op=\"shorten\"}"),
            "{body}"
        );
        assert!(
            body.contains("shrt_cache_lookups_total{result=\"miss\"}"),
            "{body}"
        );
        assert!(body.contains("shrt_links_total"), "{body}");
        assert!(body.contains("shrt_uptime_seconds"), "{body}");
        assert!(body.contains("shrt_rate_limited_total"), "{body}");
    });
}

#[test]
fn ui_served() {
    run_suite(|port| {
        let r = get(port, "/");
        assert_eq!(r.status, 200);
        assert!(r.header("content-type").unwrap_or("").contains("text/html"));
        assert!(String::from_utf8_lossy(&r.body).contains("<title>shrt"));
    });
}

#[test]
fn shorten_redirect_stats_flow() {
    run_suite(|port| {
        let r = post(
            port,
            "/api/shorten",
            "{\"url\":\"https://example.com/some/path\"}",
        );
        assert_eq!(r.status, 201);
        let m = r.json();
        let code = m["code"].as_str().unwrap();
        assert_eq!(m["short_url"].as_str().unwrap(), format!("/{code}"));

        let redir = get(port, &format!("/{code}"));
        assert_eq!(redir.status, 302);
        assert_eq!(
            redir.header("location").unwrap(),
            "https://example.com/some/path"
        );

        let st = get(port, &format!("/api/stats/{code}")).json();
        assert_eq!(st["url"], "https://example.com/some/path");
        assert_eq!(st["hits"].as_i64().unwrap(), 1);
    });
}

#[test]
fn ttl_defaults_and_cap() {
    run_suite(|port| {
        const DAY: f64 = 86_400_000.0;
        let now = shrt::store::now_ms() as f64;

        let code = shorten(port, "https://ttl-default.example");
        let st = get(port, &format!("/api/stats/{code}")).json();
        let d = st["expires_at"].as_f64().unwrap() - (now + DAY);
        assert!(d.abs() < 5000.0, "default ttl off by {d}");

        let r = post(
            port,
            "/api/shorten",
            &format!(
                "{{\"url\":\"https://ttl-cap.example\",\"ttl_ms\":{}}}",
                (365.0 * DAY) as i64
            ),
        );
        let code2 = r.json()["code"].as_str().unwrap().to_string();
        let st2 = get(port, &format!("/api/stats/{code2}")).json();
        let d = st2["expires_at"].as_f64().unwrap() - (now + DAY);
        assert!(d.abs() < 5000.0, "capped ttl off by {d}");

        let code3 = shorten_body(
            port,
            "{\"url\":\"https://ttl-short.example\",\"ttl_ms\":5000}",
        );
        let st3 = get(port, &format!("/api/stats/{code3}")).json();
        let d = st3["expires_at"].as_f64().unwrap() - (now + 5000.0);
        assert!(d.abs() < 5000.0, "short ttl off by {d}");
    });
}

#[test]
fn custom_alias() {
    run_suite(|port| {
        let r = post(
            port,
            "/api/shorten",
            "{\"url\":\"https://a.com\",\"alias\":\"cool\"}",
        );
        assert_eq!(r.status, 201);
        let redir = get(port, "/cool");
        assert_eq!(redir.header("location").unwrap(), "https://a.com");
        let dup = post(
            port,
            "/api/shorten",
            "{\"url\":\"https://b.com\",\"alias\":\"cool\"}",
        );
        assert_eq!(dup.status, 409);
    });
}

#[test]
fn rejects_invalid_url() {
    run_suite(|port| {
        for u in ["notaurl", "ftp://x.com", "javascript:alert(1)"] {
            let r = post(port, "/api/shorten", &format!("{{\"url\":\"{u}\"}}"));
            assert_eq!(r.status, 400, "{u} -> {}", r.status);
        }
    });
}

#[test]
fn rejects_invalid_json_and_missing_url() {
    run_suite(|port| {
        for b in ["{bad", "{}"] {
            let r = post(port, "/api/shorten", b);
            assert_eq!(r.status, 400, "{b} -> {}", r.status);
        }
    });
}

#[test]
fn not_found() {
    run_suite(|port| {
        for p in ["/zzz", "/api/stats/zzz", "/a/b/c"] {
            assert_eq!(get(port, p).status, 404, "{p}");
        }
    });
}

#[test]
fn bulk_shorten() {
    run_suite(|port| {
        let urls: Vec<String> = (0..50)
            .map(|i| format!("https://bulk.example/{i}"))
            .collect();
        let body = serde_json::json!({"urls": urls}).to_string();
        let r = post(port, "/api/shorten/bulk", &body);
        assert_eq!(r.status, 201);
        let m = r.json();
        assert_eq!(m["count"].as_i64().unwrap(), 50);
        let codes = m["codes"].as_array().unwrap();
        let seen: std::collections::HashSet<&str> =
            codes.iter().map(|c| c.as_str().unwrap()).collect();
        assert_eq!(seen.len(), 50, "dup codes");
        let redir = get(port, &format!("/{}", codes[10].as_str().unwrap()));
        assert_eq!(redir.header("location").unwrap(), urls[10]);
    });
}

#[test]
fn bulk_rejects_bad_input() {
    run_suite(|port| {
        for urls in [
            "[]",
            "[\"ftp://x\"]",
            "[\"https://ok.com\",\"nope\"]",
            "\"notarray\"",
        ] {
            let r = post(port, "/api/shorten/bulk", &format!("{{\"urls\":{urls}}}"));
            assert_eq!(r.status, 400, "{urls} -> {}", r.status);
        }
    });
}

#[test]
fn rejects_oversized_body() {
    run_suite(|port| {
        let big = format!("{{\"url\":\"https://x.com/{}\"}}", "a".repeat(5000));
        let r = post(port, "/api/shorten", &big);
        assert!(
            r.status == 400 || r.status == 413,
            "oversized -> {}",
            r.status
        );
    });
}

#[test]
fn cors() {
    run_suite(|port| {
        let pre = req(port, "OPTIONS", "/api/shorten", &[], None);
        assert_eq!(pre.status, 204);
        assert_eq!(pre.header("access-control-allow-origin").unwrap(), "*");
        assert!(pre
            .header("access-control-allow-methods")
            .unwrap_or("")
            .contains("DELETE"));
        let r = get(port, "/api/health");
        assert_eq!(r.header("access-control-allow-origin").unwrap(), "*");
    });
}

#[test]
fn list_pagination_sort_search() {
    run_suite(|port| {
        shorten(port, "https://list-one.example");
        shorten(port, "https://list-two.example");

        let m = get(port, "/api/links?limit=5&offset=0").json();
        assert!(m["total"].as_i64().unwrap() >= 1);
        let links = m["links"].as_array().unwrap();
        assert!(!links.is_empty() && links.len() <= 5);
        let first = &links[0];
        assert!(first["code"].is_string() && first["url"].is_string());

        let sm = get(port, "/api/links?q=list-one").json();
        for l in sm["links"].as_array().unwrap() {
            let (u, c) = (l["url"].as_str().unwrap(), l["code"].as_str().unwrap());
            assert!(u.contains("list-one") || c.contains("list-one"), "q filter");
        }

        let tm = get(port, "/api/links?sort=hits&limit=3").json();
        let mut prev = i64::MAX;
        for l in tm["links"].as_array().unwrap() {
            let h = l["hits"].as_i64().unwrap();
            assert!(h <= prev, "hits not sorted desc");
            prev = h;
        }
    });
}

#[test]
fn admin_mutations() {
    let _lock = ADMIN_LOCK.lock().unwrap();
    run_suite(|port| {
        std::env::remove_var("ADMIN_TOKEN");
        let code = shorten(port, "https://before.example");

        let r = req(port, "DELETE", &format!("/api/links/{code}"), &[], None);
        assert_eq!(r.status, 404, "delete without token {}", r.status);

        // empty ADMIN_TOKEN must also fail closed
        std::env::set_var("ADMIN_TOKEN", "");
        let r0 = req(
            port,
            "DELETE",
            &format!("/api/links/{code}"),
            &[("x-admin-token", "")],
            None,
        );
        assert_eq!(r0.status, 404, "delete empty token {}", r0.status);

        std::env::set_var("ADMIN_TOKEN", "secret");

        // wrong token still 404
        let r2 = req(
            port,
            "DELETE",
            &format!("/api/links/{code}"),
            &[("x-admin-token", "wrong")],
            None,
        );
        assert_eq!(r2.status, 404, "delete wrong token {}", r2.status);

        // correct token -> PATCH works
        let pr = req(
            port,
            "PATCH",
            &format!("/api/links/{code}"),
            &[
                ("content-type", "application/json"),
                ("x-admin-token", "secret"),
            ],
            Some(b"{\"url\":\"https://after.example\",\"ttl_ms\":60000}"),
        );
        assert_eq!(pr.status, 200, "patch {}", pr.status);
        let redir = get(port, &format!("/{code}"));
        assert_eq!(redir.header("location").unwrap(), "https://after.example");
        let st = get(port, &format!("/api/stats/{code}")).json();
        assert!(
            st["expires_at"].as_i64().unwrap() > shrt::store::now_ms(),
            "ttl not applied"
        );

        // invalid url -> 400
        let br = req(
            port,
            "PATCH",
            &format!("/api/links/{code}"),
            &[
                ("content-type", "application/json"),
                ("x-admin-token", "secret"),
            ],
            Some(b"{\"url\":\"notaurl\"}"),
        );
        assert_eq!(br.status, 400, "patch bad url {}", br.status);

        // nothing to update -> 400
        let nr = req(
            port,
            "PATCH",
            "/api/links/nope",
            &[("x-admin-token", "secret")],
            Some(b"{}"),
        );
        assert_eq!(nr.status, 400, "patch empty {}", nr.status);
    });
}

#[test]
fn delete_removes_and_frees_alias() {
    let _lock = ADMIN_LOCK.lock().unwrap();
    run_suite(|port| {
        std::env::set_var("ADMIN_TOKEN", "secret");

        let r = post(
            port,
            "/api/shorten",
            "{\"url\":\"https://del.example\",\"alias\":\"todelete\"}",
        );
        assert_eq!(r.status, 201, "create");

        let dr = req(
            port,
            "DELETE",
            "/api/links/todelete",
            &[("x-admin-token", "secret")],
            None,
        );
        assert_eq!(dr.status, 204, "delete {}", dr.status);
        assert_eq!(get(port, "/todelete").status, 404, "deleted still resolves");
        let dr2 = req(
            port,
            "DELETE",
            "/api/links/todelete",
            &[("x-admin-token", "secret")],
            None,
        );
        assert_eq!(dr2.status, 404, "re-delete");
        let reuse = post(
            port,
            "/api/shorten",
            "{\"url\":\"https://new.example\",\"alias\":\"todelete\"}",
        );
        assert_eq!(reuse.status, 201, "alias not freed");
    });
}
