//! RATE_LIMIT integration test — own binary so env setup can't race other
//! tests' POST traffic through the OnceLock'd global limiter.

use shrt::{server_mini, store::Store};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn post(port: u16, path: &str, body: &str) -> u16 {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap()
}

#[test]
fn per_ip_rate_limit_on_shorten() {
    unsafe {
        std::env::set_var("RATE_LIMIT", "1");
        std::env::set_var("RATE_LIMIT_BURST", "2");
    }
    let st = Store::new(":memory:", -1).unwrap();
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = ln.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let _ = server_mini::serve(ln, st);
    });
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let body = r#"{"url":"https://rl.example"}"#;
    assert_eq!(post(port, "/api/shorten", body), 201);
    assert_eq!(post(port, "/api/shorten", body), 201);
    assert_eq!(post(port, "/api/shorten", body), 429, "burst exhausted");
    // redirects are NOT rate limited
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.write_all(b"GET /nope HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 404"));
}
