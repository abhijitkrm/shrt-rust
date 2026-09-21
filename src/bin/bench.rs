//! shrt-bench spawns real shrt servers and drives load with a small raw-TCP
//! load generator (keep-alive + optional HTTP pipelining), mirroring the
//! shrt-ts autocannon scenarios.
//!
//! Usage: cargo run --release --bin shrt-bench
//!   env: BENCH_DURATION=5 CONNECTIONS=64

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn env_i64(k: &str, def: i64) -> i64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(def)
}

fn duration_secs() -> i64 {
    env_i64("BENCH_DURATION", 5)
}

fn connections() -> i64 {
    env_i64("CONNECTIONS", 64)
}

fn fmt_n(n: f64) -> String {
    format!("{n:.0}")
}

// ---------- tiny load generator ----------

struct Result_ {
    reqs: i64,
    non2xx: i64,
    errs: i64,
    avg: f64,
    p99: f64,
}

/// Parse one HTTP/1.1 response; returns the status code.
/// lbuf/bbuf are caller-owned scratch — no allocation after warmup.
fn read_resp(r: &mut BufReader<TcpStream>, lbuf: &mut String, bbuf: &mut Vec<u8>) -> std::io::Result<u16> {
    lbuf.clear();
    if r.read_line(lbuf)? == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
    }
    let code: u16 = lbuf
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut cl = 0usize;
    let mut chunked = false;
    loop {
        lbuf.clear();
        let n = r.read_line(lbuf)?;
        let h = lbuf.trim_end();
        if n == 0 || h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            let v = v.trim();
            if k.eq_ignore_ascii_case("content-length") {
                cl = v.parse().unwrap_or(0);
            } else if k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked") {
                chunked = true;
            }
        }
    }
    if chunked {
        loop {
            lbuf.clear();
            r.read_line(lbuf)?;
            let n = usize::from_str_radix(lbuf.trim(), 16).unwrap_or(0);
            if n == 0 {
                lbuf.clear();
                let _ = r.read_line(lbuf);
                break;
            }
            bbuf.clear();
            bbuf.resize(n + 2, 0);
            r.read_exact(bbuf)?;
        }
    } else if cl > 0 {
        bbuf.clear();
        bbuf.resize(cl, 0);
        r.read_exact(bbuf)?;
    }
    Ok(code)
}

/// Drive `reqs` (cycled) over `conns` keep-alive connections for `dur`,
/// `pipe`-deep HTTP pipelining.
fn blast(host: &str, conns: usize, reqs: &[Vec<u8>], pipe: usize, dur: Duration) -> Result_ {
    let total = Arc::new(AtomicI64::new(0));
    let non2xx = Arc::new(AtomicI64::new(0));
    let errs = Arc::new(AtomicI64::new(0));
    let lats = Arc::new(Mutex::new(Vec::<f64>::new()));
    let deadline = Instant::now() + dur;
    let reqs = Arc::new(reqs.to_vec());
    let host = host.to_string();

    let mut handles = Vec::new();
    for c in 0..conns {
        let (total, non2xx, errs, lats, reqs, host) = (
            total.clone(),
            non2xx.clone(),
            errs.clone(),
            lats.clone(),
            reqs.clone(),
            host.clone(),
        );
        handles.push(std::thread::spawn(move || {
            let Ok(stream) = TcpStream::connect(&host) else {
                errs.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let _ = stream.set_nodelay(true);
            let mut br = BufReader::with_capacity(
                64 << 10,
                match stream.try_clone() {
                    Ok(s) => s,
                    Err(_) => return,
                },
            );
            let mut i = c % reqs.len();
            let mut my_lats = Vec::new();
            let mut lbuf = String::with_capacity(256);
            let mut bbuf: Vec<u8> = Vec::with_capacity(1024);
            while Instant::now() < deadline {
                let start = Instant::now();
                let mut batch = 0usize;
                for _ in 0..pipe {
                    if stream.try_clone().unwrap().write_all(&reqs[i]).is_err() {
                        errs.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    batch += 1;
                    i = (i + 1) % reqs.len();
                }
                if batch == 0 {
                    break;
                }
                let mut got = 0usize;
                while got < batch {
                    match read_resp(&mut br, &mut lbuf, &mut bbuf) {
                        Ok(code) => {
                            got += 1;
                            if !(200..400).contains(&code) {
                                non2xx.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            errs.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                if got == 0 {
                    break;
                }
                let el = start.elapsed().as_secs_f64() * 1000.0 / got as f64; // ms/req
                total.fetch_add(got as i64, Ordering::Relaxed);
                my_lats.push(el);
            }
            lats.lock().unwrap().extend(my_lats);
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let mut lat = lats.lock().unwrap().clone();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = if lat.is_empty() {
        0.0
    } else {
        lat.iter().sum::<f64>() / lat.len() as f64
    };
    let p99 = if lat.is_empty() {
        0.0
    } else {
        lat[(lat.len() as f64 * 0.99) as usize]
    };
    Result_ {
        reqs: total.load(Ordering::Relaxed),
        non2xx: non2xx.load(Ordering::Relaxed),
        errs: errs.load(Ordering::Relaxed),
        avg,
        p99,
    }
}

fn report(name: &str, res: &Result_, rows_per_req: f64) {
    let rps = res.reqs as f64 / duration_secs() as f64;
    println!(
        "{:<28} {:>10} req/s  {:>10} rows/s  lat avg {:>6.2}ms  p99 {:>6.2}ms  non2xx/3xx {:>7}  err {}",
        name,
        fmt_n(rps),
        fmt_n(rps * rows_per_req),
        res.avg,
        res.p99,
        res.non2xx,
        res.errs
    );
}

// ---------- server lifecycle ----------

fn wait_healthy(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ =
                s.write_all(b"GET /api/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            let mut buf = [0u8; 256];
            if let Ok(n) = s.read(&mut buf) {
                if String::from_utf8_lossy(&buf[..n]).contains("200") {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server :{port} did not start");
}

fn start_server(envs: &[(&str, String)], bin: &str) -> Child {
    let mut cmd = Command::new(bin);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::inherit());
    let child = cmd.spawn().expect("spawn server");
    wait_healthy(
        envs.iter()
            .find(|(k, _)| *k == "PORT")
            .unwrap()
            .1
            .parse()
            .unwrap(),
    );
    child
}

fn stop_server(child: &mut Child) {
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let _ = child.wait();
}

// ---------- request templates ----------

fn get_req(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").into_bytes()
}

fn post_req(path: &str, body: &str) -> Vec<u8> {
    format!(
        "POST {path} HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

/// Create n aliased links via the API so the bench knows valid codes.
fn make_codes(port: u16, n: usize) -> Vec<String> {
    let mut codes = Vec::with_capacity(n);
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let mut br = BufReader::new(stream.try_clone().unwrap());
    let mut lbuf = String::with_capacity(256);
    let mut bbuf: Vec<u8> = Vec::with_capacity(1024);
    for i in 0..n {
        let code = format!("bk{i}");
        let body = format!("{{\"url\":\"https://bench.example/{i}\",\"alias\":\"{code}\"}}");
        stream.write_all(&post_req("/api/shorten", &body)).unwrap();
        let status = read_resp(&mut br, &mut lbuf, &mut bbuf).unwrap_or(0);
        if status != 201 {
            panic!("alias seed failed: {status}");
        }
        codes.push(code);
    }
    codes
}

fn main() {
    let keyspace = 50_000usize;
    let tmp = std::env::temp_dir();
    let bin = tmp.join("shrt-rust-bench-bin");
    let port_base = 4600u16;

    // build the server binary once (release)
    let st = Command::new("cargo")
        .args(["build", "--release", "--bin", "shrt"])
        .stderr(std::process::Stdio::inherit())
        .status()
        .expect("cargo build");
    assert!(st.success());
    let built = format!(
        "{}/target/release/shrt",
        std::env::current_dir().unwrap().display()
    );
    std::fs::copy(&built, &bin).expect("copy bin");
    let bin = bin.to_string_lossy().to_string();

    println!(
        "bench: {} conns x {}s, keyspace {}\n",
        connections(),
        duration_secs(),
        keyspace
    );

    let write_req = post_req("/api/shorten", "{\"url\":\"https://bench.example/write\"}");
    const BULK_N: usize = 1000;
    let mut sb = String::from("{\"urls\":[");
    for i in 0..BULK_N {
        if i > 0 {
            sb.push(',');
        }
        sb.push_str(&format!("\"https://b.example/{i}\""));
    }
    sb.push_str("]}");
    let bulk_req = post_req("/api/shorten/bulk", &sb);

    let mut port = port_base;
    let mut env_for = |tag: &str, extra: &[(&str, String)]| -> (u16, Vec<(String, String)>) {
        port += 10;
        let dir = tmp.join(format!("bench-rust-{tag}-{port}"));
        let _ = std::fs::remove_dir_all(&dir);
        let mut e: Vec<(String, String)> = vec![
            ("PORT".into(), port.to_string()),
            ("DATA_DIR".into(), dir.to_string_lossy().to_string()),
        ];
        for (k, v) in extra {
            e.push((k.to_string(), v.clone()));
        }
        (port, e)
    };

    // --- mini, single instance ---
    {
        let (port, env) = env_for(
            "mini1",
            &[("SERVER", "mini".into()), ("SEED", keyspace.to_string())],
        );
        let envr: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let mut srv = start_server(&envr, &bin);
        let codes = make_codes(port, 100);
        let reqs: Vec<Vec<u8>> = codes.iter().map(|c| get_req(&format!("/{c}"))).collect();
        let host = format!("127.0.0.1:{port}");
        report(
            "redirect (mini)",
            &blast(
                &host,
                connections() as usize,
                &reqs,
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        report(
            "redirect (mini, p10)",
            &blast(
                &host,
                connections() as usize,
                &reqs,
                10,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        let mut mixed: Vec<Vec<u8>> = reqs[..95].to_vec();
        for _ in 0..5 {
            mixed.push(write_req.clone());
        }
        report(
            "mixed 95/5 (mini)",
            &blast(
                &host,
                connections() as usize,
                &mixed,
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        report(
            "shorten (mini)",
            &blast(
                &host,
                connections() as usize,
                std::slice::from_ref(&write_req),
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        report(
            "bulk x1000 (mini)",
            &blast(
                &host,
                (connections() / 4) as usize,
                std::slice::from_ref(&bulk_req),
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            BULK_N as f64,
        );
        stop_server(&mut srv);
    }

    // --- hyper comparison ---
    {
        let (port, env) = env_for(
            "hyper1",
            &[("SERVER", "hyper".into()), ("SEED", keyspace.to_string())],
        );
        let envr: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let mut srv = start_server(&envr, &bin);
        let codes = make_codes(port, 100);
        let reqs: Vec<Vec<u8>> = codes.iter().map(|c| get_req(&format!("/{c}"))).collect();
        let host = format!("127.0.0.1:{port}");
        report(
            "redirect (hyper)",
            &blast(
                &host,
                connections() as usize,
                &reqs,
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        report(
            "shorten (hyper)",
            &blast(
                &host,
                connections() as usize,
                std::slice::from_ref(&write_req),
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        stop_server(&mut srv);
    }

    // --- mini multi-instance: 4 procs sharing the port via SO_REUSEPORT ---
    {
        let (port, env) = env_for(
            "mini4",
            &[
                ("SERVER", "mini".into()),
                ("WORKERS", "4".into()),
                ("SEED", keyspace.to_string()),
            ],
        );
        let envr: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let mut srv = start_server(&envr, &bin);
        let host = format!("127.0.0.1:{port}");
        report(
            "bulk x1000 (mini x4)",
            &blast(
                &host,
                (connections() / 2) as usize,
                std::slice::from_ref(&bulk_req),
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            BULK_N as f64,
        );
        let codes = make_codes(port, 100);
        let reqs: Vec<Vec<u8>> = codes.iter().map(|c| get_req(&format!("/{c}"))).collect();
        // warm once so lazy tailing merges aliases across instances
        for c in &codes {
            if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                let _ = s.write_all(&get_req(&format!("/{c}")));
                let mut b = [0u8; 512];
                let _ = s.read(&mut b);
            }
        }
        report(
            "redirect (mini x4)",
            &blast(
                &host,
                connections() as usize,
                &reqs,
                1,
                Duration::from_secs(duration_secs() as u64),
            ),
            1.0,
        );
        stop_server(&mut srv);
    }
}
