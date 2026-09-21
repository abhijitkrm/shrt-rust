//! shrt — high-performance URL shortener.
//!
//! Env config:
//!   PORT        3000    base listen port
//!   DATA_DIR    data    shard log directory (data-<i>.log, data-<i>.snap)
//!   SERVER      mini    "mini" (custom engine) or "hyper" (hyper/tokio)
//!   WORKERS     1       processes; all share PORT via SO_REUSEPORT
//!   INSTANCE    auto    instance id (auto-claimed via instance-<i>.lock files)
//!   SEED        0       bulk-insert N links if empty (random codes)
//!   HITS        1       "0" disables hit counting
//!   TAIL_MS     0       >0 enables periodic sibling-log polling (on-miss always on)
//!   CORS_ORIGIN *       value of Access-Control-Allow-Origin
//!   ADMIN_TOKEN unset   enables PATCH/DELETE; requests need x-admin-token: <value>
//!   LINK_TTL_MS 86400000  default AND max link lifetime

use std::io;
use std::net::{SocketAddr, TcpListener};
use std::process::{exit, Command};
use std::sync::Arc;

use mimalloc::MiMalloc;
use socket2::{Domain, SockAddr, Socket, Type};

use shrt::{metrics, server_hyper, server_mini, store::Store};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(def)
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| def.to_string())
}

fn port() -> i64 {
    env_int("PORT", 3000)
}

fn data_dir() -> String {
    env_str("DATA_DIR", "data")
}

/// Bulk-insert N links if the store is empty (through the write path).
fn seed(n: usize) {
    match Store::new(&data_dir(), 0) {
        Ok(s) => {
            if s.is_empty() {
                let urls: Vec<String> =
                    (0..n).map(|i| format!("https://example.com/{i}")).collect();
                s.seed(&urls);
            }
            s.close();
        }
        Err(e) => eprintln!("seed: {e}"),
    }
}

/// Spawn `workers` child processes. All children bind the same PORT via
/// SO_REUSEPORT — the kernel load-balances connections across them.
fn run_supervisor(n: usize) {
    let exe = std::env::current_exe().expect("current_exe");
    let mut children = Vec::new();
    for i in 0..n {
        let mut cmd = Command::new(&exe);
        cmd.env("SHRT_CHILD", "1")
            .env("INSTANCE", i.to_string())
            .env("WORKERS", "1")
            .env("SEED", "0");
        match cmd.spawn() {
            Ok(c) => children.push(c),
            Err(e) => eprintln!("spawn worker {i}: {e}"),
        }
    }
    println!(
        "supervisor: {} workers sharing :{} (pid {})",
        children.len(),
        port(),
        std::process::id()
    );

    let pids: Vec<u32> = children.iter().map(|c| c.id()).collect();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d2 = done.clone();
    let _ = ctrlc::set_handler(move || {
        for &p in &pids {
            unsafe {
                libc::kill(p as i32, libc::SIGTERM);
            }
        }
        d2.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    while !done.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    for mut c in children {
        let _ = c.wait();
    }
}

/// Bind a TCP socket with SO_REUSEPORT so sibling processes can share the
/// port (and single-process restarts don't hit TIME_WAIT bind errors).
fn listen(port: u16) -> io::Result<TcpListener> {
    let sock = Socket::new(Domain::IPV6, Type::STREAM, None)?;
    sock.set_only_v6(false)?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&SockAddr::from(SocketAddr::from((
        [0, 0, 0, 0, 0, 0, 0, 0],
        port,
    ))))?;
    sock.listen(1024)?;
    Ok(sock.into())
}

fn serve() {
    let st = match Store::new(&data_dir(), env_int("INSTANCE", -1) as i32) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let ln = match listen(port() as u16) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind :{}: {e}", port());
            exit(1);
        }
    };

    {
        let st = st.clone();
        let _ = ctrlc::set_handler(move || {
            st.close();
            exit(0);
        });
    }

    match env_str("SERVER", "mini").as_str() {
        "hyper" => {
            println!(
                "hyper listening on :{} (pid {})",
                port(),
                std::process::id()
            );
            let _ = server_hyper::serve(ln, st.clone());
        }
        _ => {
            println!("fast listening on :{} (pid {})", port(), std::process::id());
            let _ = server_mini::serve(ln, st.clone());
        }
    }
    st.close();
}

fn main() {
    metrics::init();
    let workers = env_int("WORKERS", 1);
    let seed_n = env_int("SEED", 0);
    if workers > 1 && std::env::var("SHRT_CHILD").is_err() {
        if seed_n > 0 {
            seed(seed_n as usize);
        }
        run_supervisor(workers as usize);
        return;
    }
    if seed_n > 0 {
        seed(seed_n as usize);
    }
    serve();
}
