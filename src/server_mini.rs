//! The default frontend: a minimal HTTP/1.1 server tuned for this workload —
//! thread-per-connection, httparse request parsing, and all complete requests
//! in a read batch answered with a single write (HTTP pipelining supported).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::app::{self, Reply};
use crate::store::Store;

const READ_CAP_INIT: usize = 16 << 10;
const MAX_HEADERS: usize = 24;

fn status_line(code: u16) -> &'static str {
    match code {
        200 => "HTTP/1.1 200 OK\r\n",
        201 => "HTTP/1.1 201 Created\r\n",
        204 => "HTTP/1.1 204 No Content\r\n",
        302 => "HTTP/1.1 302 Found\r\n",
        400 => "HTTP/1.1 400 Bad Request\r\n",
        404 => "HTTP/1.1 404 Not Found\r\n",
        409 => "HTTP/1.1 409 Conflict\r\n",
        413 => "HTTP/1.1 413 Content Too Large\r\n",
        _ => "HTTP/1.1 500 Internal Server Error\r\n",
    }
}

fn cors_block() -> &'static [u8] {
    static V: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    V.get_or_init(|| {
        format!(
            "access-control-allow-origin: {}\r\naccess-control-allow-methods: GET,POST,PATCH,DELETE,OPTIONS\r\naccess-control-allow-headers: content-type\r\naccess-control-max-age: 86400\r\n",
            app::cors_origin()
        )
        .into_bytes()
    })
}

fn write_reply(out: &mut Vec<u8>, r: &Reply) {
    out.extend_from_slice(status_line(r.status).as_bytes());
    out.extend_from_slice(cors_block());
    if let Some(loc) = &r.location {
        out.extend_from_slice(b"location: ");
        out.extend_from_slice(loc.as_bytes());
        out.extend_from_slice(b"\r\ncontent-length: 0\r\n\r\n");
        return;
    }
    out.extend_from_slice(b"content-type: ");
    out.extend_from_slice(r.ctype.unwrap_or("application/json").as_bytes());
    out.extend_from_slice(b"\r\ncontent-length: ");
    out.extend_from_slice(r.body.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(&r.body);
}

fn header_val<'a>(req: &httparse::Request<'a, '_>, name: &str) -> Option<&'a [u8]> {
    req.headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value)
}

fn keep_alive(req: &httparse::Request<'_, '_>, version: u8) -> bool {
    match header_val(req, "connection") {
        Some(v) => !v.eq_ignore_ascii_case(b"close"),
        None => version == 1, // HTTP/1.1 default keep-alive
    }
}

/// Serve connections on `listener` forever, one thread per connection.
pub fn serve(listener: TcpListener, store: Store) -> std::io::Result<()> {
    let st = Arc::new(store);
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let st = st.clone();
                thread::spawn(move || {
                    let _ = handle_conn(stream, st);
                });
            }
            Err(_) => std::thread::yield_now(),
        }
    }
    Ok(())
}

fn handle_conn(mut stream: TcpStream, st: Arc<Store>) -> std::io::Result<()> {
    let _ = stream.set_nodelay(true);
    let mut buf: Vec<u8> = Vec::with_capacity(READ_CAP_INIT);
    let mut out: Vec<u8> = Vec::with_capacity(READ_CAP_INIT);
    let mut parsed_off = 0usize;
    let mut close_after = false;

    loop {
        // grow/read into spare capacity without zeroing
        if buf.len() == buf.capacity() {
            buf.reserve(READ_CAP_INIT.max(buf.capacity()));
        }
        let n = {
            let len = buf.len();
            let cap = buf.capacity();
            let spare =
                unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(len), cap - len) };
            match stream.read(spare) {
                Ok(n) => {
                    unsafe { buf.set_len(len + n) };
                    n
                }
                Err(_) => return Ok(()),
            }
        };
        if n == 0 && buf.len() == parsed_off {
            return Ok(()); // EOF with nothing pending
        }

        // process every complete request in the buffer
        loop {
            let mut hdrs = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut req = httparse::Request::new(&mut hdrs);
            let rest = &buf[parsed_off..];
            let hdrlen = match req.parse(rest) {
                Ok(httparse::Status::Complete(n)) => n,
                Ok(httparse::Status::Partial) => break,
                Err(_) => return Ok(()), // malformed: drop conn
            };
            let method = req.method.unwrap_or("GET");
            let version = req.version.unwrap_or(1);
            let path = req.path.unwrap_or("/");
            let alive = keep_alive(&req, version);
            let cl = header_val(&req, "content-length")
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let admin = header_val(&req, "x-admin-token")
                .and_then(|v| std::str::from_utf8(v).ok())
                .unwrap_or("");

            let needs_body = method == "POST" || method == "PATCH";
            let limit = if path.split('?').next() == Some("/api/shorten/bulk") {
                app::MAX_BULK_BODY
            } else {
                app::MAX_BODY
            };
            if needs_body && cl > limit {
                write_reply(
                    &mut out,
                    &Reply {
                        status: 413,
                        location: None,
                        body: b"{\"error\":\"body too large\"}".to_vec(),
                        ctype: None,
                    },
                );
                let _ = stream.write_all(&out);
                return Ok(()); // like the TS build: destroy on oversize
            }
            let total = hdrlen + if needs_body { cl } else { 0 };
            if buf.len() - parsed_off < total {
                break; // body hasn't fully arrived; read more
            }
            let body: &[u8] = if needs_body {
                &buf[parsed_off + hdrlen..parsed_off + total]
            } else {
                &[]
            };
            let reply = app::handle(&st, method, path, body, admin);
            write_reply(&mut out, &reply);
            parsed_off += total;
            if !alive {
                close_after = true;
                break;
            }
        }

        // compact consumed prefix
        if parsed_off > 0 {
            buf.drain(..parsed_off);
            parsed_off = 0;
        }
        if !out.is_empty() {
            stream.write_all(&out)?;
            out.clear();
        }
        if close_after || n == 0 {
            return Ok(()); // close requested, or EOF with a partial tail
        }
    }
}
