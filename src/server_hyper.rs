//! Fallback frontend: hyper 1.x on tokio — the portable-comparison engine
//! (mirrors the `SERVER=node` role in shrt-ts).

use std::convert::Infallible;
use std::net::TcpListener as StdListener;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener as TokListener;

use crate::app::{self, Reply};
use crate::store::Store;

fn to_response(r: Reply) -> Response<Full<Bytes>> {
    let mut b = Response::builder().status(r.status);
    let h = b.headers_mut().unwrap();
    h.insert(
        "access-control-allow-origin",
        app::cors_origin().parse().unwrap(),
    );
    h.insert(
        "access-control-allow-methods",
        "GET,POST,PATCH,DELETE,OPTIONS".parse().unwrap(),
    );
    h.insert(
        "access-control-allow-headers",
        "content-type".parse().unwrap(),
    );
    h.insert("access-control-max-age", "86400".parse().unwrap());
    if let Some(loc) = &r.location {
        if let Ok(v) = loc.as_ref().parse() {
            h.insert("location", v);
        }
    } else {
        h.insert(
            "content-type",
            r.ctype.unwrap_or("application/json").parse().unwrap(),
        );
    }
    b.body(Full::new(Bytes::from(r.body))).unwrap()
}

async fn serve_conn(
    stream: tokio::net::TcpStream,
    st: Store,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peer = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let trust_proxy = std::env::var_os("TRUST_PROXY").is_some();
    let io = TokioIo::new(stream);
    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
        let st = st.clone();
        let peer = peer.clone();
        async move {
            let method = req.method().as_str().to_string();
            let path = req
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/")
                .to_string();
            let admin = req
                .headers()
                .get("x-admin-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let client = if trust_proxy {
                req.headers()
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.split(',').next())
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| peer.clone())
            } else {
                peer.clone()
            };
            let needs_body = method == "POST" || method == "PATCH";
            let limit = if path.split('?').next() == Some("/api/shorten/bulk") {
                app::MAX_BULK_BODY
            } else {
                app::MAX_BODY
            };
            let mut body = Vec::new();
            let mut oversized = false;
            if needs_body {
                let mut stream_body = req.into_body();
                while let Some(frame) = stream_body.frame().await {
                    match frame {
                        Ok(f) => {
                            if let Some(chunk) = f.data_ref() {
                                if body.len() + chunk.len() > limit {
                                    oversized = true;
                                    break;
                                }
                                body.extend_from_slice(chunk);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            let reply = if oversized {
                Reply {
                    status: 413,
                    location: None,
                    body: b"{\"error\":\"body too large\"}".to_vec(),
                    ctype: None,
                }
            } else {
                app::handle(&st, &method, &path, &body, &admin, &client)
            };
            Ok::<_, Infallible>(to_response(reply))
        }
    });
    hyper::server::conn::http1::Builder::new()
        .serve_connection(io, svc)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
}

/// Serve hyper on an already-bound std listener (shared with the mini
/// frontend so SO_REUSEPORT works identically).
pub fn serve(listener: StdListener, store: Store) -> std::io::Result<()> {
    listener.set_nonblocking(true)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let ln = TokListener::from_std(listener)?;
        loop {
            match ln.accept().await {
                Ok((stream, _)) => {
                    let st = store.clone();
                    tokio::spawn(async move {
                        let _ = serve_conn(stream, st).await;
                    });
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
}
