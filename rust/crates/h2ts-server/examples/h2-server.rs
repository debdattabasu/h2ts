//! Example: serve an in-process hyper HTTP/2 service over a WebSocket tunnel
//! using [`h2ts_server::serve_h2`]. This is just a *caller* of the library — the
//! reusable machinery all lives in the `h2ts-server` crate.
//!
//!   browser (h2ts) --ws--> [accept -> serve_h2(ws, service)] --> your service
//!
//! The routes mirror the Node echo server so the h2ts e2e suite validates this
//! pure-Rust path end to end.
//!
//! Run: cargo run -p h2ts-server --example h2-server -- 127.0.0.1:8092
use std::convert::Infallible;
use std::net::SocketAddr;

use anyhow::Result;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use h2ts_server::{accept, serve_h2};

#[tokio::main]
async fn main() -> Result<()> {
    let listen: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8092".to_string())
        .parse()?;
    let listener = TcpListener::bind(listen).await?;
    eprintln!("[h2-server] ws://{listen}  ->  in-process hyper HTTP/2 (h2c) service");

    loop {
        let (socket, _peer) = listener.accept().await?;
        tokio::spawn(async move {
            // Outer HTTP/1.1 connection carries the WebSocket handshake.
            let io = TokioIo::new(socket);
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(on_request))
                .with_upgrades()
                .await
            {
                eprintln!("[h2-server] http/1 connection error: {err}");
            }
        });
    }
}

/// Accept the WebSocket upgrade, then serve HTTP/2 over the tunnel.
async fn on_request(mut req: Request<Incoming>) -> Result<Response<Full<Bytes>>> {
    // `accept` requires the h2ts subprotocol; a non-upgrade request (426) or a
    // client without h2ts (400) is rejected with a clean response.
    let (response, ws_fut) = match accept(&mut req) {
        Ok(pair) => pair,
        Err(err) => return Ok(err.rejection_response().map(|_| Full::new(Bytes::new()))),
    };
    tokio::spawn(async move {
        let ws = match ws_fut.await {
            Ok(ws) => ws,
            Err(err) => {
                eprintln!("[h2-server] ws upgrade failed: {err}");
                return;
            }
        };
        // Hand the WebSocket to the library, which serves our service as HTTP/2
        // over the tunnel. `app` here is just an example — any hyper service
        // (service_fn, axum::Router, tower service) works.
        if let Err(err) = serve_h2(ws, service_fn(app)).await {
            eprintln!("[h2-server] h2 connection error: {err}");
        }
    });

    // `accept` returns a `Response<Empty<Bytes>>`; align the body type.
    Ok(response.map(|_| Full::new(Bytes::new())))
}

/// The application service, served over HTTP/2.
async fn app(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(route(req).await.unwrap_or_else(|err| {
        Response::builder()
            .status(500)
            .body(Full::new(Bytes::from(format!("error: {err}\n"))))
            .unwrap()
    }))
}

async fn route(req: Request<Incoming>) -> Result<Response<Full<Bytes>>> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    eprintln!("[h2-server] {method} {path}");

    match path.as_str() {
        "/hello" => text("hello from in-process Rust h2 server over websocket!\n"),

        "/json" => build(
            200,
            "application/json",
            &[],
            Bytes::from(format!(
                "{{\"ok\":true,\"method\":\"{method}\",\"path\":\"{path}\",\"ts\":1234567890}}"
            )),
        ),

        "/big" => {
            let size = 256 * 1024;
            build(
                200,
                "application/octet-stream",
                &[("x-size", size.to_string())],
                Bytes::from(vec![b'x'; size]),
            )
        }

        "/echo" => {
            let body = req.into_body().collect().await?.to_bytes();
            let len = body.len();
            build(
                200,
                "application/octet-stream",
                &[("x-echo-bytes", len.to_string())],
                body,
            )
        }

        "/headers" => {
            let seen = req
                .headers()
                .get("x-custom")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            build(200, "text/plain", &[("x-saw-custom", seen)], Bytes::from("ok"))
        }

        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("not found\n")))?),
    }
}

fn text(body: &str) -> Result<Response<Full<Bytes>>> {
    build(200, "text/plain; charset=utf-8", &[], Bytes::from(body.to_string()))
}

fn build(
    status: u16,
    content_type: &str,
    extra: &[(&str, String)],
    body: Bytes,
) -> Result<Response<Full<Bytes>>> {
    let mut b = Response::builder().status(status).header("content-type", content_type);
    for (name, value) in extra {
        b = b.header(*name, value);
    }
    Ok(b.body(Full::new(body))?)
}
