//! h2ts-proxy — terminate a WebSocket and forward raw bytes to an upstream h2c
//! server. A drop-in, in-Rust replacement for websockify (item 3), shipped as a
//! binary of the `ws-tcp` crate.
//!
//!   browser (h2ts) --ws--> [h2ts-proxy] --tcp--> upstream h2c server
//!
//! Usage: h2ts-proxy [listen_addr] [upstream_addr] [keepalive_secs]
//!        defaults:   127.0.0.1:8091   127.0.0.1:8000   0 (off)
//!
//! `keepalive_secs > 0` turns on server-initiated keepalive: the proxy pings an
//! idle client every N seconds and closes it (1001 Going Away) if no response
//! arrives within N more seconds.
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use ws_tcp::{accept, bridge_with, is_upgrade_request, BridgeConfig, KeepAlive};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let listen: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8091".to_string())
        .parse()?;
    let upstream: String = args.next().unwrap_or_else(|| "127.0.0.1:8000".to_string());
    // Optional 3rd arg: keepalive interval (and pong timeout) in seconds.
    let keepalive: Option<KeepAlive> = args
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(|s| KeepAlive::new(Duration::from_secs(s), Duration::from_secs(s)));

    let listener = TcpListener::bind(listen).await?;
    let ka = match &keepalive {
        Some(k) => format!("keepalive {}s", k.interval.as_secs()),
        None => "keepalive off".to_string(),
    };
    eprintln!("[h2ts-proxy] listening ws://{listen}  ->  tcp://{upstream} (h2c, {ka})");

    loop {
        let (socket, peer) = listener.accept().await?;
        let upstream = upstream.clone();
        let keepalive = keepalive.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(socket);
            let service =
                service_fn(move |req| handle(req, upstream.clone(), peer, keepalive.clone()));
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades()
                .await
            {
                eprintln!("[h2ts-proxy] connection error ({peer}): {err}");
            }
        });
    }
}

async fn handle(
    mut req: Request<Incoming>,
    upstream: String,
    peer: SocketAddr,
    keepalive: Option<KeepAlive>,
) -> Result<Response<Empty<Bytes>>, BoxError> {
    if !is_upgrade_request(&req) {
        // Not a WebSocket handshake — this endpoint only tunnels.
        return Ok(Response::builder()
            .status(426) // Upgrade Required
            .body(Empty::new())?);
    }

    let (response, ws_fut) = accept(&mut req)?;

    // Once the 101 is sent and the connection upgrades, bridge WS <-> upstream TCP.
    tokio::spawn(async move {
        let ws = match ws_fut.await {
            Ok(ws) => ws,
            Err(err) => {
                eprintln!("[h2ts-proxy] ws upgrade failed ({peer}): {err}");
                return;
            }
        };
        let upstream_tcp = match TcpStream::connect(&upstream).await {
            Ok(tcp) => tcp,
            Err(err) => {
                eprintln!("[h2ts-proxy] upstream connect failed ({upstream}): {err}");
                return;
            }
        };
        eprintln!("[h2ts-proxy] bridging ({peer}) <-> {upstream}");

        let config = BridgeConfig {
            keepalive,
            // Surface why the tunnel closed (peer close, keepalive timeout, EOF).
            on_close: Some(Box::new(move |cf| {
                eprintln!("[h2ts-proxy] closed ({peer}): {} {:?}", cf.code, cf.reason);
            })),
            ..Default::default()
        };
        if let Err(err) = bridge_with(ws, upstream_tcp, config).await {
            eprintln!("[h2ts-proxy] bridge error ({peer}): {err}");
        }
    });

    Ok(response)
}
