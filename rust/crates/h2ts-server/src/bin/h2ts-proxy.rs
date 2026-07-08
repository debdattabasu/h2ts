//! h2ts-proxy — terminate a WebSocket and forward raw bytes to an upstream h2c
//! server. A drop-in, in-Rust replacement for websockify (item 3), shipped as a
//! binary of the `h2ts-server` crate.
//!
//!   browser (h2ts) --ws--> [h2ts-proxy] --tcp--> upstream h2c server
//!
//! Usage: h2ts-proxy [listen_addr] [upstream_addr] [keepalive_secs] [--no-keepalive] [--allow-implicit-codec]
//!        defaults:   127.0.0.1:8091   127.0.0.1:8000   30/15 (on)   (keepalive on) (off; require h2ts)
//!
//! Server-initiated keepalive is **on by default** (ping an idle client every 30s,
//! close it with 1001 Going Away if no response in a further 15s) so a silently-
//! dead client can't leak the tunnel. Override the interval with a positive
//! `keepalive_secs` (pong timeout equals the interval); pass `0` or
//! `--no-keepalive` (anywhere on the command line) to turn it off.
//!
//! By default the proxy requires the `h2ts` subprotocol (rejecting others with a
//! `400`). `--allow-implicit-codec` makes it a codec-agnostic byte tunnel that
//! accepts whatever subprotocol the client offers (websockify-style `binary`,
//! none, …) — the flag may appear anywhere on the command line.
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use h2ts_server::{
    accept_with_options, bridge_with, AcceptOptions, BridgeConfig, CloseFrame, KeepAlive,
};
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Flags may appear anywhere; the rest are positional.
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let allow_implicit_codec = args.iter().any(|a| a == "--allow-implicit-codec");
    let no_keepalive = args.iter().any(|a| a == "--no-keepalive");
    args.retain(|a| a != "--allow-implicit-codec" && a != "--no-keepalive");
    let mut args = args.into_iter();

    let listen: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8091".to_string())
        .parse()?;
    let upstream: String = args.next().unwrap_or_else(|| "127.0.0.1:8000".to_string());
    // Keepalive is on by default. Opt out with `--no-keepalive` (or a `0` in the
    // optional 3rd positional). A positive 3rd positional overrides the interval
    // (with an equal pong timeout); otherwise the sensible default (30s/15s) is used.
    let keepalive: Option<KeepAlive> = if no_keepalive {
        None
    } else {
        match args.next() {
            Some(s) => s
                .parse::<u64>()
                .ok()
                .filter(|&s| s > 0)
                .map(|s| KeepAlive::new(Duration::from_secs(s), Duration::from_secs(s))),
            None => Some(KeepAlive::default()),
        }
    };

    let listener = TcpListener::bind(listen).await?;
    let ka = match &keepalive {
        Some(k) => format!("keepalive {}s", k.interval.as_secs()),
        None => "keepalive off".to_string(),
    };
    let codec = if allow_implicit_codec {
        "any subprotocol"
    } else {
        "h2ts only"
    };
    eprintln!("[h2ts-proxy] listening ws://{listen}  ->  tcp://{upstream} (h2c, {ka}, {codec})");

    loop {
        // A transient accept error (EMFILE/ENFILE/ECONNABORTED) must not kill the
        // listener — log, back off briefly, and keep serving.
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("[h2ts-proxy] accept error: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let upstream = upstream.clone();
        let keepalive = keepalive.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(socket);
            let service = service_fn(move |req| {
                handle(
                    req,
                    upstream.clone(),
                    peer,
                    keepalive.clone(),
                    allow_implicit_codec,
                )
            });
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
    allow_implicit_codec: bool,
) -> Result<Response<Empty<Bytes>>, BoxError> {
    // Require h2ts by default; `--allow-implicit-codec` makes this a codec-agnostic
    // tunnel that accepts whatever subprotocol the client offers (websockify-style
    // `binary`, none, …). Either way a non-WebSocket request rejects (426), and a
    // non-h2ts client rejects (400) when the flag is off.
    let (response, ws_fut) = match accept_with_options(
        &mut req,
        |_offered| None,
        AcceptOptions {
            allow_implicit_codec,
        },
    ) {
        Ok(pair) => pair,
        Err(err) => {
            eprintln!("[h2ts-proxy] rejected ({peer}): {err}");
            return Ok(err.rejection_response());
        }
    };

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
                // Tell the client the origin is unreachable with a 1014 Bad Gateway
                // close (à la nginx 502) instead of dropping the WS as a bare 1006.
                // An empty peer EOFs immediately, so the bridge just sends the close.
                let cfg = BridgeConfig {
                    close: CloseFrame {
                        code: 1014,
                        reason: "bad gateway".to_string(),
                    },
                    keepalive: None,
                    ..Default::default()
                };
                let _ = bridge_with(ws, tokio::io::empty(), cfg).await;
                return;
            }
        };
        eprintln!("[h2ts-proxy] bridging ({peer}) <-> {upstream}");

        let config = BridgeConfig {
            keepalive,
            // An upstream failure (reset/write error) closes the client with 1014
            // Bad Gateway, distinct from a clean 1000 EOF.
            error_close: CloseFrame {
                code: 1014,
                reason: "bad gateway".to_string(),
            },
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
