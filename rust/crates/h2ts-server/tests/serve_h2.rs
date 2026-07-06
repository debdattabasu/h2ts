//! `serve_h2()` — a real hyper HTTP/2 client driven through a WebSocket
//! handshake (`accept` / `accept_with`) into `serve_h2`, mirroring the Node e2e
//! entirely in Rust.
mod common;
use common::{app, connect_h2, get, handshake_status, start_server};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use h2ts_server::{serve_h2, AcceptOptions, WebSocketError};

#[tokio::test]
async fn h2_over_ws_full_roundtrip() {
    let addr = start_server().await;
    let (mut sender, negotiated) = connect_h2(addr, &["h2ts"]).await;

    assert_eq!(
        negotiated.as_deref(),
        Some("h2ts"),
        "accept() should echo the offered h2ts subprotocol"
    );

    // GET /hello
    let res = sender.send_request(get(addr, "/hello")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"hi")
    );

    // POST /echo
    let echo = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/echo"))
        .body(Full::new(Bytes::from_static(b"round-trips!")))
        .unwrap();
    let res = sender.send_request(echo).await.unwrap();
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"round-trips!")
    );

    // GET /big — 100 KiB, exercises multi-frame DATA + flow control.
    let res = sender.send_request(get(addr, "/big")).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), 100 * 1024);
    assert!(body.iter().all(|&b| b == b'x'));

    // 404
    let res = sender.send_request(get(addr, "/nope")).await.unwrap();
    assert_eq!(res.status(), 404);
}

#[tokio::test]
async fn h2_over_ws_concurrent_streams() {
    let addr = start_server().await;
    let (sender, _) = connect_h2(addr, &["h2ts"]).await;

    let mut handles = Vec::new();
    for _ in 0..16 {
        let mut s = sender.clone();
        handles.push(tokio::spawn(async move {
            let res = s.send_request(get(addr, "/hello")).await.unwrap();
            res.into_body().collect().await.unwrap().to_bytes()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), Bytes::from_static(b"hi"));
    }
}

/// `accept_with` sees the full offered subprotocol list and picks one.
#[tokio::test]
async fn accept_with_selects_from_offered_subprotocols() {
    // Handler echoes "chat" if offered; otherwise declines (which makes accept_with
    // fall back to h2ts when offered).
    async fn handler(
        mut req: Request<Incoming>,
    ) -> Result<Response<Empty<Bytes>>, WebSocketError> {
        let (response, ws_fut) = h2ts_server::accept_with(&mut req, |offered| {
            offered
                .iter()
                .find(|p| p.eq_ignore_ascii_case("chat"))
                .map(|p| p.to_string())
        })?;
        tokio::spawn(async move {
            if let Ok(ws) = ws_fut.await {
                let _ = serve_h2(ws, service_fn(app)).await;
            }
        });
        Ok(response)
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let io = TokioIo::new(socket);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(handler))
                    .with_upgrades()
                    .await;
            });
        }
    });

    // Offer ["chat", "h2ts"] — the handler prefers "chat".
    let (mut sender, negotiated) = connect_h2(addr, &["chat", "h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("chat"));

    let res = sender.send_request(get(addr, "/hello")).await.unwrap();
    assert_eq!(res.status(), 200);

    // Offering only h2ts: the handler declines "chat", accept_with falls back to h2ts.
    let (_sender, negotiated) = connect_h2(addr, &["h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"));
}

/// The default (strict) server rejects a client that doesn't offer `h2ts` with a
/// real `400` over the wire — not a dropped connection — while an `h2ts` client
/// is accepted (`101`).
#[tokio::test]
async fn strict_server_rejects_non_h2ts_over_the_wire() {
    let addr = start_server().await; // uses `accept` (requires h2ts)

    assert_eq!(
        handshake_status(addr, &["h2ts"]).await,
        StatusCode::SWITCHING_PROTOCOLS,
        "h2ts client is accepted"
    );
    assert_eq!(
        handshake_status(addr, &["mystery-codec"]).await,
        StatusCode::BAD_REQUEST,
        "a non-h2ts client is rejected"
    );
    assert_eq!(
        handshake_status(addr, &[]).await,
        StatusCode::BAD_REQUEST,
        "a client offering nothing is rejected"
    );
}

/// With `allow_implicit_codec`, the same client that would be *rejected* above is
/// instead accepted: its first offered codec is echoed and the tunnel works.
/// `h2ts` still wins when offered, and an empty offer completes with none.
#[tokio::test]
async fn allow_implicit_codec_accepts_any_offered_codec() {
    async fn handler(
        mut req: Request<Incoming>,
    ) -> Result<Response<Empty<Bytes>>, WebSocketError> {
        let (response, ws_fut) = match h2ts_server::accept_with_options(
            &mut req,
            |_offered| None,
            AcceptOptions {
                allow_implicit_codec: true,
            },
        ) {
            Ok(pair) => pair,
            Err(err) => return Ok(err.rejection_response()),
        };
        tokio::spawn(async move {
            if let Ok(ws) = ws_fut.await {
                let _ = serve_h2(ws, service_fn(app)).await;
            }
        });
        Ok(response)
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let io = TokioIo::new(socket);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(handler))
                    .with_upgrades()
                    .await;
            });
        }
    });

    // Unknown codec: accepted, its first offered codec echoed, tunnel works.
    let (mut sender, negotiated) = connect_h2(addr, &["mystery-codec", "other"]).await;
    assert_eq!(negotiated.as_deref(), Some("mystery-codec"));
    assert_eq!(sender.send_request(get(addr, "/hello")).await.unwrap().status(), 200);

    // h2ts still wins when offered.
    let (_sender, negotiated) = connect_h2(addr, &["chat", "h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"));

    // A client offering nothing completes with no subprotocol (permissive).
    let (_sender, negotiated) = connect_h2(addr, &[]).await;
    assert_eq!(negotiated, None);
}
