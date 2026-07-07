//! `serve_h2_with()` — HTTP/2 over the tunnel composed with a live bridge
//! configuration (keepalive + hooks). This is the one path that runs a real h2
//! service *and* the control-frame machinery together; `serve_h2.rs` only ever
//! drives bare `serve_h2` (default config), and `control_frames.rs` only drives
//! the bare bridge with a dumb byte peer. The property that matters here — and
//! that neither of those covers — is that server keepalive Pings interleave with
//! real h2 DATA without corrupting the tunnel, and that `on_close` fires when the
//! connection ends.
mod common;
use common::{app, connect_h2, get};

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use h2ts_server::{accept, serve_h2_with, BridgeConfig, CloseFrame, KeepAlive, WebSocketError};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// h2 traffic round-trips byte-exact while a fast server keepalive pings in the
/// background, and `on_close` fires once the connection ends — exercising the
/// `serve_h2_with` compose path (h2 + keepalive + hook) that plain `serve_h2`
/// never touches.
#[tokio::test]
async fn serve_h2_with_pings_alongside_h2_and_fires_close_hook() {
    let (close_tx, mut close_rx) = mpsc::unbounded_channel::<CloseFrame>();
    let close_tx = Arc::new(close_tx); // shared across accepted connections

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let close_tx = close_tx.clone();
            tokio::spawn(async move {
                // Per-request handler: WS upgrade, then serve h2 over the tunnel
                // with a keepalive that pings every 5ms and a close hook.
                let handler = move |mut req: Request<Incoming>| {
                    let close_tx = close_tx.clone();
                    async move {
                        let (response, ws_fut) = match accept(&mut req) {
                            Ok(pair) => pair,
                            Err(err) => return Ok::<_, WebSocketError>(err.rejection_response()),
                        };
                        tokio::spawn(async move {
                            if let Ok(ws) = ws_fut.await {
                                let config = BridgeConfig {
                                    // Ping aggressively so pings certainly land
                                    // mid-transfer; generous timeout so a healthy
                                    // (auto-ponging) client is never killed.
                                    keepalive: Some(KeepAlive::new(
                                        Duration::from_millis(5),
                                        Duration::from_secs(5),
                                    )),
                                    on_close: Some(Box::new(move |cf: &CloseFrame| {
                                        let _ = close_tx.send(cf.clone());
                                    })),
                                    ..Default::default()
                                };
                                let _ = serve_h2_with(ws, service_fn(app), config).await;
                            }
                        });
                        Ok(response)
                    }
                };
                let io = TokioIo::new(socket);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(handler))
                    .with_upgrades()
                    .await;
            });
        }
    });

    let (mut sender, negotiated) = connect_h2(addr, &["h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"));

    // Real h2 traffic while keepalive Pings fly every 5ms. If a Ping corrupted the
    // byte stream, the h2 layer would error or the payload would mismatch.
    for _ in 0..3 {
        // /big = 100 KiB, multi-frame DATA + flow control — long enough that
        // several keepalive pings certainly land mid-transfer.
        let res = sender.send_request(get(addr, "/big")).await.unwrap();
        assert_eq!(res.status(), 200);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), 100 * 1024);
        assert!(
            body.iter().all(|&b| b == b'x'),
            "a keepalive ping corrupted the h2 DATA stream"
        );

        // A sizable echo round-trip in the other direction, too.
        let payload = Bytes::from(vec![b'z'; 32 * 1024]);
        let echo = Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/echo"))
            .body(Full::new(payload.clone()))
            .unwrap();
        let res = sender.send_request(echo).await.unwrap();
        assert_eq!(res.into_body().collect().await.unwrap().to_bytes(), payload);

        tokio::time::sleep(Duration::from_millis(15)).await; // let a few pings fire
    }

    // Drop the client → the h2 connection ends → serve_h2_with returns → the
    // bridge tears down and the close hook fires. Asserting it fires (not an exact
    // code, which depends on local-close vs abnormal teardown ordering) is the
    // point: the hook is wired through the compose path end-to-end.
    drop(sender);
    let _closed = tokio::time::timeout(Duration::from_secs(5), close_rx.recv())
        .await
        .expect("on_close never fired after the connection ended")
        .expect("close channel dropped without delivering a close frame");
}
