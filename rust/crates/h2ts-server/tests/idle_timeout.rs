//! HTTP/2 idle-TTL: reap a *healthy but idle* connection (no open streams for
//! `idle_timeout`) even while the client keeps answering keepalive pings — the
//! case keepalive alone does not handle. Mirrors the Go server's
//! `TestServeH2IdleTimeoutReapsHealthyConnection`.
mod common;
use common::{app, connect_h2, get};

use std::time::Duration;

use bytes::Bytes;
use h2ts_server::{
    accept, serve_h2_with_config, BridgeConfig, KeepAlive, ServeConfig, WebSocketError,
};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// Accept the WebSocket, then serve with keepalive ON (so the client stays
/// "alive" by answering pings) and a short idle TTL (so it's reaped anyway).
async fn handler(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, WebSocketError> {
    let (response, ws_fut) = match accept(&mut req) {
        Ok(pair) => pair,
        Err(err) => return Ok(err.rejection_response()),
    };
    tokio::spawn(async move {
        if let Ok(ws) = ws_fut.await {
            let config = ServeConfig {
                bridge: BridgeConfig {
                    keepalive: Some(KeepAlive::new(
                        Duration::from_millis(50),
                        Duration::from_millis(50),
                    )),
                    ..Default::default()
                },
                idle_timeout: Some(Duration::from_millis(100)),
            };
            let _ = serve_h2_with_config(ws, service_fn(app), config).await;
        }
    });
    Ok(response)
}

#[tokio::test]
async fn idle_timeout_reaps_a_healthy_but_idle_connection() {
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

    let (mut sender, negotiated) = connect_h2(addr, &["h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"));

    // One request; fully drain the response so the stream closes (open count -> 0).
    let res = sender.send_request(get(addr, "/hello")).await.unwrap();
    assert_eq!(res.status(), 200);
    let _ = res.into_body().collect().await.unwrap();

    // The client keeps auto-answering keepalive pings across this window, but with
    // no streams open the idle TTL reaps the connection anyway.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The connection is gone: a new request fails.
    assert!(
        sender.send_request(get(addr, "/hello")).await.is_err(),
        "idle timeout should have reaped the healthy-but-idle connection"
    );
}
