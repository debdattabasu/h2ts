//! Shared test helpers. Lives in a `common/` subdir so it is compiled into each
//! test binary that needs it, not run as its own test binary.
//!
//! - `client_ws_stream` presents a fastwebsockets *client* WebSocket (dev-dep,
//!   standing in for the TS client) as a full-duplex byte stream — the mirror of
//!   the library's `WsByteStream`.
//! - `start_server` / `connect_h2` / `app` / `get` are the HTTP/2-over-WebSocket
//!   harness used by the `serve_h2` tests.
//!
//! Not every binary uses every helper, hence `allow(dead_code)`.
#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, Payload, WebSocket};
use http::header::{
    CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use h2ts_server::{accept, serve_h2, WebSocketError};

// --- Client-side WebSocket → byte-stream adapter -------------------------
//
// The server's `bridge`/wslay framing only speaks the *server* WebSocket role
// (it never masks). To drive it we need a *client* that masks its frames, which
// is exactly what fastwebsockets gives us.

/// Full-duplex pump between a fastwebsockets client WebSocket and a byte peer.
async fn client_bridge<S, P>(ws: WebSocket<S>, peer: P)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    P: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_read, ws_write) = ws.split(|s| tokio::io::split(s));
    let (mut peer_read, mut peer_write) = tokio::io::split(peer);
    let ws_write = Arc::new(Mutex::new(ws_write));

    let ws_to_peer = {
        let ws_write = ws_write.clone();
        async move {
            loop {
                let mut send_fn = |frame: Frame<'_>| {
                    let ws_write = ws_write.clone();
                    let opcode = frame.opcode;
                    let payload = frame.payload.to_vec();
                    async move {
                        ws_write
                            .lock()
                            .await
                            .write_frame(Frame::new(true, opcode, None, Payload::Owned(payload)))
                            .await
                    }
                };
                let frame = match ws_read.read_frame(&mut send_fn).await {
                    Ok(f) => f,
                    Err(_) => break,
                };
                match frame.opcode {
                    OpCode::Binary | OpCode::Text | OpCode::Continuation => {
                        if peer_write.write_all(&frame.payload).await.is_err() {
                            break;
                        }
                    }
                    OpCode::Close => break,
                    OpCode::Ping | OpCode::Pong => {}
                }
            }
            let _ = peer_write.shutdown().await;
        }
    };

    let peer_to_ws = {
        let ws_write = ws_write.clone();
        async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match peer_read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let frame = Frame::binary(Payload::Owned(buf[..n].to_vec()));
                        if ws_write.lock().await.write_frame(frame).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    };

    tokio::select! {
        _ = ws_to_peer => {}
        _ = peer_to_ws => {}
    }
}

/// A fastwebsockets client WebSocket presented as a raw byte duplex.
pub fn client_ws_stream<S>(ws: WebSocket<S>) -> DuplexStream
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (app_side, ws_side) = tokio::io::duplex(64 * 1024);
    tokio::spawn(client_bridge(ws, ws_side));
    app_side
}

// --- HTTP/2-over-WebSocket server harness --------------------------------

/// The example service served over the tunnel.
pub async fn app(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let resp = match path.as_str() {
        "/hello" => Response::new(Full::new(Bytes::from_static(b"hi"))),
        "/echo" => {
            let body = req.into_body().collect().await.unwrap().to_bytes();
            Response::new(Full::new(body))
        }
        "/big" => Response::new(Full::new(Bytes::from(vec![b'x'; 100 * 1024]))),
        _ => Response::builder()
            .status(404)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    };
    Ok(resp)
}

async fn upgrade_handler(
    mut req: Request<Incoming>,
) -> Result<Response<Empty<Bytes>>, WebSocketError> {
    // `accept` requires the h2ts subprotocol; a client without it is rejected
    // with a clean 4xx rather than a dropped connection.
    let (response, ws_fut) = match accept(&mut req) {
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

/// Start the WS→h2 server on an ephemeral port; returns its address.
pub async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let io = TokioIo::new(socket);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(upgrade_handler))
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}

/// Perform the WebSocket handshake and return an HTTP/2 client over the tunnel.
/// `offer` is the list of subprotocols to advertise (empty = none).
pub async fn connect_h2(
    addr: SocketAddr,
    offer: &[&str],
) -> (
    hyper::client::conn::http2::SendRequest<Full<Bytes>>,
    Option<String>,
) {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .header("Host", addr.to_string())
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(SEC_WEBSOCKET_KEY, fastwebsockets::handshake::generate_key())
        .header(SEC_WEBSOCKET_VERSION, "13");
    if !offer.is_empty() {
        builder = builder.header(SEC_WEBSOCKET_PROTOCOL, offer.join(", "));
    }
    let req = builder.body(Empty::<Bytes>::new()).unwrap();

    let (ws, resp) = fastwebsockets::handshake::client(&TokioExecutor::new(), req, tcp)
        .await
        .unwrap();
    let negotiated = resp
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let io = TokioIo::new(client_ws_stream(ws));
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    (sender, negotiated)
}

/// Perform *only* the WebSocket handshake over HTTP/1 and return the response
/// status — `101` on accept, a `4xx` on reject. Does not upgrade or run h2, so it
/// can observe a rejected handshake (which `connect_h2` would panic on).
pub async fn handshake_status(addr: SocketAddr, offer: &[&str]) -> hyper::StatusCode {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .header("Host", addr.to_string())
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(SEC_WEBSOCKET_KEY, fastwebsockets::handshake::generate_key())
        .header(SEC_WEBSOCKET_VERSION, "13");
    if !offer.is_empty() {
        builder = builder.header(SEC_WEBSOCKET_PROTOCOL, offer.join(", "));
    }
    let req = builder.body(Empty::<Bytes>::new()).unwrap();
    sender.send_request(req).await.unwrap().status()
}

/// A simple GET request over the tunnel.
pub fn get(addr: SocketAddr, path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://{addr}{path}"))
        .body(Full::new(Bytes::new()))
        .unwrap()
}
