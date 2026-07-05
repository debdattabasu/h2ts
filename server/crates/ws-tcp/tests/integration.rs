//! Native Rust tests for the ws-tcp library — no Node client involved.
//!
//! - `bridge` / `WsByteStream` are exercised directly over an in-memory duplex,
//!   with a fastwebsockets client on the other end.
//! - The flagship test drives a real hyper HTTP/2 client through a WebSocket
//!   handshake (`accept`) into `serve_h2`, mirroring the Node e2e entirely in
//!   Rust.
use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use fastwebsockets::{Frame, Payload, Role, WebSocket};
use http::header::{
    CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use ws_tcp::{accept, bridge, serve_h2, WebSocketError, WsByteStream};

// --- bridge() ------------------------------------------------------------

#[tokio::test]
async fn bridge_forwards_bytes_both_directions() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    let server_ws = WebSocket::after_handshake(server_io, Role::Server);
    tokio::spawn(async move {
        let _ = bridge(server_ws, peer_for_bridge).await;
    });
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    // A client WS frame becomes peer bytes.
    client_ws
        .write_frame(Frame::binary(Payload::Owned(b"hello".to_vec())))
        .await
        .unwrap();
    let mut buf = [0u8; 5];
    peer_test.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    // Peer bytes become a binary WS frame.
    peer_test.write_all(b"world").await.unwrap();
    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.payload.to_vec(), b"world".to_vec());
}

#[tokio::test]
async fn bridge_streams_a_large_payload() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    let server_ws = WebSocket::after_handshake(server_io, Role::Server);
    tokio::spawn(async move {
        let _ = bridge(server_ws, peer_for_bridge).await;
    });

    // 512 KiB from peer -> client, larger than the duplex buffer, so it must
    // stream across many WS frames without deadlocking.
    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    tokio::spawn(async move {
        peer_test.write_all(&payload).await.unwrap();
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    let mut got = Vec::new();
    while got.len() < expected.len() {
        let frame = client_ws.read_frame().await.unwrap();
        got.extend_from_slice(&frame.payload);
    }
    assert_eq!(got, expected);
}

// --- WsByteStream --------------------------------------------------------

#[tokio::test]
async fn byte_stream_reads_and_writes() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let server_ws = WebSocket::after_handshake(server_io, Role::Server);
    let mut stream = WsByteStream::from_websocket(server_ws);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    client_ws
        .write_frame(Frame::binary(Payload::Owned(b"ping".to_vec())))
        .await
        .unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    stream.write_all(b"pong").await.unwrap();
    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.payload.to_vec(), b"pong".to_vec());
}

// --- serve_h2(): full HTTP/2-over-WebSocket round trip -------------------

async fn app(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
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
    let (response, ws_fut) = accept(&mut req)?;
    tokio::spawn(async move {
        if let Ok(ws) = ws_fut.await {
            let _ = serve_h2(ws, service_fn(app)).await;
        }
    });
    Ok(response)
}

/// Start the WS→h2 server on an ephemeral port; returns its address.
async fn start_server() -> SocketAddr {
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
async fn connect_h2(
    addr: SocketAddr,
    offer_binary: bool,
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
    if offer_binary {
        builder = builder.header(SEC_WEBSOCKET_PROTOCOL, "binary");
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

    let io = TokioIo::new(WsByteStream::from_websocket(ws));
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    (sender, negotiated)
}

fn get(addr: SocketAddr, path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://{addr}{path}"))
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[tokio::test]
async fn h2_over_ws_full_roundtrip() {
    let addr = start_server().await;
    let (mut sender, negotiated) = connect_h2(addr, true).await;

    assert_eq!(
        negotiated.as_deref(),
        Some("binary"),
        "accept() should echo the offered binary subprotocol"
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
    let (sender, _) = connect_h2(addr, false).await;

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

// --- Same round trip, but framed by the wslay backend (feature `wslay`) -------

#[cfg(feature = "wslay")]
mod wslay_backend {
    use super::*;
    use ws_tcp::wslay_serve_h2;

    async fn upgrade_handler(
        mut req: Request<Incoming>,
    ) -> Result<Response<Empty<Bytes>>, WebSocketError> {
        let (response, ws_fut) = accept(&mut req)?;
        tokio::spawn(async move {
            if let Ok(ws) = ws_fut.await {
                let _ = wslay_serve_h2(ws, service_fn(app)).await;
            }
        });
        Ok(response)
    }

    async fn start_wslay_server() -> SocketAddr {
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

    #[tokio::test]
    async fn h2_over_ws_via_wslay_roundtrip() {
        let addr = start_wslay_server().await;
        let (mut sender, negotiated) = connect_h2(addr, true).await;
        assert_eq!(negotiated.as_deref(), Some("binary"));

        let res = sender.send_request(get(addr, "/hello")).await.unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"hi")
        );

        let echo = Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/echo"))
            .body(Full::new(Bytes::from_static(b"wslay-round-trips!")))
            .unwrap();
        let res = sender.send_request(echo).await.unwrap();
        assert_eq!(
            res.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"wslay-round-trips!")
        );

        // 100 KiB — streamed incrementally across many frames through wslay.
        let res = sender.send_request(get(addr, "/big")).await.unwrap();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), 100 * 1024);
        assert!(body.iter().all(|&b| b == b'x'));
    }

    #[tokio::test]
    async fn h2_over_ws_via_wslay_concurrent() {
        let addr = start_wslay_server().await;
        let (sender, _) = connect_h2(addr, false).await;
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
}
