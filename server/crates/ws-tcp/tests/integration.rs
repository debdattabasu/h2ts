//! Native Rust tests for the ws-tcp library — no Node client involved.
//!
//! - `bridge` / `WsByteStream` are exercised directly over an in-memory duplex.
//!   The server side is the real thing (wslay framing); the client side is a
//!   fastwebsockets *dev-dependency* standing in for the TS client.
//! - The flagship test drives a real hyper HTTP/2 client through a WebSocket
//!   handshake (`accept`) into `serve_h2`, mirroring the Node e2e entirely in
//!   Rust.
//! - The `bridge_*` receive-path tests push large / fragmented / control frames
//!   at wslay directly, which is the one place it must never buffer a whole frame.
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
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
use ws_tcp::{accept, bridge, serve_h2, WebSocketError, WsByteStream};

// --- Client-side helper --------------------------------------------------
//
// The server's `bridge`/wslay framing only speaks the *server* WebSocket role
// (it never masks). To drive it we need a *client* that masks its frames, which
// is exactly what fastwebsockets gives us — used here as a dev-dependency only.
// `client_ws_stream` mirrors the library's `WsByteStream` on the client side:
// present a fastwebsockets client WebSocket as a full-duplex byte stream so a
// hyper h2 client can run over the tunnel.

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
fn client_ws_stream<S>(ws: WebSocket<S>) -> DuplexStream
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (app_side, ws_side) = tokio::io::duplex(64 * 1024);
    tokio::spawn(client_bridge(ws, ws_side));
    app_side
}

// --- bridge(): raw byte pump (wslay framing on the server side) ----------

#[tokio::test]
async fn bridge_forwards_bytes_both_directions() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    // Server side: the raw stream straight into the wslay bridge.
    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
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

    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
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

// --- bridge() receive path: wslay must never buffer a whole frame --------
//
// The h2 round-trips below only push large payloads peer->WS (the wslay *send*
// path). These drive the incremental *receive* state machine directly: a client
// WS frame -> wslay decode -> peer bytes.

/// A single inbound frame far larger than wslay's 64 KiB read chunk must be
/// streamed across many socket reads — exercising the recv WOULDBLOCK + resume
/// loop, client->server unmasking, and delivery to the peer without ever
/// buffering the whole frame. This is the property wslay exists for.
#[tokio::test]
async fn bridge_reassembles_a_large_inbound_frame() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    // 256 KiB = 4x the read chunk, sent as ONE WebSocket binary frame. The 16 KiB
    // duplex forces it to arrive in many small reads, so wslay must resume a
    // partial frame repeatedly.
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    tokio::spawn(async move {
        client_ws
            .write_frame(Frame::binary(Payload::Owned(payload)))
            .await
            .unwrap();
    });

    let mut got = vec![0u8; expected.len()];
    peer_test.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected, "every byte of the large frame must reach the peer, in order");
}

/// Several complete frames can land in a single read. wslay must decode all of
/// them in one recv pass and forward their payloads concatenated, in order.
#[tokio::test]
async fn bridge_handles_many_frames_in_one_read() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(64 * 1024);

    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    let mut expected = Vec::new();
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    for i in 0..64u32 {
        let msg = format!("frame-{i:04}-");
        expected.extend_from_slice(msg.as_bytes());
        client_ws
            .write_frame(Frame::binary(Payload::Owned(msg.into_bytes())))
            .await
            .unwrap();
    }

    let mut got = vec![0u8; expected.len()];
    peer_test.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected, "all frames forwarded once, in order");
}

/// wslay auto-answers a ping with a pong carrying the same payload, and does NOT
/// leak the control-frame payload to the peer.
#[tokio::test]
async fn bridge_answers_ping_with_pong() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    // Keep `_peer_test` bound so the peer side never EOFs and tears the bridge
    // down before the ping is handled.
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws
        .write_frame(Frame::new(
            true,
            OpCode::Ping,
            None,
            Payload::Owned(b"ping-payload".to_vec()),
        ))
        .await
        .unwrap();

    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Pong);
    assert_eq!(frame.payload.to_vec(), b"ping-payload".to_vec());
}

/// A client-initiated close travels through wslay and shuts down the peer, which
/// observes EOF.
#[tokio::test]
async fn bridge_propagates_client_close_to_peer() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws.write_frame(Frame::close(1000, b"")).await.unwrap();

    let mut buf = [0u8; 16];
    let n = peer_test.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "peer should observe EOF after the client close");
}

// --- WsByteStream --------------------------------------------------------

#[tokio::test]
async fn byte_stream_reads_and_writes() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
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

    let io = TokioIo::new(client_ws_stream(ws));
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
