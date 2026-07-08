//! `bridge()` — the raw byte pump (wslay framing on the server side), and its
//! receive path. These drive wslay directly over an in-memory duplex, with a
//! fastwebsockets client on the other end, and never involve HTTP/2.
//!
//! The receive-path tests push large / fragmented / control frames at wslay,
//! which is the one place it must never buffer a whole frame.
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use h2ts_server::{bridge, bridge_with, BridgeConfig, CloseFrame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

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
    assert_eq!(
        got, expected,
        "every byte of the large frame must reach the peer, in order"
    );
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
    client_ws
        .write_frame(Frame::close(1000, b""))
        .await
        .unwrap();

    let mut buf = [0u8; 16];
    let n = peer_test.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "peer should observe EOF after the client close");
}

// --- Teardown reason surfacing -------------------------------------------
//
// Every close test above sends a proper Close frame (the `PeerClose` path). The
// two below cover the *abnormal* endings — a transport drop and a write failure —
// which are the common real-world teardowns and both surface as 1006.

/// The WS transport dying **without** a Close frame — a dropped TCP connection,
/// the common network-drop / killed-tab case — ends the bridge as *abnormal*:
/// `on_close` fires with 1006 (RFC 6455 §7.1.5), distinct from every clean close.
#[tokio::test]
async fn bridge_reports_1006_on_transport_drop_without_close() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    // Keep the peer bound so only the WS side ends the bridge.
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CloseFrame>();
    let config = BridgeConfig {
        on_close: Some(Box::new(move |cf: &CloseFrame| {
            let _ = tx.send(cf.clone());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    // Drop the client transport outright — no Close frame, just EOF.
    drop(client_io);

    let got = rx
        .recv()
        .await
        .expect("on_close should fire on abnormal end");
    assert_eq!(got.code, 1006, "abnormal closure code");
    assert!(got.reason.is_empty(), "no reason on an abnormal close");
}

/// A peer whose writes always fail and whose reads never complete. This forces the
/// bridge's *write-failure* teardown deterministically: a plain duplex signals
/// read-EOF and write-error together, racing the two teardown branches, so the
/// write-error path can't be isolated with one. Reads park forever (never EOF) so
/// only a write error can end the bridge.
struct FailingPeer;

impl AsyncRead for FailingPeer {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for FailingPeer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer write failed",
        )))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A write failure while forwarding data to the peer/upstream tears the bridge
/// down with the configured `error_close` (a distinct, informative close) rather
/// than a bare 1006 — here a proxy-style 1014 Bad Gateway.
#[tokio::test]
async fn bridge_reports_error_close_on_peer_write_failure() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CloseFrame>();
    let config = BridgeConfig {
        // Proxy-style: an upstream failure is a Bad Gateway.
        error_close: CloseFrame {
            code: 1014,
            reason: "bad gateway".to_string(),
        },
        on_close: Some(Box::new(move |cf: &CloseFrame| {
            let _ = tx.send(cf.clone());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, FailingPeer, config).await;
    });

    // Client sends data → the bridge decodes it and tries to write it to the peer,
    // whose write fails → the bridge tears down with the error close.
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws
        .write_frame(Frame::binary(Payload::Owned(b"trigger".to_vec())))
        .await
        .unwrap();

    let got = rx
        .recv()
        .await
        .expect("on_close should fire on write failure");
    assert_eq!(got.code, 1014, "a write failure uses error_close, not a bare 1006");
    assert_eq!(got.reason, "bad gateway");
}

/// A peer/upstream whose read errors (a reset), so `peer_to_ws` hits its error arm.
struct ReadErrPeer;

impl AsyncRead for ReadErrPeer {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "upstream reset",
        )))
    }
}

impl AsyncWrite for ReadErrPeer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A read error from the peer/upstream (a reset) tears down with `error_close`,
/// distinct from the clean `close` a plain EOF uses.
#[tokio::test]
async fn bridge_reports_error_close_on_peer_read_error() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CloseFrame>();
    let config = BridgeConfig {
        error_close: CloseFrame {
            code: 1014,
            reason: "bad gateway".to_string(),
        },
        keepalive: None,
        on_close: Some(Box::new(move |cf: &CloseFrame| {
            let _ = tx.send(cf.clone());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, ReadErrPeer, config).await;
    });

    // Keep the client side open (no data) so `peer_to_ws`'s immediate read error is
    // the branch that ends the bridge, not a client EOF.
    let _client_ws = WebSocket::after_handshake(client_io, Role::Client);

    let got = rx.recv().await.expect("on_close should fire on read error");
    assert_eq!(got.code, 1014, "a read error uses error_close, not the clean close");
    assert_eq!(got.reason, "bad gateway");
}
