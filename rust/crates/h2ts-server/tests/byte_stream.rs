//! `WsByteStream` — a WebSocket presented as `AsyncRead + AsyncWrite`. These
//! isolate the adapter's contract: streaming, backpressure, full-duplex, and the
//! two teardown directions (peer-close → EOF, shutdown → WS close).
mod common;
use common::client_ws_stream;

use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use h2ts_server::{BridgeConfig, CloseFrame, WsByteStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// A large app-side write emerges on the WebSocket as binary frames, byte-exact
/// — exercising the write path through the duplex + bridge + wslay framing under
/// backpressure (payload >> the 64 KiB internal duplex).
#[tokio::test]
async fn byte_stream_streams_large_write_to_ws() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    tokio::spawn(async move {
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
    });

    let mut got = Vec::new();
    while got.len() < expected.len() {
        let frame = client_ws.read_frame().await.unwrap();
        match frame.opcode {
            OpCode::Binary | OpCode::Continuation => got.extend_from_slice(&frame.payload),
            OpCode::Close => break,
            _ => {}
        }
    }
    assert_eq!(got, expected);
}

/// A large inbound WS frame is delivered to the reader in full, byte-exact.
#[tokio::test]
async fn byte_stream_reads_large_ws_frame() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    tokio::spawn(async move {
        client_ws
            .write_frame(Frame::binary(Payload::Owned(payload)))
            .await
            .unwrap();
    });

    let mut got = vec![0u8; expected.len()];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected);
}

/// Read and write stream in both directions at once without deadlocking — the
/// adapter is genuinely full-duplex. Uses `client_ws_stream` so both ends are
/// byte streams, like two TCP sockets over the tunnel.
#[tokio::test]
async fn byte_stream_full_duplex() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let server = WsByteStream::new(server_io);
    let client = client_ws_stream(WebSocket::after_handshake(client_io, Role::Client));

    let (mut sr, mut sw) = tokio::io::split(server);
    let (mut cr, mut cw) = tokio::io::split(client);

    let up: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let down: Vec<u8> = (0..200 * 1024).map(|i| (i % 241) as u8).collect();
    let up_expected = up.clone();
    let down_expected = down.clone();
    let (up_len, down_len) = (up.len(), down.len());

    let w1 = tokio::spawn(async move { sw.write_all(&up).await.unwrap() });
    let w2 = tokio::spawn(async move { cw.write_all(&down).await.unwrap() });
    let r1 = tokio::spawn(async move {
        let mut g = vec![0u8; up_len];
        cr.read_exact(&mut g).await.unwrap();
        g
    });
    let r2 = tokio::spawn(async move {
        let mut g = vec![0u8; down_len];
        sr.read_exact(&mut g).await.unwrap();
        g
    });

    w1.await.unwrap();
    w2.await.unwrap();
    assert_eq!(
        r1.await.unwrap(),
        up_expected,
        "server->client stream intact"
    );
    assert_eq!(
        r2.await.unwrap(),
        down_expected,
        "client->server stream intact"
    );
}

/// The reader sees a continuous byte stream: WS frame boundaries are invisible,
/// and reads smaller than a frame work.
#[tokio::test]
async fn byte_stream_reads_are_frame_agnostic() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    let parts: [&[u8]; 5] = [b"aa", b"bbb", b"c", b"dddd", b"ee"];
    let expected: Vec<u8> = parts.concat();
    tokio::spawn(async move {
        for p in parts {
            client_ws
                .write_frame(Frame::binary(Payload::Owned(p.to_vec())))
                .await
                .unwrap();
        }
    });

    // Read 3 bytes at a time across the 5 frames.
    let mut got = Vec::new();
    let mut buf = [0u8; 3];
    while got.len() < expected.len() {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "unexpected EOF mid-stream");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, expected);
}

/// A Close from the peer surfaces as EOF on the byte stream.
#[tokio::test]
async fn byte_stream_eof_on_peer_close() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    client_ws
        .write_frame(Frame::close(1000, b""))
        .await
        .unwrap();

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "byte stream should hit EOF after the peer's close");
}

/// Shutting down the byte stream closes the WebSocket peer.
#[tokio::test]
async fn byte_stream_shutdown_closes_peer() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let mut stream = WsByteStream::new(server_io);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws.set_auto_close(false);

    stream.shutdown().await.unwrap();

    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Close);
    let payload = frame.payload.to_vec();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1000);
}

/// `WsByteStream::with_config` threads the `BridgeConfig` through to the bridge:
/// the peer's close reason is surfaced to `on_close`.
#[tokio::test]
async fn byte_stream_with_config_surfaces_close() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CloseFrame>();
    let config = BridgeConfig {
        on_close: Some(Box::new(move |cf: &CloseFrame| {
            let _ = tx.send(cf.clone());
        })),
        ..Default::default()
    };
    let mut stream = WsByteStream::with_config(server_io, config);
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    client_ws
        .write_frame(Frame::close(4010, b"client-gone"))
        .await
        .unwrap();

    let surfaced = rx.recv().await.unwrap();
    assert_eq!(surfaced.code, 4010);
    assert_eq!(surfaced.reason, "client-gone");

    let mut buf = [0u8; 8];
    assert_eq!(stream.read(&mut buf).await.unwrap(), 0, "stream at EOF too");
}
