//! Control-frame surfacing + sending (`BridgeConfig` / `WsControl`) and
//! server-initiated keepalive (`KeepAlive`).
use std::time::Duration;

use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use h2ts_server::{bridge_with, control_channel, BridgeConfig, CloseFrame, KeepAlive};
use tokio::io::AsyncWriteExt;

// --- Surfacing + sending -------------------------------------------------

/// The peer's close code and reason are surfaced via `on_close`.
#[tokio::test]
async fn bridge_surfaces_client_close_reason() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
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

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws
        .write_frame(Frame::close(4000, b"bye"))
        .await
        .unwrap();

    let got = rx.recv().await.unwrap();
    assert_eq!(
        got,
        CloseFrame {
            code: 4000,
            reason: "bye".to_string()
        }
    );
}

/// A received Ping is surfaced via `on_ping`, and wslay still auto-answers it.
#[tokio::test]
async fn bridge_surfaces_ping_and_still_auto_pongs() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let config = BridgeConfig {
        on_ping: Some(Box::new(move |p: &[u8]| {
            let _ = tx.send(p.to_vec());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws
        .write_frame(Frame::new(
            true,
            OpCode::Ping,
            None,
            Payload::Owned(b"hi".to_vec()),
        ))
        .await
        .unwrap();

    assert_eq!(rx.recv().await.unwrap(), b"hi".to_vec(), "on_ping saw it");
    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Pong, "auto-pong still happens");
    assert_eq!(frame.payload.to_vec(), b"hi".to_vec());
}

/// The close code+reason configured in `BridgeConfig::close` is sent when the
/// peer reaches EOF.
#[tokio::test]
async fn bridge_sends_configured_close_on_peer_eof() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, peer_test) = tokio::io::duplex(16 * 1024);

    let config = BridgeConfig {
        close: CloseFrame {
            code: 4001,
            reason: "upstream gone".to_string(),
        },
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    // We only want to observe the close frame; don't auto-reply (the bridge tears
    // down right after sending it, so a reply write would race the teardown).
    client_ws.set_auto_close(false);
    drop(peer_test); // peer EOF -> the bridge sends the configured close

    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Close);
    let payload = frame.payload.to_vec();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 4001);
    assert_eq!(&payload[2..], b"upstream gone");
}

/// A `WsControl` handle can inject a Ping and a Close into a running bridge.
#[tokio::test]
async fn control_handle_sends_ping_and_close() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    let (control, control_rx) = control_channel();
    let config = BridgeConfig {
        control: Some(control_rx),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    // Observe control frames as-is: disable the client's auto ping/close handling
    // (auto-pong would consume the incoming ping instead of returning it).
    client_ws.set_auto_pong(false);
    client_ws.set_auto_close(false);

    // Server-initiated ping reaches the client.
    control.ping(b"ping!".to_vec()).unwrap();
    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Ping);
    assert_eq!(frame.payload.to_vec(), b"ping!".to_vec());

    // Server-initiated close reaches the client with its code + reason.
    control.close(4003, "done").unwrap();
    let frame = client_ws.read_frame().await.unwrap();
    assert_eq!(frame.opcode, OpCode::Close);
    let payload = frame.payload.to_vec();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 4003);
    assert_eq!(&payload[2..], b"done");
}

/// `WsControl::pong` sends an unsolicited pong to the client, and `on_pong`
/// surfaces the pong the client sends back in answer to a server ping.
#[tokio::test]
async fn control_sends_pong_and_on_pong_surfaces_received_pong() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    let (control, control_rx) = control_channel();
    let (pong_tx, mut pong_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let config = BridgeConfig {
        control: Some(control_rx),
        on_pong: Some(Box::new(move |p: &[u8]| {
            let _ = pong_tx.send(p.to_vec());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    // A default client (auto-answers pings). Its read loop forwards any Pong it
    // *receives*, and auto-pongs any Ping it receives.
    let (client_saw_tx, mut client_saw_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
        while let Ok(f) = client_ws.read_frame().await {
            match f.opcode {
                OpCode::Pong => {
                    let _ = client_saw_tx.send(f.payload.to_vec());
                }
                OpCode::Close => break,
                _ => {} // Ping is auto-answered internally
            }
        }
    });

    // 1) WsControl::pong -> the client receives an unsolicited Pong.
    control.pong(b"unsolicited".to_vec()).unwrap();
    assert_eq!(client_saw_rx.recv().await.unwrap(), b"unsolicited".to_vec());

    // 2) WsControl::ping -> the client auto-answers -> our on_pong fires with the
    //    echoed payload (the round trip a keepalive relies on).
    control.ping(b"rtt-probe".to_vec()).unwrap();
    assert_eq!(pong_rx.recv().await.unwrap(), b"rtt-probe".to_vec());
}

// --- Keepalive (server-initiated ping + timeout) --------------------------

/// A peer that never answers the keepalive ping is disconnected, and the reason
/// is both sent to the client and surfaced to `on_close`.
#[tokio::test]
async fn keepalive_closes_peer_on_no_pong() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, _peer_test) = tokio::io::duplex(16 * 1024);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CloseFrame>();
    let config = BridgeConfig {
        keepalive: Some(KeepAlive::new(
            Duration::from_millis(50),
            Duration::from_millis(50),
        )),
        on_close: Some(Box::new(move |cf: &CloseFrame| {
            let _ = tx.send(cf.clone());
        })),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    // A client that ignores pings entirely (never pongs).
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    client_ws.set_auto_pong(false);
    client_ws.set_auto_close(false);

    // First the keepalive Ping arrives; then, with no pong, the keepalive Close.
    let ping = client_ws.read_frame().await.unwrap();
    assert_eq!(ping.opcode, OpCode::Ping);
    let close = client_ws.read_frame().await.unwrap();
    assert_eq!(close.opcode, OpCode::Close);
    let payload = close.payload.to_vec();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001); // Going Away
    assert_eq!(&payload[2..], b"keepalive timeout");

    // The backend learns why it closed, with the same reason.
    let surfaced = rx.recv().await.unwrap();
    assert_eq!(surfaced.code, 1001);
    assert_eq!(surfaced.reason, "keepalive timeout");
}

/// A peer that auto-answers pings keeps the tunnel alive across many keepalive
/// intervals — data still flows and no close is sent.
#[tokio::test]
async fn keepalive_stays_up_while_peer_responds() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(16 * 1024);

    let config = BridgeConfig {
        keepalive: Some(KeepAlive::new(
            Duration::from_millis(30),
            Duration::from_millis(30),
        )),
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = bridge_with(server_io, peer_for_bridge, config).await;
    });

    // Peer sends data only after several keepalive intervals have elapsed.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await; // 5x interval
        let _ = peer_test.write_all(b"still-alive").await;
        tokio::time::sleep(Duration::from_millis(200)).await; // keep peer open
    });

    // Default client auto-answers pings. read_frame answers them internally and
    // returns once the peer's data arrives — proving keepalive didn't kill a
    // healthy connection.
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
    let frame = tokio::time::timeout(Duration::from_secs(2), client_ws.read_frame())
        .await
        .expect("timed out — keepalive may have closed a healthy connection")
        .unwrap();
    assert_eq!(
        frame.opcode,
        OpCode::Binary,
        "expected peer data, got {:?}",
        frame.opcode
    );
    assert_eq!(frame.payload.to_vec(), b"still-alive".to_vec());
}
