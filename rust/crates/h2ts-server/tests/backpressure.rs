//! Backpressure: the bridge must **propagate** backpressure end to end, not absorb
//! an unbounded amount when the far consumer stalls. The other bridge tests assert
//! byte-exactness under normal flow; these assert the *liveness* property — a
//! producer stays blocked while its consumer is paused (so memory can't grow without
//! bound), then the whole stream drains byte-exact once the consumer resumes.
//!
//! The check is `JoinHandle::is_finished()` after letting the pipeline fill: if the
//! bridge buffered everything in memory, the producer's write would complete
//! immediately (finished == true) even with nobody reading. Backpressure means it
//! does NOT finish until the consumer drains.
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use h2ts_server::bridge;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};

/// Small duplex buffers on both sides, so the total buffering in the path
/// (~two duplexes + wslay's read-chunk + one framed chunk ≈ 160 KiB) is far below
/// the payload — the producer must block long before finishing.
const BUF: usize = 16 * 1024;
const BIG: usize = 1024 * 1024; // 1 MiB

/// peer -> WS: a fast peer producer stays blocked while the WS consumer is paused,
/// then every byte arrives in order once the consumer drains.
#[tokio::test]
async fn peer_to_ws_producer_blocks_until_consumer_drains() {
    let (client_io, server_io) = tokio::io::duplex(BUF);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(BUF);
    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    // The WS consumer starts PAUSED — we don't read any frames yet.
    let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);

    let payload: Vec<u8> = (0..BIG).map(|i| (i & 0xff) as u8).collect();
    let expected = payload.clone();
    // Shove the whole payload at the peer side; return the handle so the peer end
    // stays alive (no premature EOF) until we've drained.
    let writer = tokio::spawn(async move {
        peer_test.write_all(&payload).await.unwrap();
        peer_test
    });

    // With nobody reading the WS side, the pipeline fills and the writer blocks well
    // before finishing — backpressure, not unbounded buffering.
    sleep(Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "producer finished with the consumer paused — the bridge buffered unboundedly instead of applying backpressure"
    );

    // Drain the WS side; the producer now completes and every byte arrives in order.
    let mut got = Vec::with_capacity(expected.len());
    while got.len() < expected.len() {
        let frame = client_ws.read_frame().await.unwrap();
        match frame.opcode {
            OpCode::Binary | OpCode::Continuation => got.extend_from_slice(&frame.payload),
            OpCode::Close => break,
            _ => {}
        }
    }
    let _peer_test = writer.await.unwrap();
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected, "a backpressured stream must stay byte-exact");
}

/// WS -> peer: a fast WS producer stays blocked while the peer consumer is paused,
/// then every byte arrives in order once the peer drains.
#[tokio::test]
async fn ws_to_peer_producer_blocks_until_consumer_drains() {
    let (client_io, server_io) = tokio::io::duplex(BUF);
    let (peer_for_bridge, mut peer_test) = tokio::io::duplex(BUF);
    tokio::spawn(async move {
        let _ = bridge(server_io, peer_for_bridge).await;
    });

    let payload: Vec<u8> = (0..BIG).map(|i| (i & 0xff) as u8).collect();
    let expected = payload.clone();

    // WS producer: send the whole payload as one big binary frame. With the peer
    // consumer paused, the bridge stops reading the WS side and write_frame blocks.
    let writer = tokio::spawn(async move {
        let mut client_ws = WebSocket::after_handshake(client_io, Role::Client);
        client_ws
            .write_frame(Frame::binary(Payload::Owned(payload)))
            .await
            .unwrap();
        client_ws // keep the WS end alive
    });

    // peer_test (consumer) is paused: the producer must not finish.
    sleep(Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "WS producer finished with the peer paused — no backpressure"
    );

    // Drain the peer; the producer completes and the bytes match.
    let mut got = vec![0u8; expected.len()];
    peer_test.read_exact(&mut got).await.unwrap();
    let _client_ws = writer.await.unwrap();
    assert_eq!(got, expected, "a backpressured stream must stay byte-exact");
}
