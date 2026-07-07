//! HTTP/2 connection tests — prior-knowledge startup, request/response, PING RTT,
//! and the streaming story (upload streaming, flow control, bidirectional
//! ordering, incremental download). Drives the public client over an in-memory
//! transport, playing the server side by hand.

use std::task::Poll;

use futures::channel::{mpsc, oneshot};
use futures::executor::LocalPool;
use futures::future::poll_fn;
use futures::stream;
use futures::task::LocalSpawnExt;
use futures::{SinkExt, StreamExt};

use h2ts_client::frames::{serialize_frame, Frame, FrameDecoder, Settings};
use h2ts_client::hpack::{Header, HpackEncoder};
use h2ts_client::{connect, ConnectOptions, RequestBody, RequestInit, Transport, TransportError};

/// The HTTP/2 client connection preface (RFC 7540 §3.5).
const CONNECTION_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// The spec-default initial flow-control window (§6.9.2).
const SPEC_INITIAL_WINDOW: i64 = 65535;

/// An in-memory transport; returns the client's outbound-read end and the
/// inbound-write end for the test to drive the "server" side.
fn mock_transport() -> (
    Transport,
    mpsc::UnboundedReceiver<Vec<u8>>,
    mpsc::UnboundedSender<Vec<u8>>,
) {
    let (c2s_tx, c2s_rx) = mpsc::unbounded::<Vec<u8>>(); // client -> test
    let (s2c_tx, s2c_rx) = mpsc::unbounded::<Vec<u8>>(); // test -> client
    let writer = Box::pin(c2s_tx.sink_map_err(|e| TransportError(e.to_string())));
    let reader = Box::pin(s2c_rx);
    (Transport::new(reader, writer), c2s_rx, s2c_tx)
}

/// Read the client's opening bytes: the 24-byte preface, then `want` frames.
async fn read_startup(c2s_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>, want: usize) -> Vec<Frame> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < CONNECTION_PREFACE.len() {
        buf.extend(c2s_rx.next().await.expect("client closed before preface"));
    }
    assert_eq!(&buf[..CONNECTION_PREFACE.len()], CONNECTION_PREFACE);
    let mut dec = FrameDecoder::default();
    let mut frames = dec.push(&buf[CONNECTION_PREFACE.len()..]).unwrap();
    while frames.len() < want {
        let chunk = c2s_rx.next().await.expect("client closed before frames");
        frames.extend(dec.push(&chunk).unwrap());
    }
    frames
}

/// The peer side of a mock transport: buffers and decodes the frames the client
/// writes, so a test can assert on them one at a time.
struct ServerSide {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    dec: FrameDecoder,
    queue: std::collections::VecDeque<Frame>,
}

impl ServerSide {
    fn new(rx: mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx,
            dec: FrameDecoder::default(),
            queue: std::collections::VecDeque::new(),
        }
    }

    /// Consume and verify the 24-byte connection preface.
    async fn read_preface(&mut self) {
        let mut buf: Vec<u8> = Vec::new();
        while buf.len() < CONNECTION_PREFACE.len() {
            buf.extend(self.rx.next().await.expect("client closed before preface"));
        }
        assert_eq!(&buf[..CONNECTION_PREFACE.len()], CONNECTION_PREFACE);
        for f in self.dec.push(&buf[CONNECTION_PREFACE.len()..]).unwrap() {
            self.queue.push_back(f);
        }
    }

    /// The next frame the client sends, awaiting more bytes if needed.
    async fn next_frame(&mut self) -> Frame {
        loop {
            if let Some(f) = self.queue.pop_front() {
                return f;
            }
            let chunk = self.rx.next().await.expect("client closed unexpectedly");
            for f in self.dec.push(&chunk).unwrap() {
                self.queue.push_back(f);
            }
        }
    }

    /// The next DATA frame from the client, skipping control frames (a SETTINGS
    /// ACK, WINDOW_UPDATE, …) that may interleave with the body. Returns
    /// `(payload, end_stream)`.
    async fn next_data(&mut self) -> (Vec<u8>, bool) {
        loop {
            match self.next_frame().await {
                Frame::Data {
                    data, end_stream, ..
                } => return (data, end_stream),
                Frame::Settings { .. } | Frame::WindowUpdate { .. } | Frame::Ping { .. } => {
                    continue
                }
                _ => panic!("unexpected non-DATA frame during upload"),
            }
        }
    }

    /// A frame that is already available without awaiting, or `None`.
    fn try_next_frame(&mut self) -> Option<Frame> {
        // `try_recv` yields `Ok` per ready chunk, `Err` (Empty/Closed) when drained.
        while let Ok(chunk) = self.rx.try_recv() {
            for f in self.dec.push(&chunk).unwrap() {
                self.queue.push_back(f);
            }
        }
        self.queue.pop_front()
    }
}

/// Yield to the executor enough times for any *ready* work to flush, so a
/// following assertion sees a client that is genuinely parked (e.g. blocked on
/// flow control) rather than merely not-yet-scheduled.
async fn quiesce() {
    for _ in 0..64 {
        let mut yielded = false;
        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}

#[test]
fn opens_with_preface_and_settings_not_http1_upgrade() {
    let mut pool = LocalPool::new();
    let (transport, mut c2s_rx, _s2c_tx) = mock_transport();
    let (_conn, driver) = connect(transport, ConnectOptions::default());
    pool.spawner().spawn_local(driver).unwrap();

    pool.run_until(async move {
        let frames = read_startup(&mut c2s_rx, 1).await;
        assert!(matches!(frames[0], Frame::Settings { ack: false, .. }));
    });
}

#[test]
fn sends_first_request_before_any_server_bytes() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, mut c2s_rx, _s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // Fire a request; never feed the client any server bytes. If it gated on
    // the server's preface, the HEADERS would never be written.
    let conn2 = conn.clone();
    sp.spawn_local(async move {
        let _ = conn2
            .request(RequestInit {
                method: Some("GET".into()),
                path: Some("/hello".into()),
                authority: Some("example.com".into()),
                ..Default::default()
            })
            .await;
    })
    .unwrap();

    pool.run_until(async move {
        let frames = read_startup(&mut c2s_rx, 2).await;
        assert!(matches!(frames[0], Frame::Settings { ack: false, .. }));
        assert!(matches!(frames[1], Frame::Headers { stream_id: 1, .. }));
    });
}

#[test]
fn completes_a_request_when_the_server_replies_afterwards() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, mut c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let req = conn.request(RequestInit {
            method: Some("GET".into()),
            path: Some("/hello".into()),
            authority: Some("example.com".into()),
            ..Default::default()
        });

        let server = async move {
            // Wait until the client has written its request HEADERS (stream 1).
            let frames = read_startup(&mut c2s_rx, 2).await;
            assert!(matches!(frames[1], Frame::Headers { stream_id: 1, .. }));
            // Reply after the request was already sent (prior-knowledge ordering).
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"ok".to_vec(),
                    end_stream: true,
                }))
                .unwrap();
        };

        let (res, ()) = futures::join!(req, server);
        let res = res.unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.text().await, "ok");
    });
}

#[test]
fn ping_resolves_with_the_round_trip_time() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let ping = conn.ping();

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            // Echo the client's PING back as an ACK (same opaque payload).
            let opaque = loop {
                match server.next_frame().await {
                    Frame::Ping {
                        ack: false,
                        opaque_data,
                    } => break opaque_data,
                    Frame::Settings { .. } => continue,
                    _ => panic!("expected a PING frame"),
                }
            };
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Ping {
                    ack: true,
                    opaque_data: opaque,
                }))
                .unwrap();
        };

        let (rtt, ()) = futures::join!(ping, server);
        let rtt = rtt.unwrap();
        assert!(
            rtt >= 0.0,
            "round-trip time should be non-negative, got {rtt}"
        );
    });
}

#[test]
fn streams_a_request_body_from_a_stream() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let body = RequestBody::stream(stream::iter(vec![
            b"aaaa".to_vec(),
            b"bbbb".to_vec(),
            b"cccc".to_vec(),
        ]));
        let req = conn.request(RequestInit {
            method: Some("POST".into()),
            path: Some("/upload".into()),
            authority: Some("example.com".into()),
            body,
            ..Default::default()
        });

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(
                server.next_frame().await,
                Frame::Settings { ack: false, .. }
            ));
            // Body present -> HEADERS must NOT carry END_STREAM.
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers {
                    stream_id: 1,
                    end_stream: false,
                    ..
                }
            ));

            let mut received = Vec::new();
            loop {
                let (data, end_stream) = server.next_data().await;
                received.extend(data);
                if end_stream {
                    break;
                }
            }
            assert_eq!(received, b"aaaabbbbcccc");

            // Reply so the request resolves.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"ok".to_vec(),
                    end_stream: true,
                }))
                .unwrap();
        };

        let (res, ()) = futures::join!(req, server);
        let res = res.unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.text().await, "ok");
    });
}

#[test]
fn upload_respects_connection_and_stream_flow_control() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // A body larger than the spec-default send window (65535 on both the
    // connection and the stream) — so flow control, not buffering, must gate it.
    let total_len = 70_000usize;
    let body = vec![0x61u8; total_len];
    sp.spawn_local(async move {
        let _ = conn
            .request(RequestInit {
                method: Some("POST".into()),
                path: Some("/upload".into()),
                authority: Some("example.com".into()),
                body: body.into(),
                ..Default::default()
            })
            .await;
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(
            server.next_frame().await,
            Frame::Settings { ack: false, .. }
        ));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers {
                stream_id: 1,
                end_stream: false,
                ..
            }
        ));

        // Before any WINDOW_UPDATE the client may send at most the initial
        // 65535-byte window — and no END_STREAM (the body isn't done).
        let mut sent = 0usize;
        while sent < SPEC_INITIAL_WINDOW as usize {
            match server.next_frame().await {
                Frame::Data {
                    stream_id: 1,
                    data,
                    end_stream,
                } => {
                    assert!(!end_stream, "client ran past the flow-control window");
                    sent += data.len();
                }
                _ => panic!("expected DATA while uploading"),
            }
        }
        assert_eq!(sent, SPEC_INITIAL_WINDOW as usize);

        // A correct client is now parked on a zero window and cannot produce
        // another byte until we replenish — even after the executor quiesces.
        quiesce().await;
        assert!(
            server.try_next_frame().is_none(),
            "client ignored flow control and kept sending"
        );

        // Replenish both the connection and the stream window.
        let rest = (total_len - sent) as u32;
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 0,
                window_size_increment: rest,
            }))
            .unwrap();
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 1,
                window_size_increment: rest,
            }))
            .unwrap();

        // The remainder now flows, ending with END_STREAM.
        let mut done = false;
        while !done {
            match server.next_frame().await {
                Frame::Data {
                    stream_id: 1,
                    data,
                    end_stream,
                } => {
                    sent += data.len();
                    done = end_stream;
                }
                _ => panic!("expected DATA after the window update"),
            }
        }
        assert_eq!(sent, total_len);
    });
}

#[test]
fn returns_the_response_before_the_upload_finishes() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // The body yields "part1", then blocks until `release` fires, then "part2".
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let body = RequestBody::stream(stream::unfold(
        (0u8, Some(release_rx)),
        |(i, mut gate)| async move {
            match i {
                0 => Some((b"part1".to_vec(), (1, gate))),
                1 => {
                    if let Some(rx) = gate.take() {
                        let _ = rx.await;
                    }
                    Some((b"part2".to_vec(), (2, gate)))
                }
                _ => None,
            }
        },
    ));

    pool.run_until(async move {
        let client = async move {
            let res = conn
                .request(RequestInit {
                    method: Some("POST".into()),
                    path: Some("/upload".into()),
                    authority: Some("example.com".into()),
                    body,
                    ..Default::default()
                })
                .await
                .unwrap();
            // We hold the response while the upload is still parked mid-stream.
            assert_eq!(res.status, 200);
            // Only now do we let the rest of the body upload.
            release_tx.send(()).unwrap();
            assert_eq!(res.text().await, "done");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(
                server.next_frame().await,
                Frame::Settings { ack: false, .. }
            ));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers {
                    stream_id: 1,
                    end_stream: false,
                    ..
                }
            ));
            // First chunk arrives...
            let (data, end_stream) = server.next_data().await;
            assert_eq!(data, b"part1");
            assert!(!end_stream);
            // ...and we respond *before* the upload completes (bidirectional).
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            // The client, upon seeing the response, releases the rest of the body.
            let (data, end_stream) = server.next_data().await;
            assert_eq!(data, b"part2");
            assert!(!end_stream);
            let (data, end_stream) = server.next_data().await;
            assert!(data.is_empty());
            assert!(end_stream);
            // Finish the response body.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"done".to_vec(),
                    end_stream: true,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn streams_the_response_body_incrementally() {
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let res = conn
                .request(RequestInit {
                    method: Some("GET".into()),
                    path: Some("/stream".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
            // Each server DATA frame surfaces as its own body chunk, in order.
            let mut body = res.into_body();
            assert_eq!(body.next().await.as_deref(), Some(&b"one"[..]));
            assert_eq!(body.next().await.as_deref(), Some(&b"two"[..]));
            assert_eq!(body.next().await.as_deref(), Some(&b"three"[..]));
            assert!(body.next().await.is_none());
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(
                server.next_frame().await,
                Frame::Settings { ack: false, .. }
            ));
            // No body -> HEADERS carries END_STREAM.
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers {
                    stream_id: 1,
                    end_stream: true,
                    ..
                }
            ));

            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            for (chunk, last) in [
                (&b"one"[..], false),
                (&b"two"[..], false),
                (&b"three"[..], true),
            ] {
                s2c_tx
                    .unbounded_send(serialize_frame(&Frame::Data {
                        stream_id: 1,
                        data: chunk.to_vec(),
                        end_stream: last,
                    }))
                    .unwrap();
            }
        };

        futures::join!(client, server);
    });
}

#[test]
fn finishes_upload_after_an_early_complete_response() {
    // Regression: if the server sends a COMPLETE response (END_STREAM) before the
    // client has finished uploading, the stream is only half-closed(remote) — the
    // client MUST still send the rest of its body and its own END_STREAM (RFC 7540
    // §5.1). A prior bug retired the stream on the peer's END_STREAM, which aborted
    // the upload pump and silently truncated the request body.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // "part1", then block until `release`, then "part2".
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let body = RequestBody::stream(stream::unfold(
        (0u8, Some(release_rx)),
        |(i, mut gate)| async move {
            match i {
                0 => Some((b"part1".to_vec(), (1, gate))),
                1 => {
                    if let Some(rx) = gate.take() {
                        let _ = rx.await;
                    }
                    Some((b"part2".to_vec(), (2, gate)))
                }
                _ => None,
            }
        },
    ));

    pool.run_until(async move {
        let client = async move {
            let res = conn
                .request(RequestInit {
                    method: Some("POST".into()),
                    path: Some("/echo".into()),
                    authority: Some("example.com".into()),
                    body,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
            // The server has ALREADY fully responded; now let the rest upload.
            release_tx.send(()).unwrap();
            assert_eq!(res.text().await, "done");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(
                server.next_frame().await,
                Frame::Settings { ack: false, .. }
            ));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers {
                    stream_id: 1,
                    end_stream: false,
                    ..
                }
            ));
            // First chunk arrives.
            let (data, end_stream) = server.next_data().await;
            assert_eq!(data, b"part1");
            assert!(!end_stream);

            // Send a COMPLETE response NOW — headers + END_STREAM data — while the
            // client is still mid-upload.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"done".to_vec(),
                    end_stream: true,
                }))
                .unwrap();

            // The client must STILL upload the remainder and close its send side.
            let (data, end_stream) = server.next_data().await;
            assert_eq!(data, b"part2", "client dropped its body after the early response");
            assert!(!end_stream);
            let (data, end_stream) = server.next_data().await;
            assert!(data.is_empty());
            assert!(end_stream, "client never sent END_STREAM for its request body");
        };

        futures::join!(client, server);
    });
}

#[test]
fn ping_errors_when_the_connection_closes_in_flight() {
    // An in-flight PING whose connection tears down before the ACK arrives must
    // resolve with an *error* — not hang, and not report a bogus round-trip time.
    // (Aligned with the TS client, which rejects the ping promise.)
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, _c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let ping = conn.ping();
        // EOF the transport (server side) while the ping is still outstanding.
        drop(s2c_tx);
        assert!(
            ping.await.is_err(),
            "ping should error when the connection closes in flight"
        );
    });
}
