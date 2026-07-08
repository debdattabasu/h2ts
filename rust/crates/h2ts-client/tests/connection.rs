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
use h2ts_client::hpack::{Header, HpackDecoder, HpackEncoder};
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
    // Skip the startup connection-window growth WINDOW_UPDATE(0) so opening-flight
    // assertions (SETTINGS then HEADERS) stay position-stable.
    let keep = |raw: Vec<Frame>, frames: &mut Vec<Frame>| {
        for f in raw {
            if !matches!(f, Frame::WindowUpdate { stream_id: 0, .. }) {
                frames.push(f);
            }
        }
    };
    let mut frames: Vec<Frame> = Vec::new();
    keep(dec.push(&buf[CONNECTION_PREFACE.len()..]).unwrap(), &mut frames);
    while frames.len() < want {
        let chunk = c2s_rx.next().await.expect("client closed before frames");
        keep(dec.push(&chunk).unwrap(), &mut frames);
    }
    frames
}

/// The peer side of a mock transport: buffers and decodes the frames the client
/// writes, so a test can assert on them one at a time.
struct ServerSide {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    dec: FrameDecoder,
    queue: std::collections::VecDeque<Frame>,
    /// Skip the one startup connection-window growth WINDOW_UPDATE(0) so tests see
    /// the [SETTINGS, HEADERS, …] flight they expect; later WINDOW_UPDATEs pass.
    skip_conn_wu: bool,
}

impl ServerSide {
    fn new(rx: mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx,
            dec: FrameDecoder::default(),
            queue: std::collections::VecDeque::new(),
            skip_conn_wu: true,
        }
    }

    /// Pop the next queued frame, transparently dropping the startup
    /// connection-window WINDOW_UPDATE(0) the first time it appears.
    fn take_queued(&mut self) -> Option<Frame> {
        while let Some(f) = self.queue.pop_front() {
            if self.skip_conn_wu && matches!(f, Frame::WindowUpdate { stream_id: 0, .. }) {
                self.skip_conn_wu = false;
                continue;
            }
            return Some(f);
        }
        None
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
            if let Some(f) = self.take_queued() {
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
        self.take_queued()
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
        let mut res = res.unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.text().await.unwrap(), "ok");
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
        let mut res = res.unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.text().await.unwrap(), "ok");
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
            let mut res = conn
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
            assert_eq!(res.text().await.unwrap(), "done");
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
            // Each server DATA frame surfaces as its own body chunk (Ok), in order.
            let mut body = res.into_body();
            assert_eq!(body.next().await.unwrap().unwrap(), b"one");
            assert_eq!(body.next().await.unwrap().unwrap(), b"two");
            assert_eq!(body.next().await.unwrap().unwrap(), b"three");
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
            let mut res = conn
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
            assert_eq!(res.text().await.unwrap(), "done");
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
fn finishes_a_flow_limited_upload_after_an_early_complete_response() {
    // Regression (flow-limited variant of `finishes_upload_after_an_early_complete_response`):
    // the server completes its response (END_STREAM) before the upload finishes AND
    // the body is larger than the flow-control window, so the client must honor a
    // stream-level WINDOW_UPDATE that arrives AFTER the early response. The stream
    // must stay routable in the map post-remote-END_STREAM (retire only when BOTH
    // sides end). Mirrors the TS client's regression test.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    let total = 100_000usize;
    let body = vec![0x61u8; total];
    sp.spawn_local(async move {
        let mut res = conn
            .request(RequestInit {
                method: Some("POST".into()),
                path: Some("/upload".into()),
                authority: Some("example.com".into()),
                body: body.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(res.status, 200);
        let _ = res.bytes().await;
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers { stream_id: 1, .. }
        ));

        // Drain exactly the initial 65535-byte window, then the client parks.
        let mut sent = 0usize;
        while sent < SPEC_INITIAL_WINDOW as usize {
            let (d, end) = server.next_data().await;
            assert!(!end);
            sent += d.len();
        }
        assert_eq!(sent, SPEC_INITIAL_WINDOW as usize);

        // COMPLETE response now (headers-only, END_STREAM) — while still uploading.
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
                end_stream: true,
                end_headers: true,
                priority: None,
            }))
            .unwrap();

        // Grant more window so the client can finish uploading.
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 0,
                window_size_increment: 1_000_000,
            }))
            .unwrap();
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 1,
                window_size_increment: 1_000_000,
            }))
            .unwrap();

        // The client must upload the remainder and send its own END_STREAM.
        loop {
            let (d, end) = server.next_data().await;
            sent += d.len();
            if end {
                break;
            }
        }
        assert_eq!(sent, total, "client dropped its body after the early response");
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

#[test]
fn rst_stream_mid_upload_fails_the_request_without_hanging() {
    // The peer resets the stream while the client is still uploading. The request
    // must resolve with an error and the body pump must stop — never hang.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // Yields one chunk, then never produces another (upload is "in progress").
    let body = RequestBody::stream(stream::once(async { b"part1".to_vec() }).chain(stream::pending()));

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
                .await;
            assert!(res.is_err(), "request should error when the stream is reset");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            let (data, _end) = server.next_data().await;
            assert_eq!(data, b"part1");
            // Reset the stream mid-upload (CANCEL = 0x8).
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::RstStream {
                    stream_id: 1,
                    error_code: 8,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn goaway_with_error_tears_down_and_fails_in_flight_requests() {
    // A GOAWAY with a non-zero error code fails the in-flight request and closes
    // the connection (RFC 7540 §6.8).
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error after a GOAWAY(error)");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            // GOAWAY: last_stream_id 0 (so stream 1 > 0 is doomed), PROTOCOL_ERROR.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Goaway {
                    last_stream_id: 0,
                    error_code: 1,
                    debug_data: Vec::new(),
                }))
                .unwrap();
        };

        futures::join!(client, server);
        assert!(
            conn_probe.is_closed(),
            "connection should be closed after a GOAWAY error"
        );
    });
}

#[test]
fn rst_stream_mid_download_errors_the_response_body() {
    // The head resolves, but a reset mid-body must ERROR the body (not look like a
    // clean EOF) so a truncated download is visible — matching the TS client.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let mut res = conn
                .request(RequestInit {
                    path: Some("/download".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
            // One chunk arrived, then a reset — buffering the body must error.
            assert!(
                res.bytes().await.is_err(),
                "a reset mid-download must surface as a body error"
            );
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
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
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"one".to_vec(),
                    end_stream: false,
                }))
                .unwrap();
            // Reset mid-download (CANCEL).
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::RstStream {
                    stream_id: 1,
                    error_code: 8,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn surfaces_response_trailers() {
    // A HEADERS block after the body is trailers; the client exposes them via
    // Response::trailers() once the body has been read.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let mut res = conn
                .request(RequestInit {
                    path: Some("/rpc".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
            assert_eq!(res.bytes().await.unwrap(), b"data");
            let trailers = res.trailers().expect("trailers should be present");
            assert_eq!(trailers.get("grpc-status").map(String::as_str), Some("0"));
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            let head = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: head,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"data".to_vec(),
                    end_stream: false,
                }))
                .unwrap();
            // Trailers: a second HEADERS block, carrying END_STREAM.
            let trailers = HpackEncoder::new().encode(&[Header::new("grpc-status", "0")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: trailers,
                    end_stream: true,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn honors_a_retroactively_shrunk_send_window() {
    // The peer lowering SETTINGS_INITIAL_WINDOW_SIZE mid-stream retroactively
    // shrinks a live stream's send window — possibly negative (RFC 7540 §6.9.2).
    // The client must not over-send: after the window goes negative, only a
    // WINDOW_UPDATE that makes it positive again releases more DATA, byte-exact.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // 65535 (the initial stream window) + 1000 more to send afterwards.
    let total = 65535usize + 1000;
    let body = vec![0x61u8; total];

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
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers { stream_id: 1, .. }
        ));
        // Take the connection window out of the picture so only the *stream*
        // window gates the upload.
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 0,
                window_size_increment: 1_000_000,
            }))
            .unwrap();

        // The client sends exactly the initial 65535-byte window, then parks.
        let mut sent = 0usize;
        while sent < 65535 {
            let (d, end) = server.next_data().await;
            assert!(!end);
            sent += d.len();
        }
        assert_eq!(sent, 65535, "client sent past its initial stream window");

        // Shrink the initial window to 100: the live stream window becomes
        // 0 + (100 - 65535) = -65435 (negative).
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Settings {
                ack: false,
                settings: Settings {
                    initial_window_size: Some(100),
                    ..Default::default()
                },
            }))
            .unwrap();
        // Grant just enough to make it +10.
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 1,
                window_size_increment: 65445,
            }))
            .unwrap();

        // The client releases exactly 10 bytes — proof it tracked the negative window.
        let (d, end) = server.next_data().await;
        assert_eq!(d.len(), 10, "client ignored the retroactively-shrunk window");
        assert!(!end);
        sent += d.len();

        // Let the remainder flow.
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                stream_id: 1,
                window_size_increment: 2000,
            }))
            .unwrap();
        loop {
            let (d, end) = server.next_data().await;
            sent += d.len();
            if end {
                break;
            }
        }
        assert_eq!(sent, total);
    });
}

#[test]
fn graceful_goaway_fails_higher_streams_but_lets_lower_finish() {
    // A GOAWAY(last_stream_id = N, code = 0) fails streams with id > N but leaves
    // id <= N to complete (RFC 7540 §6.8). The connection is not torn down.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let req1 = conn.request(RequestInit {
            path: Some("/a".into()),
            authority: Some("example.com".into()),
            ..Default::default()
        });
        let req3 = conn.request(RequestInit {
            path: Some("/b".into()),
            authority: Some("example.com".into()),
            ..Default::default()
        });

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 3, .. }
            ));
            // Graceful GOAWAY: last_stream_id = 1 dooms stream 3 but not stream 1.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Goaway {
                    last_stream_id: 1,
                    error_code: 0,
                    debug_data: Vec::new(),
                }))
                .unwrap();
            // Complete stream 1.
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

        let (r1, r3, ()) = futures::join!(req1, req3, server);
        assert!(r3.is_err(), "stream above lastStreamId must fail");
        let mut r1 = r1.expect("stream at/below lastStreamId should complete");
        assert_eq!(r1.status, 200);
        assert_eq!(r1.text().await.unwrap(), "ok");
    });
}

#[test]
fn connection_window_update_zero_tears_down_with_goaway() {
    // A connection-level WINDOW_UPDATE with a 0 increment is a PROTOCOL_ERROR
    // (§6.9): the client emits GOAWAY and closes.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error on a protocol error");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                    stream_id: 0,
                    window_size_increment: 0,
                }))
                .unwrap();
            // The client must answer a connection error with a GOAWAY.
            assert!(matches!(server.next_frame().await, Frame::Goaway { .. }));
        };

        futures::join!(client, server);
        assert!(conn_probe.is_closed(), "connection should be closed");
    });
}

#[test]
fn reassembles_a_header_block_split_across_continuation() {
    // A response header block split across HEADERS + CONTINUATION must reassemble.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            // Encode :status 200 and split the block across two frames.
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            let mid = block.len() / 2;
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block[..mid].to_vec(),
                    end_stream: false,
                    end_headers: false,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Continuation {
                    stream_id: 1,
                    header_block_fragment: block[mid..].to_vec(),
                    end_headers: true,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: Vec::new(),
                    end_stream: true,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn honors_the_peers_max_concurrent_streams() {
    // The peer advertises SETTINGS_MAX_CONCURRENT_STREAMS=1: while one stream is
    // open the client must PARK a second request (not open stream 3), then release
    // it once the first stream completes (§5.1.2).
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // req1: kept open until the server ends it.
    let conn1 = conn.clone();
    sp.spawn_local(async move {
        let mut res = conn1
            .request(RequestInit {
                path: Some("/a".into()),
                authority: Some("e".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let _ = res.bytes().await;
    })
    .unwrap();

    // req2: fired only once the gate opens (after the client has applied max=1).
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel::<u16>();
    let conn2 = conn.clone();
    sp.spawn_local(async move {
        let _ = gate_rx.await;
        let mut res = conn2
            .request(RequestInit {
                path: Some("/b".into()),
                authority: Some("e".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let _ = res.bytes().await;
        let _ = done_tx.send(res.status);
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers { stream_id: 1, .. }
        ));
        // Advertise a limit of 1.
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Settings {
                ack: false,
                settings: Settings {
                    max_concurrent_streams: Some(1),
                    ..Default::default()
                },
            }))
            .unwrap();
        // The client's SETTINGS ack means it has applied the limit.
        assert!(matches!(
            server.next_frame().await,
            Frame::Settings { ack: true, .. }
        ));

        // Release req2: it must PARK — stream 1 is open, so we're at the limit.
        gate_tx.send(()).unwrap();
        quiesce().await;
        assert!(
            server.try_next_frame().is_none(),
            "client opened a second stream past the peer's limit"
        );

        // Complete stream 1 → frees the slot → req2 proceeds.
        let head = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Headers {
                stream_id: 1,
                header_block_fragment: head,
                end_stream: false,
                end_headers: true,
                priority: None,
            }))
            .unwrap();
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Data {
                stream_id: 1,
                data: b"a".to_vec(),
                end_stream: true,
            }))
            .unwrap();

        // The parked request now opens stream 3 (skipping any WINDOW_UPDATE).
        loop {
            match server.next_frame().await {
                Frame::Headers { stream_id: 3, .. } => break,
                Frame::WindowUpdate { .. } => continue,
                _ => panic!("expected stream 3 HEADERS after the slot freed"),
            }
        }
        let head3 = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Headers {
                stream_id: 3,
                header_block_fragment: head3,
                end_stream: false,
                end_headers: true,
                priority: None,
            }))
            .unwrap();
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Data {
                stream_id: 3,
                data: b"b".to_vec(),
                end_stream: true,
            }))
            .unwrap();

        assert_eq!(done_rx.await.unwrap(), 200);
    });
}

#[test]
fn stream_window_update_zero_resets_only_the_stream() {
    // §6.9.1: a stream-level WINDOW_UPDATE with a 0 increment is a stream error —
    // the client RST_STREAMs the stream and fails the request, but the connection
    // survives.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("e".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error, not hang");
        };
        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::WindowUpdate {
                    stream_id: 1,
                    window_size_increment: 0,
                }))
                .unwrap();
            loop {
                if matches!(server.next_frame().await, Frame::RstStream { stream_id: 1, .. }) {
                    break;
                }
            }
        };
        futures::join!(client, server);
        assert!(
            !conn_probe.is_closed(),
            "the connection should survive a single stream reset"
        );
    });
}

#[test]
fn a_frame_over_max_frame_size_tears_down_with_goaway() {
    // A frame whose length exceeds our advertised SETTINGS_MAX_FRAME_SIZE (16384)
    // is a FRAME_SIZE_ERROR: the client emits GOAWAY and closes.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("e".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err());
        };
        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            // 20000 bytes > the default 16384 max frame size.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: vec![0u8; 20000],
                    end_stream: false,
                }))
                .unwrap();
            loop {
                if matches!(server.next_frame().await, Frame::Goaway { .. }) {
                    break;
                }
            }
        };
        futures::join!(client, server);
        assert!(conn_probe.is_closed());
    });
}

#[test]
fn strips_padding_from_a_padded_headers_frame() {
    // A padded HEADERS frame on receive: the client strips the padding and decodes
    // the header block (only padded DATA was covered before).
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("e".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
        };
        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            // Hand-build a PADDED HEADERS frame (serialize_frame doesn't emit padding).
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            let pad_len = 4usize;
            let mut payload = Vec::new();
            payload.push(pad_len as u8);
            payload.extend_from_slice(&block);
            payload.resize(payload.len() + pad_len, 0); // padding
            let mut frame = Vec::new();
            let len = payload.len();
            frame.push(((len >> 16) & 0xff) as u8);
            frame.push(((len >> 8) & 0xff) as u8);
            frame.push((len & 0xff) as u8);
            frame.push(0x1); // HEADERS
            frame.push(0x4 | 0x8); // END_HEADERS | PADDED
            frame.extend_from_slice(&[0, 0, 0, 1]); // stream 1
            frame.extend_from_slice(&payload);
            s2c_tx.unbounded_send(frame).unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: Vec::new(),
                    end_stream: true,
                }))
                .unwrap();
        };
        futures::join!(client, server);
    });
}

#[test]
fn refuses_an_inbound_push_promise() {
    // The client refuses server push (no on_push): an inbound PUSH_PROMISE is
    // answered with RST_STREAM(REFUSED_STREAM) on the promised stream.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    sp.spawn_local(async move {
        let _ = conn
            .request(RequestInit {
                path: Some("/x".into()),
                authority: Some("e".into()),
                ..Default::default()
            })
            .await;
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers { stream_id: 1, .. }
        ));
        let push = HpackEncoder::new().encode(&[
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":authority", "e"),
            Header::new(":path", "/pushed"),
        ]);
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::PushPromise {
                stream_id: 1,
                promised_stream_id: 2,
                header_block_fragment: push,
                end_headers: true,
            }))
            .unwrap();
        loop {
            match server.next_frame().await {
                Frame::RstStream {
                    stream_id: 2,
                    error_code,
                } => {
                    assert_eq!(error_code, 7); // REFUSED_STREAM
                    break;
                }
                _ => continue,
            }
        }
    });
}

#[test]
fn splits_an_oversized_request_header_block_on_send() {
    // A request header block larger than the peer's max frame size is split across
    // HEADERS + CONTINUATION, and reassembles to the original headers.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, _s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    // Big enough that even Huffman-coded (~5 bits/char) it exceeds 16384.
    let big = "a".repeat(40000);
    let big_send = big.clone();
    sp.spawn_local(async move {
        let _ = conn
            .request(RequestInit {
                path: Some("/x".into()),
                authority: Some("e".into()),
                headers: vec![("x-big".into(), big_send)],
                ..Default::default()
            })
            .await;
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));

        let mut fragments: Vec<u8> = Vec::new();
        match server.next_frame().await {
            Frame::Headers {
                stream_id: 1,
                header_block_fragment,
                end_headers,
                ..
            } => {
                assert!(!end_headers, "block should not fit one HEADERS frame");
                fragments.extend_from_slice(&header_block_fragment);
            }
            _ => panic!("expected HEADERS"),
        }
        loop {
            match server.next_frame().await {
                Frame::Continuation {
                    header_block_fragment,
                    end_headers,
                    ..
                } => {
                    fragments.extend_from_slice(&header_block_fragment);
                    if end_headers {
                        break;
                    }
                }
                _ => panic!("expected CONTINUATION"),
            }
        }
        let decoded = HpackDecoder::new(4096).decode(&fragments).unwrap();
        assert!(decoded.iter().any(|h| h.name == "x-big" && h.value == big));
    });
}

#[test]
fn replenishes_the_receive_window_only_on_consumption() {
    // Consumption-driven flow control (à la node:http2): the client does NOT send a
    // WINDOW_UPDATE when DATA is buffered — only when the application *reads* it.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel::<Vec<u8>>();

    sp.spawn_local(async move {
        let res = conn
            .request(RequestInit {
                path: Some("/download".into()),
                authority: Some("e".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(res.status, 200);
        let mut body = res.into_body();
        gate_rx.await.unwrap(); // read only once the server has checked backpressure
        let chunk = body.next().await.unwrap().unwrap();
        let _ = done_tx.send(chunk);
    })
    .unwrap();

    pool.run_until(async move {
        let mut server = ServerSide::new(c2s_rx);
        server.read_preface().await;
        assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
        assert!(matches!(
            server.next_frame().await,
            Frame::Headers { stream_id: 1, .. }
        ));
        // Respond with the head + one body chunk (no END_STREAM).
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Settings {
                ack: false,
                settings: Settings::default(),
            }))
            .unwrap();
        let head = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Headers {
                stream_id: 1,
                header_block_fragment: head,
                end_stream: false,
                end_headers: true,
                priority: None,
            }))
            .unwrap();
        s2c_tx
            .unbounded_send(serialize_frame(&Frame::Data {
                stream_id: 1,
                data: b"hello".to_vec(),
                end_stream: false,
            }))
            .unwrap();

        // Backpressure: the app hasn't read, so no WINDOW_UPDATE has been sent.
        quiesce().await;
        let mut pre = Vec::new();
        while let Some(f) = server.try_next_frame() {
            pre.push(f);
        }
        assert!(
            !pre.iter().any(|f| matches!(f, Frame::WindowUpdate { .. })),
            "replenished the window before the body was read"
        );

        // Let the client read one chunk → it returns those 5 bytes to both windows.
        gate_tx.send(()).unwrap();
        assert_eq!(done_rx.await.unwrap(), b"hello");

        let (mut stream_wu, mut conn_wu) = (false, false);
        for _ in 0..8 {
            match server.next_frame().await {
                Frame::WindowUpdate {
                    stream_id: 1,
                    window_size_increment,
                } => {
                    assert_eq!(window_size_increment, 5);
                    stream_wu = true;
                }
                Frame::WindowUpdate {
                    stream_id: 0,
                    window_size_increment,
                } => {
                    assert_eq!(window_size_increment, 5);
                    conn_wu = true;
                }
                _ => {}
            }
            if stream_wu && conn_wu {
                break;
            }
        }
        assert!(stream_wu && conn_wu, "consumption must replenish both windows");
    });
}

#[test]
fn treats_a_1xx_interim_response_as_non_final() {
    // An interim 1xx (103 Early Hints) before the final response must NOT be
    // surfaced as the response, nor should the real response be filed as trailers.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let client = async move {
            let mut res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(res.status, 200);
            assert_eq!(res.bytes().await.unwrap(), b"body");
            assert!(res.trailers().is_none());
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            // Interim 103 Early Hints (no END_STREAM).
            let early = HpackEncoder::new()
                .encode(&[Header::new(":status", "103"), Header::new("link", "</a.css>")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: early,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            // Real final response.
            let head = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: head,
                    end_stream: false,
                    end_headers: true,
                    priority: None,
                }))
                .unwrap();
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"body".to_vec(),
                    end_stream: true,
                }))
                .unwrap();
        };

        futures::join!(client, server);
    });
}

#[test]
fn out_of_range_max_frame_size_tears_down_with_goaway() {
    // A peer SETTINGS_MAX_FRAME_SIZE below the 16384 floor is a PROTOCOL_ERROR
    // (§6.5.2): the client emits GOAWAY and closes.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error on an invalid SETTINGS");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings {
                        max_frame_size: Some(100),
                        ..Default::default()
                    },
                }))
                .unwrap();
            while !matches!(server.next_frame().await, Frame::Goaway { .. }) {}
        };

        futures::join!(client, server);
        assert!(conn_probe.is_closed(), "connection should be closed");
    });
}

#[test]
fn a_header_block_over_the_cap_tears_down_with_goaway() {
    // An endless CONTINUATION stream must be bounded: once the accumulated block
    // exceeds the cap, the client emits GOAWAY and closes rather than buffering.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error on a header-block flood");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Settings {
                    ack: false,
                    settings: Settings::default(),
                }))
                .unwrap();
            // HEADERS (no END_HEADERS) then CONTINUATION frames past the 1 MiB cap.
            let frag = vec![0u8; 16000];
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: frag.clone(),
                    end_stream: false,
                    end_headers: false,
                    priority: None,
                }))
                .unwrap();
            for _ in 0..70 {
                s2c_tx
                    .unbounded_send(serialize_frame(&Frame::Continuation {
                        stream_id: 1,
                        header_block_fragment: frag.clone(),
                        end_headers: false,
                    }))
                    .unwrap();
            }
            while !matches!(server.next_frame().await, Frame::Goaway { .. }) {}
        };

        futures::join!(client, server);
        assert!(conn_probe.is_closed(), "connection should be closed");
    });
}

#[test]
fn an_unterminated_header_block_interrupted_by_data_is_a_protocol_error() {
    // While a header block is pending (HEADERS without END_HEADERS), only a
    // CONTINUATION may follow (§6.2). A DATA frame there is a PROTOCOL_ERROR.
    let mut pool = LocalPool::new();
    let sp = pool.spawner();
    let (transport, c2s_rx, s2c_tx) = mock_transport();
    let (conn, driver) = connect(transport, ConnectOptions::default());
    sp.spawn_local(driver).unwrap();

    pool.run_until(async move {
        let conn_probe = conn.clone();
        let client = async move {
            let res = conn
                .request(RequestInit {
                    path: Some("/x".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
            assert!(res.is_err(), "request should error on a protocol error");
        };

        let server = async move {
            let mut server = ServerSide::new(c2s_rx);
            server.read_preface().await;
            assert!(matches!(server.next_frame().await, Frame::Settings { .. }));
            assert!(matches!(
                server.next_frame().await,
                Frame::Headers { stream_id: 1, .. }
            ));
            let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
            // HEADERS without END_HEADERS...
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Headers {
                    stream_id: 1,
                    header_block_fragment: block,
                    end_stream: false,
                    end_headers: false,
                    priority: None,
                }))
                .unwrap();
            // ...then a DATA frame instead of the required CONTINUATION.
            s2c_tx
                .unbounded_send(serialize_frame(&Frame::Data {
                    stream_id: 1,
                    data: b"nope".to_vec(),
                    end_stream: false,
                }))
                .unwrap();
            assert!(matches!(server.next_frame().await, Frame::Goaway { .. }));
        };

        futures::join!(client, server);
        assert!(conn_probe.is_closed(), "connection should be closed");
    });
}
