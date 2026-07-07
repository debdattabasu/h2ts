//! Cross-stack conformance driver for the **Rust** `h2ts-client`.
//!
//! It runs the same fixed battery as `conformance/run.mjs` (the TS driver) —
//! routing, JSON, byte-exact upload/download, concurrent multiplexed streams,
//! streaming reads, PING, custom headers, 404 — but through the Rust client, over
//! a real native WebSocket to whatever gateway `WS_URL` points at (default the
//! `h2ts-proxy` the harness starts). This gives the Rust engine genuine
//! end-to-end coverage against a real proxy + h2c origin, not just its unit-test
//! mocks.
//!
//! The Rust client is a `!Send`, tokio-free engine, so the driver runs on a
//! current-thread runtime + `LocalSet`: the fastwebsockets read/write tasks are
//! ordinary (Send) `tokio::spawn`s that shuttle bytes through channels to the
//! client's (`!Send`) `Transport`, and the connection driver is `spawn_local`ed.

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, Payload, WebSocket};
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use h2ts_client::{
    connect, ConnectOptions, RequestBody, RequestInit, Transport, TransportError,
};
use http::header::{
    CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use http_body_util::Empty;
use hyper::upgrade::Upgraded;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// The upstream origin authority the requests target (mirrors `run.mjs`).
const AUTH: &str = "127.0.0.1:8000";

// --- check bookkeeping (mirrors run.mjs's `check`) -------------------------

struct Checks {
    failures: u32,
}

impl Checks {
    fn check(&mut self, name: &str, cond: bool, extra: &str) {
        if !cond {
            self.failures += 1;
        }
        let status = if cond { "ok  " } else { "FAIL" };
        if extra.is_empty() {
            println!("[{status}] {name}");
        } else {
            println!("[{status}] {name}  ({extra})");
        }
    }
}

// --- native WebSocket -> h2ts_client::Transport ----------------------------

/// Open a client WebSocket to `host:port`, offering `offer` (h2ts first). Returns
/// the raw WS plus the subprotocol the gateway echoed.
async fn open_ws(
    hostport: &str,
    offer: &[&str],
) -> (WebSocket<TokioIo<Upgraded>>, Option<String>) {
    let tcp = TcpStream::connect(hostport)
        .await
        .expect("connect to gateway");
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{hostport}/"))
        .header("Host", hostport)
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
        .expect("websocket handshake");
    let negotiated = resp
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    (ws, negotiated)
}

/// Present a client WebSocket as the client's byte `Transport`: inbound WS binary
/// payloads become byte chunks; outbound byte chunks become WS binary frames.
/// Control frames stay at the WebSocket layer (fastwebsockets auto-answers pings).
fn ws_transport(ws: WebSocket<TokioIo<Upgraded>>) -> Transport {
    let (mut ws_read, ws_write) = ws.split(|s| tokio::io::split(s));
    let ws_write = Arc::new(Mutex::new(ws_write));

    // Inbound: WS frames -> byte chunks. Dropping `in_tx` (on Close/EOF/error)
    // EOFs the client's reader.
    let (in_tx, in_rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    {
        let ws_write = ws_write.clone();
        tokio::spawn(async move {
            loop {
                // `read_frame` performs obligated writes (pong to a ping, close
                // echo) via this callback.
                let mut obligated = |frame: Frame<'_>| {
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
                match ws_read.read_frame(&mut obligated).await {
                    Ok(frame) => match frame.opcode {
                        OpCode::Binary | OpCode::Text | OpCode::Continuation => {
                            if !frame.payload.is_empty()
                                && in_tx.unbounded_send(frame.payload.to_vec()).is_err()
                            {
                                break;
                            }
                        }
                        OpCode::Close => break,
                        OpCode::Ping | OpCode::Pong => {}
                    },
                    Err(_) => break,
                }
            }
        });
    }

    // Outbound: byte chunks -> WS binary frames.
    let (out_tx, mut out_rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    {
        let ws_write = ws_write.clone();
        tokio::spawn(async move {
            while let Some(bytes) = out_rx.next().await {
                let frame = Frame::binary(Payload::Owned(bytes));
                if ws_write.lock().await.write_frame(frame).await.is_err() {
                    break;
                }
            }
        });
    }

    let reader = Box::pin(in_rx);
    let writer = Box::pin(out_tx.sink_map_err(|e: futures::channel::mpsc::SendError| {
        TransportError(e.to_string())
    }));
    Transport::new(reader, writer)
}

// --- request helpers -------------------------------------------------------

fn get(path: &str) -> RequestInit {
    RequestInit {
        path: Some(path.into()),
        authority: Some(AUTH.into()),
        ..Default::default()
    }
}

// --- the battery -----------------------------------------------------------

async fn run() -> i32 {
    let ws_url = std::env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8091".into());
    // ws:// only (the proxy is cleartext); strip scheme + any path -> host:port.
    let hostport = ws_url.strip_prefix("ws://").unwrap_or(&ws_url);
    let hostport = hostport.split('/').next().unwrap_or(hostport).to_string();

    // Offer `h2ts` first (the proxy echoes it).
    let (ws, protocol) = open_ws(&hostport, &["h2ts"]).await;
    let transport = ws_transport(ws);
    let (conn, driver) = connect(transport, ConnectOptions::default());

    // The connection driver is !Send; run it on the LocalSet alongside the battery.
    tokio::task::spawn_local(driver);

    println!(
        "connected (protocol={})\n",
        protocol.as_deref().unwrap_or("")
    );

    let mut c = Checks { failures: 0 };

    // 1. Basic GET
    match conn.request(get("/hello")).await {
        Ok(r) => {
            let status = r.status;
            c.check("GET /hello -> 200", status == 200, &format!("status={status}"));
            let body = r.text().await.to_lowercase();
            c.check("GET /hello body", body.contains("hello"), "");
        }
        Err(e) => c.check("GET /hello", false, &format!("{e}")),
    }

    // 2. JSON (substring checks — the client library has no JSON parser)
    match conn.request(get("/json")).await {
        Ok(r) => {
            let ct = r.headers.get("content-type").cloned().unwrap_or_default();
            c.check("GET /json content-type", ct == "application/json", &ct);
            let body = r.text().await;
            c.check(
                "GET /json parsed",
                body.contains("\"ok\":true") && body.contains("\"path\":\"/json\""),
                "",
            );
        }
        Err(e) => c.check("GET /json", false, &format!("{e}")),
    }

    // 3. Small POST echo
    match conn
        .request(RequestInit {
            method: Some("POST".into()),
            path: Some("/echo".into()),
            authority: Some(AUTH.into()),
            body: "ping-pong".into(),
            ..Default::default()
        })
        .await
    {
        Ok(r) => {
            c.check("POST /echo -> 200", r.status == 200, "");
            c.check("POST /echo echoes body", r.text().await == "ping-pong", "");
        }
        Err(e) => c.check("POST /echo", false, &format!("{e}")),
    }

    // 4. Large download (256 KiB) — inbound flow control across many DATA frames
    match conn.request(get("/big")).await {
        Ok(r) => {
            let x_size = r.headers.get("x-size").cloned().unwrap_or_default();
            c.check("GET /big x-size header", x_size == (256 * 1024).to_string(), &x_size);
            let big = r.bytes().await;
            c.check("GET /big size", big.len() == 256 * 1024, &format!("got {}", big.len()));
        }
        Err(e) => c.check("GET /big", false, &format!("{e}")),
    }

    // 5. Concurrent multiplexing (8 streams at once)
    let many = join_all((0..8).map(|_| conn.request(get("/json")))).await;
    let ok_count = {
        let mut n = 0usize;
        for r in many {
            if let Ok(r) = r {
                if r.text().await.contains("\"ok\":true") {
                    n += 1;
                }
            }
        }
        n
    };
    c.check("8 concurrent streams", ok_count == 8, &format!("ok={ok_count}/8"));

    // 6. Large upload (512 KiB) — outbound flow control + content integrity
    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i & 0xff) as u8).collect();
    match conn
        .request(RequestInit {
            method: Some("POST".into()),
            path: Some("/echo".into()),
            authority: Some(AUTH.into()),
            body: RequestBody::from(payload.clone()),
            ..Default::default()
        })
        .await
    {
        Ok(r) => {
            let x_echo = r.headers.get("x-echo-bytes").cloned().unwrap_or_default();
            c.check(
                "echo x-echo-bytes header",
                x_echo == payload.len().to_string(),
                &x_echo,
            );
            let echoed = r.bytes().await;
            c.check(
                "512KiB upload echo size",
                echoed.len() == payload.len(),
                &format!("got {}", echoed.len()),
            );
            c.check("512KiB upload echo content", echoed == payload, "");
        }
        Err(e) => c.check("512KiB upload", false, &format!("{e}")),
    }

    // 7. Custom request header round-trips
    match conn
        .request(RequestInit {
            path: Some("/headers".into()),
            authority: Some(AUTH.into()),
            headers: vec![("x-custom".into(), "h2ts-rocks".into())],
            ..Default::default()
        })
        .await
    {
        Ok(r) => {
            let saw = r.headers.get("x-saw-custom").cloned().unwrap_or_default();
            c.check("custom header reflected", saw == "h2ts-rocks", &saw);
        }
        Err(e) => c.check("custom header", false, &format!("{e}")),
    }

    // 8. PING RTT
    match conn.ping().await {
        Ok(rtt) => c.check("ping rtt >= 0", rtt >= 0.0, &format!("rtt={rtt:.2}ms")),
        Err(e) => c.check("ping", false, &format!("{e}")),
    }

    // 9. Streaming body read via the response chunk stream
    match conn.request(get("/big")).await {
        Ok(r) => {
            let mut body = r.into_body();
            let mut streamed = 0usize;
            while let Some(chunk) = body.next().await {
                streamed += chunk.len();
            }
            c.check(
                "streamed /big size",
                streamed == 256 * 1024,
                &format!("got {streamed}"),
            );
        }
        Err(e) => c.check("streamed /big", false, &format!("{e}")),
    }

    // 10. 404 handling
    match conn.request(get("/nope")).await {
        Ok(r) => c.check("GET /nope -> 404", r.status == 404, &format!("status={}", r.status)),
        Err(e) => c.check("GET /nope", false, &format!("{e}")),
    }

    println!(
        "{}",
        if c.failures == 0 {
            "\n✅ ALL E2E PASSED (rust client)".to_string()
        } else {
            format!("\n❌ {} E2E FAILURE(S) (rust client)", c.failures)
        }
    );

    conn.close();
    let _ = std::io::stdout().flush();
    i32::from(c.failures != 0)
}

fn main() {
    // One thread drives everything: a current-thread tokio runtime backs the
    // fastwebsockets read/write tasks + TCP, and a LocalSet lets the !Send client
    // driver and battery run on it too. `block_on` polls both sets, so bytes flow.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = tokio::task::LocalSet::new().block_on(&rt, run());
    std::process::exit(code);
}
