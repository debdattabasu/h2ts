//! Wasm conformance driver for the **Rust** `h2ts-client`, exercising its **real
//! browser `WebSocket` transport** (`web.rs`: `connect_websocket` /
//! `websocket_transport` / `WsSink`) — the code that actually ships to a wasm
//! frontend and that nothing else tests.
//!
//! It runs the same fixed battery as the native `h2ts-conformance` driver and
//! `conformance/run.mjs` — routing, JSON, byte-exact upload/download, concurrent
//! multiplexed streams, streaming reads, PING, custom headers, 404 — but the
//! transport is a genuine `web_sys::WebSocket`. Compiled to `wasm32`, wrapped by
//! `wasm-bindgen --target nodejs`, and run under Node (which provides the global
//! `WebSocket`) by `conformance/wasm-run.mjs`, against whatever gateway `ws_url`
//! points at (the `h2ts-proxy` the harness starts).
//!
//! The whole crate is `#![cfg(target_arch = "wasm32")]`: on a native target the
//! browser transport doesn't exist, so the workspace's normal build/test compile
//! this to an empty library.
#![cfg(target_arch = "wasm32")]

use futures::future::join_all;
use futures::StreamExt;
use h2ts_client::{connect_websocket, ConnectOptions, RequestBody, RequestInit};
use wasm_bindgen::prelude::*;
use web_sys::console;

/// The upstream origin authority the requests target (mirrors `run.mjs` and the
/// native driver — it's the `:authority` pseudo-header, not a routing address).
const AUTH: &str = "127.0.0.1:8000";

fn log(msg: &str) {
    console::log_1(&JsValue::from_str(msg));
}

/// Check bookkeeping, mirroring `run.mjs`'s `check` and the native driver.
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
            log(&format!("[{status}] {name}"));
        } else {
            log(&format!("[{status}] {name}  ({extra})"));
        }
    }
}

fn get(path: &str) -> RequestInit {
    RequestInit {
        path: Some(path.into()),
        authority: Some(AUTH.into()),
        ..Default::default()
    }
}

/// Run the shared conformance battery through the real browser `WebSocket`
/// transport against the gateway at `ws_url`, offering the `h2ts` subprotocol.
///
/// Resolves to the number of failed checks (`0` = all passed); rejects only if the
/// initial WebSocket/HTTP-2 connection can't be established. Each check is logged
/// via `console.log` so the Node harness surfaces the same line-by-line report the
/// other drivers print.
#[wasm_bindgen]
pub async fn run_battery(ws_url: String) -> Result<JsValue, JsValue> {
    // This is the code under test: open a real WebSocket, adapt it to the client's
    // Transport (`websocket_transport` + `WsSink`), and spawn the driver.
    let conn = connect_websocket(&ws_url, &["h2ts"], ConnectOptions::default()).await?;
    log("connected (wasm / real WebSocket)\n");

    let mut c = Checks { failures: 0 };

    // 1. Basic GET
    match conn.request(get("/hello")).await {
        Ok(mut r) => {
            let status = r.status;
            c.check(
                "GET /hello -> 200",
                status == 200,
                &format!("status={status}"),
            );
            let body = r.text().await.unwrap_or_default().to_lowercase();
            c.check("GET /hello body", body.contains("hello"), "");
        }
        Err(e) => c.check("GET /hello", false, &format!("{e}")),
    }

    // 2. JSON (substring checks — the client library has no JSON parser)
    match conn.request(get("/json")).await {
        Ok(mut r) => {
            let ct = r.headers.get("content-type").cloned().unwrap_or_default();
            c.check("GET /json content-type", ct == "application/json", &ct);
            let body = r.text().await.unwrap_or_default();
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
        Ok(mut r) => {
            c.check("POST /echo -> 200", r.status == 200, "");
            c.check(
                "POST /echo echoes body",
                r.text().await.unwrap_or_default() == "ping-pong",
                "",
            );
        }
        Err(e) => c.check("POST /echo", false, &format!("{e}")),
    }

    // 4. Large download (256 KiB) — inbound flow control across many DATA frames
    match conn.request(get("/big")).await {
        Ok(mut r) => {
            let x_size = r.headers.get("x-size").cloned().unwrap_or_default();
            c.check(
                "GET /big x-size header",
                x_size == (256 * 1024).to_string(),
                &x_size,
            );
            let big = r.bytes().await.unwrap_or_default();
            c.check(
                "GET /big size",
                big.len() == 256 * 1024,
                &format!("got {}", big.len()),
            );
        }
        Err(e) => c.check("GET /big", false, &format!("{e}")),
    }

    // 5. Concurrent multiplexing (8 streams at once)
    let many = join_all((0..8).map(|_| conn.request(get("/json")))).await;
    let mut ok_count = 0usize;
    for r in many {
        if let Ok(mut r) = r {
            if r.text().await.unwrap_or_default().contains("\"ok\":true") {
                ok_count += 1;
            }
        }
    }
    c.check(
        "8 concurrent streams",
        ok_count == 8,
        &format!("ok={ok_count}/8"),
    );

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
        Ok(mut r) => {
            let x_echo = r.headers.get("x-echo-bytes").cloned().unwrap_or_default();
            c.check(
                "echo x-echo-bytes header",
                x_echo == payload.len().to_string(),
                &x_echo,
            );
            let echoed = r.bytes().await.unwrap_or_default();
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
                if let Ok(chunk) = chunk {
                    streamed += chunk.len();
                }
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
        Ok(r) => c.check(
            "GET /nope -> 404",
            r.status == 404,
            &format!("status={}", r.status),
        ),
        Err(e) => c.check("GET /nope", false, &format!("{e}")),
    }

    log(if c.failures == 0 {
        "\n✅ ALL E2E PASSED (rust wasm client)"
    } else {
        "\n❌ E2E FAILURE(S) (rust wasm client)"
    });

    conn.close();
    Ok(JsValue::from_f64(c.failures as f64))
}
