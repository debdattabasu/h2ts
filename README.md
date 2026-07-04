# h2ts

> **HTTP/2 in the browser — tunneled over WebSockets.**

![license](https://img.shields.io/badge/license-MIT-blue)
![client](https://img.shields.io/badge/client-~9%20KB%20gzip-brightgreen)
![deps](https://img.shields.io/badge/runtime%20deps-0-brightgreen)
![tests](https://img.shields.io/badge/tests-vitest%20%2B%20cargo-informational)

Browsers can't open raw TCP sockets, can't speak HTTP/2 with prior knowledge, and give you no control over framing, multiplexing, or server push. **h2ts** gives a browser a real HTTP/2 client by carrying HTTP/2 frames inside a WebSocket, and a small Rust server side that terminates the WebSocket and hands the raw bytes to any HTTP/2 server.

It comes in two halves that are useful independently:

| | Package | Language | What it is |
|---|---|---|---|
| **Client** | `h2ts` | TypeScript | A from-scratch HTTP/2 client (RFC 7540 + HPACK/RFC 7541). ~9 KB gzipped, zero runtime deps, no `Buffer`/Node-stream polyfills. Runs in browsers and Node. |
| **Server** | `ws-tcp` (`server/`) | Rust | Makes a WebSocket look like a TCP byte stream, so **any** HTTP/2 server can run over the tunnel — as a standalone proxy or in-process behind your routes. |

---

## Architecture

```
   ┌─────────────────────────┐        ┌──────────────────────────┐        ┌─────────────────────┐
   │  Browser / Node          │  wss   │  Rust WebSocket gateway   │  h2c   │  HTTP/2 server       │
   │  ── h2ts client (TS) ──  │ ─────▶ │  ── ws-tcp ──────────────  │ ─────▶ │  hyper / axum / any  │
   │  speaks real HTTP/2      │ frames │  WebSocket ⇄ raw TCP bytes │  TCP   │  h2c upstream        │
   └─────────────────────────┘ ◀───── └──────────────────────────┘ ◀───── └─────────────────────┘
          HPACK · multiplexing              full-duplex byte pump              plain HTTP/2 (cleartext)
          flow control · push
```

HTTP/2 frames are carried **inside** the WebSocket as binary messages. TLS is provided by `wss://` on the outside; the tunnelled HTTP/2 is cleartext (**h2c**, prior-knowledge). No TLS, ALPN, or `Upgrade` dance on the inside.

The server side supports two deployment shapes:

- **Standalone proxy** (`h2ts-proxy`) — terminates the WebSocket and forwards raw bytes to an upstream h2c server. A drop-in, in-Rust replacement for [`websockify`](https://github.com/novnc/websockify).
- **In-process** (`serve_h2`) — wrap your own hyper/axum/tower service and serve it directly over the tunnel, on any route.

---

## Features

### Client (`h2ts`, TypeScript)

- **Complete HTTP/2** — framing, stream multiplexing, connection + stream **flow control**, and **server push**.
- **Full HPACK** — a complete decoder (Huffman + dynamic table) and a compact encoder, validated against the RFC 7541 Appendix C test vectors.
- **`fetch`-like API** — request/response with **Web Streams** bodies (`ReadableStream<Uint8Array>`), plus `.text()`, `.json()`, `.bytes()`, `.arrayBuffer()`.
- **Pluggable transport** — runs over any byte duplex (`{ readable, writable }`). A WebSocket adapter is included; bring your own for anything else.
- **Tiny & dependency-free** — ~9 KB gzipped (28 KB minified), zero runtime dependencies. Built on `Uint8Array`/`DataView` and Web Streams — no `Buffer`, no `readable-stream`.
- **Browser + Node** — uses the platform's native `WebSocket`.

### Server (`ws-tcp`, Rust)

- **`WsByteStream`** — a WebSocket presented as `AsyncRead + AsyncWrite`. Anything that speaks TCP works over it unchanged.
- **`serve_h2(ws, service)`** — run **any** `hyper::Service` (a `service_fn`, an **`axum::Router`**, a `tower` service) as HTTP/2 over the tunnel.
- **`bridge(ws, peer)`** — full-duplex byte pump between a WebSocket and any `AsyncRead + AsyncWrite` peer.
- **`accept(&mut req)`** — the server-side WebSocket handshake as a plain hyper handler — **pluggable into any hyper/axum route**.
- **`h2ts-proxy`** — a ~90-line standalone WS→upstream-h2c proxy binary.

> The framing backend is [`fastwebsockets`](https://github.com/denoland/fastwebsockets) today (pure Rust, no C dependency). A [`wslay`](https://github.com/tatsuhiro-t/wslay)-based backend for true sub-frame streaming is planned behind the same API — see [Roadmap](#roadmap).

---

## Quick start

### Client

```ts
import { connectWebSocket } from "h2ts";

// Open the WebSocket and start an HTTP/2 client over it.
const client = await connectWebSocket("ws://localhost:8091", { protocols: "binary" });

// fetch-like requests, multiplexed over one connection.
const res = await client.request({
  method: "GET",
  path: "/hello",
  authority: "example.com",
});

console.log(res.status);          // 200
console.log(res.headers);         // { "content-type": "text/plain", ... }
console.log(await res.text());    // response body

// Stream a large response body without buffering it all:
const big = await client.request({ path: "/big", authority: "example.com" });
const reader = big.body.getReader();
for (let r; !(r = await reader.read()).done; ) {
  // r.value is a Uint8Array chunk
}

// Upload a body (Uint8Array, string, or a ReadableStream):
await client.request({ method: "POST", path: "/upload", authority: "example.com", body: bytes });

const rttMs = await client.ping();
await client.close();
```

Use `connect(transport, options)` if you already have a byte-duplex `Transport` (e.g. a non-WebSocket tunnel).

### Server — standalone proxy

```bash
cd server
cargo run -p h2ts-proxy -- 127.0.0.1:8091 127.0.0.1:8000
#                          └ listen (ws)   └ upstream h2c server
```

Now `connectWebSocket("ws://127.0.0.1:8091", …)` reaches the HTTP/2 server on `:8000`.

### Server — in-process, wrap any hyper service

```rust
use ws_tcp::{accept, serve_h2};

// In a hyper/axum route handler:
async fn on_ws(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, ws_tcp::WebSocketError> {
    let (response, ws_fut) = accept(&mut req)?;         // 101 back to the client
    tokio::spawn(async move {
        if let Ok(ws) = ws_fut.await {
            // `my_service` is any hyper Service — service_fn, axum::Router, tower…
            let _ = serve_h2(ws, my_service).await;
        }
    });
    Ok(response)
}
```

A runnable version lives in [`server/crates/ws-tcp/examples/h2-server.rs`](server/crates/ws-tcp/examples/h2-server.rs):

```bash
cd server
cargo run -p ws-tcp --example h2-server -- 127.0.0.1:8092
```

---

## Installation

**Client** — not yet published to npm. Build from source:

```bash
npm install
npm run build      # -> dist/ (ESM + .d.ts) via tsup
```

**Server** — a standard Cargo workspace:

```bash
cd server
cargo build --release
```

---

## API reference

### Client

`connectWebSocket(url, options?)` / `connect(transport, options?)` → `H2Connection`

| `H2Connection` | |
|---|---|
| `request(init): Promise<H2Response>` | Issue a request. `init`: `{ method?, path?, authority?, scheme?, headers?, body?, signal? }` |
| `ping(): Promise<number>` | Round-trip time in ms. |
| `close(): Promise<void>` | Graceful GOAWAY + teardown. |
| `ready` / `closed` | Promises for connection lifecycle. |

| `H2Response` | |
|---|---|
| `status` · `headers` · `rawHeaders` | Status code, joined headers, headers in order. |
| `body: ReadableStream<Uint8Array>` | Streaming response body. |
| `text()` · `json()` · `bytes()` · `arrayBuffer()` | Buffer the body. |
| `trailers()` | Trailers, after the body is consumed. |

Server push: pass `onPush` in `ConnectOptions`.

### Server (`ws-tcp`)

| Function | Purpose |
|---|---|
| `accept(&mut req) -> (Response, impl Future<WebSocket>)` | WebSocket handshake for any hyper route (item 4). |
| `serve_h2(ws, service)` | Serve any hyper `Service` as HTTP/2 over the tunnel (item 2). |
| `bridge(ws, peer)` | Full-duplex byte pump WS ⇄ peer (item 3 core). |
| `WsByteStream::from_websocket(ws)` | A WebSocket as `AsyncRead + AsyncWrite` (item 1). |

---

## Project layout

```
h2ts/
├── src/                        # h2ts client (TypeScript)
│   ├── hpack/                  #   HPACK: encoder, decoder, tables, Huffman
│   ├── frames/                 #   HTTP/2 frame codec
│   ├── connection.ts           #   multiplexer, request/response flow
│   ├── stream.ts · flow.ts     #   per-stream state, flow control
│   └── transport/              #   Transport interface + WebSocket adapter
├── test/                       # vitest unit tests + Node↔Rust e2e (test/e2e)
├── server/                     # Rust workspace
│   └── crates/
│       ├── ws-tcp/             #   the library (accept · bridge · WsByteStream · serve_h2)
│       │   ├── examples/       #     h2-server (in-process demo)
│       │   └── tests/          #     native integration tests
│       └── proxy/              #   h2ts-proxy binary (standalone)
└── poc/                        # original websockify proof-of-concept
```

---

## Development & testing

Two independent test suites, plus a cross-stack end-to-end check.

```bash
# Client (TypeScript)
npm test                    # vitest: HPACK (RFC 7541 vectors), frame codec, round-trips
npm run typecheck           # strict tsc

# Server (Rust)
cd server && cargo test     # unit + integration: bridge, WsByteStream, full h2-over-ws round trip

# End-to-end: h2ts client ↔ Rust server (no mocks)
npm run build
node poc/server.cjs &                                        # h2c echo server on :8000
cargo run -p h2ts-proxy -- 127.0.0.1:8091 127.0.0.1:8000 &   # WS gateway on :8091
WS_URL=ws://127.0.0.1:8091 node test/e2e/run.mjs
```

The e2e suite exercises the real path: routing, JSON, byte-exact uploads/downloads, concurrent multiplexed streams, ping, and streaming reads — and passes identically against `websockify`, the Rust proxy, and the in-process `serve_h2`.

---

## Roadmap

- [x] HTTP/2 client (framing, HPACK, flow control, multiplexing, push)
- [x] WebSocket transport + `fetch`-like API
- [x] Rust `ws-tcp`: `accept`, `bridge`, `WsByteStream`, `serve_h2`
- [x] `h2ts-proxy` standalone proxy
- [ ] **`wslay` framing backend** — true sub-frame streaming (never buffer a whole frame), behind the existing `bridge`/`WsByteStream` API. `fastwebsockets` buffers one frame at a time, which is bounded for h2ts traffic but not for arbitrary TCP.
- [ ] Publish `h2ts` to npm; publish `ws-tcp` to crates.io

---

## License

MIT — see [LICENSE](LICENSE).
