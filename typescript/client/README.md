# h2ts

**A tiny HTTP/2 client for the browser, tunneled over a WebSocket** (or any byte duplex). From-scratch HTTP/2 (RFC 7540) + HPACK (RFC 7541) in idiomatic TypeScript — **~9 KB gzipped, zero runtime dependencies**, no `Buffer` or Node-stream polyfills. Runs in browsers and Node.

Browsers can't open raw TCP sockets or speak HTTP/2 with prior knowledge. `h2ts` gives a browser a real HTTP/2 client by carrying HTTP/2 frames **inside** a WebSocket; a small server side (the [`h2ts-server`](https://crates.io/crates/h2ts-server) Rust crate, or `websockify`) terminates the WebSocket and hands the raw bytes to any HTTP/2 server.

## Features

- **Complete HTTP/2** — framing, stream multiplexing, connection + stream **flow control**, and **server push**.
- **Full HPACK** — a complete decoder (Huffman + dynamic table) and a compact encoder, validated against the RFC 7541 Appendix C vectors.
- **`fetch`-like API** — request/response with **Web Streams** bodies (`ReadableStream<Uint8Array>`), plus `.text()`, `.json()`, `.bytes()`, `.arrayBuffer()`.
- **Prior knowledge** — opens with the HTTP/2 preface and issues the first request immediately; no HTTP/1.1 `Upgrade` round-trip.
- **Pluggable transport** — runs over any byte duplex (`{ readable, writable }`). A WebSocket adapter is included; bring your own for anything else.
- **Tiny & dependency-free** — ~9 KB gzipped, zero runtime deps. Built on `Uint8Array`/`DataView` and Web Streams.

## Install

```bash
npm install @debdattabasu/h2ts
```

## Usage

```ts
import { connectWebSocket } from "@debdattabasu/h2ts";

// Open the WebSocket and start an HTTP/2 client over it. The `h2ts` subprotocol
// is offered by default; append your own via `{ protocols: [...] }`.
const client = await connectWebSocket("ws://localhost:8091");
console.log(client.protocol); // negotiated subprotocol, e.g. "h2ts"

// fetch-like requests, multiplexed over one connection.
const res = await client.request({ method: "GET", path: "/hello", authority: "example.com" });
console.log(res.status);        // 200
console.log(await res.text());  // response body

// Stream a large body without buffering it all:
const big = await client.request({ path: "/big", authority: "example.com" });
for await (const chunk of big.body) { /* Uint8Array */ }

// Upload (Uint8Array, string, or a ReadableStream):
await client.request({ method: "POST", path: "/upload", authority: "example.com", body: bytes });

const rttMs = await client.ping();
await client.close();
```

Already have a byte-duplex `Transport` (a non-WebSocket tunnel)? Use `connect(transport, options)` directly.

## Part of h2ts

This is the TypeScript client — one of three implementations in the [h2ts monorepo](https://github.com/debdattabasu/h2ts), all sharing a single wire spec and conformance suite:

- **This package (`@debdattabasu/h2ts`)** — the browser/Node client, in TypeScript.
- **Rust backend — [`h2ts-server`](https://crates.io/crates/h2ts-server)** — makes a WebSocket look like raw TCP and serves or proxies HTTP/2 over it; ships the `h2ts-proxy` binary (a drop-in `websockify`).
- **Rust frontend — [`h2ts-client`](https://github.com/debdattabasu/h2ts/tree/main/rust/crates/h2ts-client)** — the same client for Rust/WASM frontends (no `hyper`, no `tokio`), behavior-mirrored against this one by the shared conformance battery.

Point `h2ts` at any HTTP/2 server by terminating the WebSocket with `h2ts-server` / `h2ts-proxy` (or `websockify`).

## License

MIT
