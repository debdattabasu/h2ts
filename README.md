# h2ts

> **HTTP/2 in the frontend — tunneled over WebSockets.** Native HTTP/2 clients and servers that carry HTTP/2 frames inside a WebSocket, one language at a time.

![license](https://img.shields.io/badge/license-MIT-blue)
![client](https://img.shields.io/badge/ts%20client-~9%20KB%20gzip-brightgreen)
![tests](https://img.shields.io/badge/tests-vitest%20%2B%20cargo-informational)

Browsers can't open raw TCP sockets or speak HTTP/2 with prior knowledge, and give you no control over framing, multiplexing, or server push. **h2ts** gives a frontend a real HTTP/2 client by carrying HTTP/2 frames inside a WebSocket, plus servers that terminate the WebSocket and hand the raw bytes to any HTTP/2 server.

It's a monorepo of **native, per-language implementations**. They share **no code** — they stay interoperable by conforming to one [wire spec](spec/protocol.md) and passing one [conformance suite](conformance). Shared *behavior*, not shared implementation.

## Packages

**Clients** originate HTTP/2 from a frontend; **servers** terminate the WebSocket and serve or proxy HTTP/2.

| | Client (frontend) | Server (gateway) |
|---|---|---|
| **TypeScript** | [`h2ts`](typescript/client) — the ~9 KB, zero-dep browser/Node client · *npm* | [`@h2ts/server`](typescript/server) · *planned* |
| **Rust** | [`h2ts-client`](rust/crates/h2ts-client) — for WASM frontends (Leptos/Yew/Dioxus), no hyper/tokio · *crates.io · scaffold* | [`h2ts-server`](rust/crates/h2ts-server) — hyper/axum/tower + the `h2ts-proxy` binary · *[crates.io](https://crates.io/crates/h2ts-server)* |
| **Go** | — | [`.../h2ts/go`](go) · *planned* |

Shared: [`spec/protocol.md`](spec/protocol.md) (the wire contract) · [`conformance/`](conformance) (cross-stack e2e) · [`wslay-sys`](rust/crates/wslay-sys) (wslay FFI — powers the Rust server's sub-frame streaming, [crates.io](https://crates.io/crates/wslay-sys)).

**Writing your frontend in Rust?** [`h2ts-client`](rust/crates/h2ts-client) is a from-scratch, sans-I/O HTTP/2 implementation built for `wasm32` — it **won't pull in `hyper`, `tokio`, or any other heavy async/server crate** — so Rust frontends (Leptos, Yew, Dioxus, …) get real multiplexed HTTP/2 with server push over a WebSocket, in Rust, without dropping to JS or bloating the bundle.

## Architecture

```
   ┌─────────────────────────┐        ┌──────────────────────────┐        ┌─────────────────────┐
   │  frontend (client)       │  wss   │  gateway (h2ts server)    │  h2c   │  HTTP/2 server       │
   │  ── h2ts / h2ts-client ─ │ ─────▶ │  ── terminates the WS ──────── │ ─────▶ │  hyper / axum / any  │
   │  speaks real HTTP/2      │ frames │  WebSocket ⇄ raw TCP bytes │  TCP   │  h2c upstream        │
   └─────────────────────────┘ ◀───── └──────────────────────────┘ ◀───── └─────────────────────┘
          HPACK · multiplexing              full-duplex byte pump              plain HTTP/2 (cleartext)
          flow control · push
```

HTTP/2 frames ride **inside** the WebSocket as binary messages. TLS is provided by `wss://` on the outside; the tunneled HTTP/2 is cleartext (**h2c**, prior-knowledge) — no TLS, ALPN, or `Upgrade` dance on the inside. The client offers the **`h2ts`** subprotocol; the gateway negotiates it and rejects clients that don't (unless configured otherwise). Full details — subprotocol negotiation, control frames, keepalive — are in [**`spec/protocol.md`**](spec/protocol.md).

Servers come in two shapes: a standalone **proxy** (`h2ts-proxy` — forward raw bytes to an upstream h2c server, a drop-in [`websockify`](https://github.com/novnc/websockify) replacement) or **in-process** (serve your own service over the tunnel).

## Repo layout

```
h2ts/
├── spec/
│   └── protocol.md             # the language-neutral wire contract
├── conformance/                # cross-stack e2e (any client × any gateway, by WS_URL)
├── typescript/                 # npm workspace
│   ├── client/                 #   h2ts — the TypeScript client
│   └── server/                 #   @h2ts/server (planned)
├── rust/                       # Cargo workspace
│   └── crates/
│       ├── h2ts-client/        #   Rust client for WASM frontends (scaffold)
│       ├── h2ts-server/        #   server library + h2ts-proxy binary
│       └── wslay-sys/          #   wslay FFI framing backend
├── go/                         # Go module (planned)
│   └── server/
└── Makefile                    # top-level tasks (fan out to each stack)
```

## Build & test

The top-level `Makefile` fans out across every stack; or drive each directly.

```bash
make test          # everything: rust + typescript + conformance
make conformance   # cross-stack e2e (builds the client, starts origin + proxy, runs checks)

# or per stack:
cd rust && cargo test
cd typescript && npm install && npm test -w h2ts
```

The conformance suite runs a fixed battery — routing, JSON, byte-exact uploads/downloads, concurrent multiplexed streams, streaming reads, ping, 404 — and passes identically against the Rust proxy (`h2ts-proxy`) and the in-process `serve_h2` (`h2-server` example). Per-package usage lives in each package's README: [`h2ts` client](typescript/client), [`h2ts-server`](rust/crates/h2ts-server), [`h2ts-client`](rust/crates/h2ts-client).

## Roadmap

- [x] TypeScript client `h2ts` — HTTP/2 (framing, HPACK, flow control, multiplexing, push), WebSocket transport, `fetch`-like API
- [x] Rust server `h2ts-server` — `accept`, `bridge`, `WsByteStream`, `serve_h2`, the `h2ts-proxy` binary, and `wslay` sub-frame streaming (via `wslay-sys`)
- [x] Publish [`h2ts-server`](https://crates.io/crates/h2ts-server) + [`wslay-sys`](https://crates.io/crates/wslay-sys) to crates.io
- [x] Monorepo restructure: one wire spec + conformance suite across languages
- [ ] **`h2ts-client` (Rust)** — port the TS engine to a `wasm32`, no-hyper client for Rust frontends *(scaffolded)*
- [ ] Publish `h2ts` client to npm
- [ ] **Go server** — terminate the tunnel and serve/proxy HTTP/2 in Go *(scaffolded)*
- [ ] **Node.js server** (`@h2ts/server`) — serve a `node:http2` service over the tunnel *(scaffolded)*
- [ ] **Envoy filter** — terminate the WebSocket tunnel as an Envoy HTTP filter, to run the gateway inside an existing Envoy/proxy mesh

## License

MIT — see [LICENSE](LICENSE).
