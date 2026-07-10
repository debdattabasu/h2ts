# h2ts-client

A from-scratch **HTTP/2 client** (RFC 7540 + HPACK/RFC 7541) for **Rust WASM frontends**, tunneled over a WebSocket — the Rust sibling of the TypeScript [`@debdattabasu/h2ts`](https://www.npmjs.com/package/@debdattabasu/h2ts) client.

It deliberately avoids `hyper`/`tokio` so it stays tiny in a `wasm32` bundle: a **sans-I/O protocol engine** (frames, HPACK, multiplexing, flow control) plus a pluggable `Transport`. The default `web` feature provides a browser `WebSocket` transport via `web-sys`. HTTP/2 is spoken with prior knowledge — no HTTP/1.1 `Upgrade` round-trip.

The point: Rust frontend devs (Leptos, Yew, Dioxus, …) get real multiplexed HTTP/2 with server push over a WebSocket tunnel, entirely in Rust, without dropping to JS.

The engine is a module-for-module port of the TypeScript client; both conform to one wire spec (`spec/protocol.md`) and the shared conformance suite — shared *behaviour*, not shared code.

## Part of h2ts

One of three implementations in the [h2ts monorepo](https://github.com/debdattabasu/h2ts), all sharing a single wire spec and conformance suite:

- **This crate (`h2ts-client`)** — the Rust/WASM frontend client.
- **TypeScript client — [`@debdattabasu/h2ts`](https://www.npmjs.com/package/@debdattabasu/h2ts)** — the browser/Node sibling, behavior-mirrored against this one.
- **Rust backend — [`h2ts-server`](https://crates.io/crates/h2ts-server)** — terminates the WebSocket and serves or proxies HTTP/2 over it (ships the `h2ts-proxy` binary). Point this client at any HTTP/2 origin through it (or through `websockify`).

## License

MIT.
