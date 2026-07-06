# h2ts-client

A from-scratch **HTTP/2 client** (RFC 7540 + HPACK/RFC 7541) for **Rust WASM frontends**, tunneled over a WebSocket — the Rust sibling of the TypeScript [`h2ts`](https://www.npmjs.com/package/h2ts) client.

It deliberately avoids `hyper`/`tokio` so it stays tiny in a `wasm32` bundle: a **sans-I/O protocol engine** (frames, HPACK, multiplexing, flow control) plus a pluggable `Transport`. The default `web` feature provides a browser `WebSocket` transport via `web-sys`. HTTP/2 is spoken with prior knowledge — no HTTP/1.1 `Upgrade` round-trip.

The point: Rust frontend devs (Leptos, Yew, Dioxus, …) get real multiplexed HTTP/2 with server push over a WebSocket tunnel, entirely in Rust, without dropping to JS.

> **Status: scaffold.** The engine is being ported module-for-module from the TypeScript client. Both clients conform to one wire spec (`spec/protocol.md`) and the shared conformance suite — shared *behaviour*, not shared code.

## License

MIT. Part of [h2ts](https://github.com/debdattabasu/h2ts).
