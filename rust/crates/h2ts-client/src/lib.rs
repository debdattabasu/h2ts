//! # h2ts-client
//!
//! A from-scratch HTTP/2 client (RFC 7540 + HPACK/RFC 7541) for **Rust WASM
//! frontends**, tunneled over a WebSocket — the Rust sibling of the TypeScript
//! [`h2ts`] client. It deliberately avoids `hyper`/`tokio` so it stays tiny in a
//! `wasm32` bundle: a **sans-I/O protocol engine** plus a pluggable [`Transport`].
//! HTTP/2 is spoken with prior knowledge (no HTTP/1.1 `Upgrade` round-trip).
//!
//! The protocol engine is a module-for-module port of the TypeScript client
//! (`typescript/client/src`). Both clients conform to one wire spec
//! (`spec/protocol.md`) and the shared conformance suite (`conformance/`) — that
//! shared *behaviour*, not shared code, is what keeps them from diverging.
//!
//! | module          | ports from (TS)     | responsibility                         |
//! |-----------------|---------------------|----------------------------------------|
//! | [`frames`]      | `frames/`           | HTTP/2 frame codec                     |
//! | [`hpack`]       | `hpack/`            | HPACK encoder + decoder                |
//! | [`flow`]        | `flow.ts`           | connection/stream flow control         |
//! | [`connection`]  | `connection.ts`     | multiplexer, request/response flow     |
//! | [`transport`]   | `transport/`        | the pluggable byte-duplex              |
//!
//! The default `web` feature provides a browser `WebSocket` [`Transport`] via
//! `web-sys`; disable it for host-side engine tests.
//!
//! Terminate the WebSocket with the Rust server [`h2ts-server`] (its `h2ts-proxy`
//! binary, or `websockify`) to reach any HTTP/2 origin.
//!
//! [`h2ts`]: https://www.npmjs.com/package/@debdattabasu/h2ts
//! [`h2ts-server`]: https://crates.io/crates/h2ts-server

mod bytes;
pub mod connection;
pub mod errors;
pub mod flow;
pub mod frames;
pub mod hpack;
pub mod pool;
pub mod transport;

/// The WebSocket subprotocol an h2ts client offers by default (echoed by the
/// gateway). Offer it first; see `spec/protocol.md`.
pub const DEFAULT_SUBPROTOCOL: &str = "h2ts";

pub use connection::{
    connect, ConnectOptions, H2Connection, RequestBody, RequestInit, Response, ResponseBody,
};
pub use errors::{ErrorCode, H2Error};
pub use hpack::Header;
pub use pool::{H2Pool, PoolConnection};
pub use transport::{ByteSink, ByteStream, Transport, TransportError};

// Browser WebSocket transport — wasm32 only, behind the default `web` feature.
#[cfg(all(feature = "web", target_arch = "wasm32"))]
mod web;
#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use web::{connect_pool, connect_websocket, websocket_transport};
