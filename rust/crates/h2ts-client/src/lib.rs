//! # h2ts-client
//!
//! A from-scratch HTTP/2 client (RFC 7540 + HPACK/RFC 7541) for **Rust WASM
//! frontends**, tunneled over a WebSocket — the Rust sibling of the TypeScript
//! [`h2ts`] client. It deliberately avoids `hyper`/`tokio` so it stays tiny in a
//! `wasm32` bundle: a **sans-I/O protocol engine** plus a pluggable [`Transport`].
//! HTTP/2 is spoken with prior knowledge (no HTTP/1.1 `Upgrade` round-trip).
//!
//! ## Status: scaffold
//!
//! The protocol engine is being ported, module-for-module, from the TypeScript
//! client (`typescript/client/src`). Both clients conform to one wire spec
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
//! The default `web` feature will provide a browser `WebSocket` [`Transport`] via
//! `web-sys`; disable it for host-side engine tests.
//!
//! [`h2ts`]: https://www.npmjs.com/package/h2ts

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

pub use connection::{connect, ConnectOptions, H2Connection, RequestBody, RequestInit, Response};
pub use errors::{ErrorCode, H2Error};
pub use hpack::Header;
pub use pool::{H2Pool, PoolConnection};
pub use transport::{ByteSink, ByteStream, Transport, TransportError};

// Browser WebSocket transport — wasm32 only, behind the default `web` feature.
#[cfg(all(feature = "web", target_arch = "wasm32"))]
mod web;
#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use web::{connect_pool, connect_websocket, websocket_transport};
