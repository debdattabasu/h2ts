//! The byte-duplex a connection runs over — the Rust analogue of the TS
//! `Transport` (`{ readable, writable }`). Anything expressible as a pair of byte
//! streams works: a browser `WebSocket` (behind the `web` feature), an in-memory
//! channel pair for tests, etc.
//!
//! It is deliberately I/O-model-agnostic: a [`Stream`] of inbound chunks and a
//! [`Sink`] of outbound chunks, both boxed so the connection is not generic. This
//! uses only `futures` — no tokio, no hyper — so it compiles for `wasm32`.

use std::pin::Pin;

use futures::{Sink, Stream};

/// An error from the underlying transport (socket closed, send failed, …).
#[derive(Debug, Clone)]
pub struct TransportError(pub String);

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TransportError {}

/// Inbound byte chunks from the peer. The stream ending signals EOF.
pub type ByteStream = Pin<Box<dyn Stream<Item = Vec<u8>>>>;

/// Outbound byte chunks to the peer.
pub type ByteSink = Pin<Box<dyn Sink<Vec<u8>, Error = TransportError>>>;

/// A bidirectional byte transport: a reader (inbound) and a writer (outbound).
pub struct Transport {
    pub reader: ByteStream,
    pub writer: ByteSink,
}

impl Transport {
    pub fn new(reader: ByteStream, writer: ByteSink) -> Self {
        Self { reader, writer }
    }
}
