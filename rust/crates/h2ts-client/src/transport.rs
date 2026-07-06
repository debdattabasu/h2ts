//! The byte-duplex a connection runs over — the Rust analogue of the TypeScript
//! `Transport` (`{ readable, writable }`). Anything expressible as a pair of byte
//! streams works: a browser `WebSocket` (behind the `web` feature), an in-memory
//! pair for tests, etc.
//!
//! TODO (port): settle the async surface (likely `poll`-based or a small async
//! read/write trait that works on `wasm32` without tokio) while porting the
//! engine from `typescript/client/src/transport/`.

/// A bidirectional byte stream the HTTP/2 engine drives. Design in progress.
pub trait Transport {
    // TODO: define the async read/write surface during the engine port.
}
