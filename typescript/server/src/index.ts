// @h2ts/server — Node.js server for h2ts: terminate the WebSocket tunnel and
// serve or proxy HTTP/2 over it. NOT YET IMPLEMENTED.
//
// Planned shape (mirrors the Rust `h2ts-server`):
//   - accept()  — the RFC 6455 handshake as a Node http handler, negotiating the
//                 `h2ts` subprotocol (see ../../../spec/protocol.md).
//   - proxy     — terminate the WS and pipe raw bytes to an upstream h2c server.
//   - serve     — bridge the WS byte stream to a `node:http2` (h2c) server in-process.
//
// Conforms to the wire contract in spec/protocol.md; validated by conformance/.

/** The WebSocket subprotocol h2ts clients offer (see the wire spec). */
export const DEFAULT_SUBPROTOCOL = "h2ts";
