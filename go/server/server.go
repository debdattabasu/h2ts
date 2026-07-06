// Package server is the Go server for h2ts: terminate the WebSocket tunnel and
// serve or proxy HTTP/2 over it. NOT YET IMPLEMENTED.
//
// Planned shape (mirrors the Rust h2ts-server):
//
//   - Accept: the RFC 6455 handshake as an http.Handler, negotiating the `h2ts`
//     subprotocol (see ../../spec/protocol.md).
//   - Proxy:  terminate the WS and pipe raw bytes to an upstream h2c server.
//   - Serve:  bridge the WS byte stream to a net/http h2c server in-process.
//
// Conforms to the wire contract in spec/protocol.md; validated by conformance/.
package server

// DefaultSubprotocol is the WebSocket subprotocol h2ts clients offer.
const DefaultSubprotocol = "h2ts"
