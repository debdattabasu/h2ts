// Package server is the Go server for h2ts: terminate a WebSocket tunnel and
// serve HTTP/2 (h2c, prior knowledge) over it in-process — the Go sibling of the
// Rust [h2ts-server].
//
// A frontend h2ts client speaks real HTTP/2 inside a WebSocket; this package is
// the gateway that terminates that WebSocket and runs an ordinary
// [net/http.Handler] as HTTP/2 on top of it. Two entry points:
//
//   - [Accept] performs the RFC 6455 server handshake on an
//     [net/http.ResponseWriter]/[net/http.Request], negotiating the h2ts
//     subprotocol, and returns the upgraded connection as a [*Conn] — the
//     WebSocket presented as a raw byte stream (a [net.Conn]).
//   - [ServeH2] hands that [*Conn] to a prior-knowledge h2c server
//     (golang.org/x/net/http2) and serves your handler as HTTP/2 over the tunnel.
//
// WebSocket message payloads become one continuous byte stream in each
// direction, so h2c framing rides straight through; message boundaries carry no
// meaning (see spec/protocol.md). WebSocket control frames are handled at the
// WebSocket layer — an inbound ping is auto-answered with a pong, a close ends
// the stream — and are never surfaced to the HTTP/2 layer. Optional
// server-initiated keepalive (on by default in [ServeH2]) pings an idle client
// and drops it if it stops responding, so a silently-dead browser can't leak the
// tunnel.
//
// Unlike the Rust server this package intentionally ships the in-process serve
// shape only; the standalone WS→upstream-h2c proxy is a single implementation,
// the Rust h2ts-proxy binary. A [*Conn] is a plain [net.Conn], though, so
// bridging one to a TCP upstream with io.Copy is straightforward if you want it.
//
// Conforms to the wire contract in spec/protocol.md; validated by conformance/.
//
// [h2ts-server]: https://crates.io/crates/h2ts-server
package server

// DefaultSubprotocol is the WebSocket subprotocol h2ts clients offer, and the one
// [Accept] requires by default. See [AcceptOptions] to relax that.
const DefaultSubprotocol = "h2ts"
