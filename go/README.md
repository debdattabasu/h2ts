# h2ts (Go)

Go server for [h2ts](https://github.com/debdattabasu/h2ts): terminate a WebSocket tunnel and **serve HTTP/2 (h2c, prior knowledge) over it in-process** — the Go sibling of the Rust [`h2ts-server`](https://crates.io/crates/h2ts-server). Conforms to [`spec/protocol.md`](../spec/protocol.md) and passes the shared [`conformance/`](../conformance) suite against both the TypeScript and Rust clients.

Module: `github.com/debdattabasu/h2ts/go` · package `.../go/server`

```go
import "github.com/debdattabasu/h2ts/go/server"
```

## Surface

Two entry points. WebSocket message payloads become one continuous byte stream, so h2c framing rides straight through; ping/close are handled at the WebSocket layer and never surface to HTTP/2.

- **`Accept(w, r) (*Conn, error)`** — the RFC 6455 server handshake on a `net/http` handler, negotiating the `h2ts` subprotocol. Returns the upgraded WebSocket as a **`*Conn`** — a `net.Conn` presenting the tunnel as a raw byte stream (payloads stream incrementally; a `DATA` frame larger than any WebSocket frame, or vice versa, rides through untouched). `AcceptWithOptions` adds a custom subprotocol selector, `AllowImplicitCodec` (codec-agnostic tunnel), and an `AllowedOrigins` CSWSH allowlist.
- **`ServeH2(ws, handler) error`** — serve any `http.Handler` as HTTP/2 over the tunnel (via `golang.org/x/net/http2`), with server-initiated keepalive **on by default** (ping an idle client, drop it if it stops answering, so a silently-dead browser can't leak the tunnel). `ServeH2With` tunes/disables keepalive, supplies a configured `*http2.Server`, and takes control-frame callbacks fired inline on the read path — `OnClose(CloseFrame)` (why the tunnel ended), `OnPing([]byte)` / `OnPong([]byte)` (received ping/pong; pings are still auto-answered).

Control frames are handled for you (ping→pong, close→EOF), and you can drive your own: **`conn.Ping(payload)`** / **`conn.Pong(payload)`** send a control frame on a live tunnel (safe to call alongside the served traffic) — e.g. app-driven RTT probing, pairing `conn.Ping` with `OnPong`.

**Serve shape only, by design.** Unlike the Rust server, this package ships the in-process *serve* path only; the standalone WS→upstream-h2c **proxy is a single implementation**, the Rust [`h2ts-proxy`](https://crates.io/crates/h2ts-server) binary. A `*Conn` is a plain `net.Conn`, though, so bridging one to a TCP upstream with `io.Copy` is a few lines if you want it.

## Usage

The outer server must be **HTTP/1.1** so the upgrade request is hijackable — the HTTP/2 lives *inside* the tunnel.

```go
func upgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := server.Accept(w, r) // requires the h2ts subprotocol; writes the 4xx on reject
	if err != nil {
		return
	}
	go server.ServeH2(conn, myHandler) // blocks for the tunnel's lifetime
}

srv := &http.Server{
	Addr:    ":8093",
	Handler: http.HandlerFunc(upgrade),
	// keep Go from advertising h2 on the *outer* connection
	TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
}
log.Fatal(srv.ListenAndServe())
```

A runnable version — serving the conformance routes — is in [`examples/h2-server`](examples/h2-server):

```bash
go run ./examples/h2-server 127.0.0.1:8093
```

## Test

```bash
go test ./...          # framing, handshake negotiation, keepalive, h2-over-WS round-trips
go test -race ./...    # the Conn has concurrent writers (h2 writer, auto-pong, keepalive)

# Cross-stack e2e — the TS and Rust clients' battery against this Go gateway:
GATEWAY=go bash ../conformance/run.sh          # or: make conformance-go   (from repo root)
```

## License

MIT. Part of [h2ts](https://github.com/debdattabasu/h2ts).
