# h2ts (Go)

Go server for [h2ts](https://github.com/debdattabasu/h2ts): terminate the WebSocket tunnel and **serve or proxy HTTP/2** over it — the Go sibling of the Rust [`h2ts-server`](https://crates.io/crates/h2ts-server).

Module: `github.com/debdattabasu/h2ts/go`

> **Status: scaffold — not yet implemented.**

Planned surface (mirrors `h2ts-server`), all conforming to [`spec/protocol.md`](../spec/protocol.md):

- **Accept** — the RFC 6455 handshake as an `http.Handler`, negotiating the `h2ts` subprotocol.
- **Proxy** — terminate the WS and pipe raw bytes to an upstream h2c server.
- **Serve** — bridge the WS byte stream to a `net/http` h2c server in-process.

## License

MIT. Part of [h2ts](https://github.com/debdattabasu/h2ts).
