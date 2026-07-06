# @h2ts/server

Node.js server for [h2ts](https://github.com/debdattabasu/h2ts): terminate the WebSocket tunnel and **serve or proxy HTTP/2** over it — the Node sibling of the Rust [`h2ts-server`](https://crates.io/crates/h2ts-server).

> **Status: scaffold — not yet implemented.**

Planned surface (mirrors `h2ts-server`), all conforming to [`spec/protocol.md`](../../spec/protocol.md):

- **`accept`** — the RFC 6455 handshake as a Node http handler, negotiating the `h2ts` subprotocol.
- **proxy** — terminate the WS and pipe raw bytes to an upstream h2c server.
- **serve** — bridge the WS byte stream to a `node:http2` (h2c) server in-process.

## License

MIT. Part of [h2ts](https://github.com/debdattabasu/h2ts).
