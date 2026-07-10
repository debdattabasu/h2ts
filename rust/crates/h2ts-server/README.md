# h2ts-server

Make a WebSocket look like a raw TCP byte stream — then **serve or proxy HTTP/2 over it**.

`h2ts-server` is the Rust server side of [**h2ts**](https://github.com/debdattabasu/h2ts), which gives browsers a real HTTP/2 client by tunnelling HTTP/2 frames inside a WebSocket. This crate terminates that WebSocket and hands the raw bytes to any HTTP/2 server — as a standalone proxy, or in-process behind your own routes. It pairs with the [`@debdattabasu/h2ts`](https://www.npmjs.com/package/@debdattabasu/h2ts) TypeScript client and the [`h2ts-client`](https://crates.io/crates/h2ts-client) Rust/WASM client — or any client that forwards a byte stream over a WebSocket.

A WebSocket carries discrete messages; HTTP/2 needs a continuous byte stream. This crate bridges the two — WebSocket message *payloads* become a byte stream, so h2c (cleartext HTTP/2, prior-knowledge) framing rides straight through.

## Entry points

- **`WsByteStream`** — a WebSocket presented as `AsyncRead + AsyncWrite`. Anything that speaks TCP works over it unchanged.
- **`serve_h2(ws, service)`** — run **any** `hyper::Service` (a `service_fn`, an `axum::Router`, a `tower` service) as HTTP/2 over the tunnel.
- **`bridge(ws, peer)`** — full-duplex byte pump between a WebSocket and any `AsyncRead + AsyncWrite` peer (a TCP upstream, an in-process server, …).
- **`accept(&mut req)`** — the server-side WebSocket handshake as a plain hyper handler, pluggable into any hyper/axum route.
- **`h2ts-proxy`** — a standalone WS → upstream-h2c proxy **binary** that ships with the crate; a drop-in, in-Rust replacement for [`websockify`](https://github.com/novnc/websockify).

## Sub-frame streaming

Framing is done by [wslay](https://github.com/tatsuhiro-t/wslay) (vendored C, via the [`wslay-sys`](https://crates.io/crates/wslay-sys) crate). Driven through its event API with buffering off, it streams each frame's payload **incrementally** — it never buffers a whole frame, no matter how large — which matters for a proxy carrying arbitrary TCP. The RFC 6455 handshake is done in-crate (`sha1` + `base64`); there is no pure-Rust WebSocket-framing dependency.

## Usage

### In-process — serve any hyper service over the tunnel

```rust
use h2ts_server::{accept, serve_h2};

// In a hyper/axum route handler:
async fn on_ws(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, h2ts_server::WebSocketError> {
    let (response, ws_fut) = match accept(&mut req) {
        Ok(pair) => pair,
        // e.g. a client that didn't offer the `h2ts` subprotocol.
        Err(err) => return Ok(err.rejection_response()), // send the 4xx back
    };
    tokio::spawn(async move {
        if let Ok(ws) = ws_fut.await {
            let _ = serve_h2(ws, my_service).await; // any hyper Service
        }
    });
    Ok(response) // send the 101 back
}
```

Prefer a raw byte tunnel instead of HTTP/2? Hand the upgraded `ws` to `WsByteStream::new(ws)` (`AsyncRead + AsyncWrite`) or `bridge(ws, peer)`.

### Standalone proxy

```bash
cargo install h2ts-server
h2ts-proxy 127.0.0.1:8091 127.0.0.1:8000 30
#          └ listen (ws)   └ upstream h2c  └ keepalive secs (0/omit = off)
```

Now a WebSocket client connecting to `ws://127.0.0.1:8091` reaches the HTTP/2 server on `:8000`. The proxy requires the `h2ts` subprotocol by default; add `--allow-implicit-codec` to accept any offered subprotocol (a generic byte tunnel / `websockify` replacement).

## Control frames & keepalive

wslay auto-answers pings with pongs and echoes closes. A `BridgeConfig` (passed via the `*_with_config` / `bridge_with` / `serve_h2_with` variants) lets you go further — and it applies identically to **both** the raw-byte and HTTP/2 pathways, since control frames are handled at the WebSocket layer beneath:

- **Observe** every control frame: `on_ping`, `on_pong`, and `on_close` (which fires once with *why* the tunnel ended — peer close, keepalive timeout, or `1006` abnormal).
- **Send** your own control frames via a `control_channel()`: `WsControl::ping` / `pong` / `close`.
- Set the **close code + reason** sent on teardown.
- Turn on server-initiated **keepalive** (`KeepAlive`): ping when idle, close if no pong arrives in time.

## Subprotocol negotiation

The client offers the `h2ts` subprotocol. `accept` echoes it and **rejects** a client that doesn't offer it (`400` via `err.rejection_response()`). `accept_with` hands your handler the full offered list to choose from; `accept_with_options` with `allow_implicit_codec` accepts whatever codec the client offered first.

## Build requirements

This crate depends on `wslay-sys`, which compiles vendored C and generates bindings at build time:

- A C compiler (`cc` finds one automatically).
- `libclang` (required by `bindgen`). macOS: ships with the Command Line Tools. Debian/Ubuntu: `apt install libclang-dev`.

## License

MIT. Part of [h2ts](https://github.com/debdattabasu/h2ts).
