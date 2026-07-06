# The h2ts wire protocol

This is the language-neutral contract shared by every h2ts implementation — the TypeScript client, the Rust client, and every server (Rust, Go, Node). Implementations share **no code**; they stay compatible by conforming to this document and passing the [`conformance/`](../conformance) suite.

In one line: **HTTP/2 (h2c, prior knowledge) carried inside a WebSocket.**

```
┌────────────────────┐  wss / ws   ┌────────────────────────┐  h2c / TCP  ┌───────────────┐
│  client (frontend) │ ──────────▶ │  gateway (h2ts server) │ ──────────▶ │ HTTP/2 origin │
│  real HTTP/2       │  WS frames  │  WebSocket ⇄ raw bytes │  TCP bytes  │  (or served   │
│                    │ ◀────────── │                        │ ◀────────── │  in-process)  │
└────────────────────┘             └────────────────────────┘             └───────────────┘
```

## Roles

- **Client** — originates HTTP/2. Opens a WebSocket to a gateway and speaks HTTP/2 over it. (`h2ts` TS client, `h2ts-client` Rust crate.)
- **Gateway / server** — terminates the WebSocket and either forwards the raw byte stream to an upstream h2c origin (**proxy** shape) or serves an HTTP/2 service over it in-process (**serve** shape). (`h2ts-server` Rust, and the forthcoming Go/Node servers.)

## 1. Transport: WebSocket (RFC 6455)

- The tunnel is a single WebSocket connection. `wss://` provides TLS on the outside; the tunneled HTTP/2 is **cleartext** inside it.
- HTTP/2 bytes travel as WebSocket **binary** messages (opcode `0x2`). Text messages are not used for tunnel data.
- **Message boundaries are not significant.** A receiver MUST treat the concatenation of all binary-message payloads (in each direction) as one continuous byte stream and feed it to its HTTP/2 layer. A sender MAY split or coalesce bytes into binary messages however it likes. Neither side may assume a binary message equals an HTTP/2 frame.
- Applies symmetrically to both directions (client→gateway and gateway→client).

### Subprotocol negotiation (RFC 6455 §1.9, §4.2.2)

- The client MUST offer the **`h2ts`** subprotocol, and it MUST be offered **first** (`Sec-WebSocket-Protocol: h2ts, …`). A client MAY append others (e.g. `binary` to interoperate with `websockify`).
- The gateway sees the full offered list and echoes exactly one, or none:
  - If a handler selects one of the offered protocols, echo that.
  - Otherwise echo `h2ts` if the client offered it.
  - Otherwise (no `h2ts`, no selection) the gateway **rejects** the handshake with `400`, unless it is configured for a codec-agnostic tunnel (`allow_implicit_codec`), in which case it echoes the client's first offered subprotocol (or none if the client offered none).
- The gateway MUST NOT echo a subprotocol the client did not offer (RFC 6455 requires the client to fail such a connection).
- The negotiated subprotocol is informational to the tunnel — framing/behavior does not change based on it. Clients SHOULD expose it (e.g. `connection.protocol`).

## 2. HTTP/2 layer: h2c with prior knowledge (RFC 7540 §3.4)

Inside the byte stream the two sides speak ordinary HTTP/2, cleartext, with **prior knowledge**:

- The client MUST open by sending the connection preface — the octets `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` followed by a `SETTINGS` frame (RFC 7540 §3.5) — as its first bytes, and MAY send its first request(s) immediately, **without** waiting for the server's `SETTINGS`. There is **no** HTTP/1.1 `Upgrade: h2c` negotiation and **no** ALPN.
- The server sends its own connection preface (`SETTINGS`) as soon as the tunnel is established, per RFC 7540 — it does not wait for the client's preface.
- Everything above the byte stream is standard RFC 7540: stream multiplexing, HPACK (RFC 7541), connection- and stream-level flow control (§6.9), `PING`, `GOAWAY`, `RST_STREAM`, and optional server push (`PUSH_PROMISE`).
- No TLS, ALPN, or `Upgrade` dance occurs on the inside; that is the whole point of terminating TLS at the WebSocket and tunneling h2c.

## 3. Control frames & keepalive

WebSocket control frames are handled at the **WebSocket layer** and MUST NOT be surfaced to (or injected into) the HTTP/2 stream:

- **Ping/Pong (`0x9`/`0xA`)** — a receiver auto-answers a ping with a pong carrying the same payload. Neither the ping nor the pong appears in the tunneled byte stream. (Note: this is the *WebSocket* ping, distinct from an HTTP/2 `PING` frame, which rides inside the byte stream as normal h2.)
- **Close (`0x8`)** — a WebSocket close tears down the tunnel; a receiver surfaces the close code/reason to its application and treats the byte stream as ended (EOF to the HTTP/2 layer).
- **Keepalive** — a gateway MAY run server-initiated keepalive: periodically send a WebSocket ping when the connection is idle, and close it (recommended code `1001`, "going away") if no pong arrives within a timeout. This is optional and configurable; when disabled, the application may drive ping/pong itself.

## 4. Byte-stream framing on the gateway

Because message boundaries carry no meaning, a gateway that bridges the WebSocket to a raw TCP/byte peer MUST stream payloads incrementally and MUST NOT require a whole WebSocket frame to be buffered before forwarding — a single HTTP/2 DATA frame may be larger than any WebSocket frame, and vice versa. (The Rust server uses `wslay` with buffering off to guarantee this.)

## 5. Conformance

The [`conformance/`](../conformance) suite is the executable form of this document: a client runs a fixed battery of requests (routing, JSON, byte-exact upload/download, concurrent multiplexed streams, streaming reads, ping, 404) against a gateway selected by `WS_URL`. Any client × any gateway combination that passes is interoperable. New implementations MUST pass it.

## Status

Living document. Sections reflect the current implementations; extend it (never fork behavior silently) as features land — e.g. richer push semantics or negotiated settings.
