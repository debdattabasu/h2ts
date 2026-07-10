# h2ts Go server — flow-control & test-coverage audit

_Audit date: 2026-07-10. Scope: the Go serve gateway (`go/server`, `go/examples/h2-server`),
landed in `feat: Go server — serve HTTP/2 (h2c) over the WebSocket tunnel`._

**Method:** read the full Go source + tests; read the relevant `golang.org/x/net/http2`
server internals (flow-control window defaults, write-deadline handling); compared the
suite against the Rust server's 48-test suite (`rust/crates/h2ts-server/tests`); ran
`go test -race ./...` (all pass) and the shared conformance battery under both the
TypeScript and Rust clients against the Go gateway (`GATEWAY=go` — all pass).

## TL;DR

- **Flow control is correct and well-provisioned**, and has *tighter* backpressure than the
  Rust server (the Go `Conn` reads/writes the socket directly; there is no intermediate
  buffer like Rust's 64 KiB `tokio::io::duplex`).
- **One genuine interaction gap:** control-frame writes share one `writeMu` with data
  writes, so server-initiated keepalive can't ping (and an inbound ping can't be
  auto-ponged) *while a data write is blocked on a full TCP send buffer*. Narrow, shared
  with the Rust design, and mitigable with `http2.Server.WriteByteTimeout`. Not a
  conformance-breaking bug.
- **Coverage is solid on framing/handshake/happy-path but thin on the flow-control and
  lifecycle *edges*** the Rust suite pins. This doc drives P1 (flow-control/lifecycle) and
  P2 (receive-path robustness) to close that.

---

## 1. Flow-control analysis

The tunnel carries **h2c bytes inside WebSocket binary frames**, so two independent
flow-control systems meet at the gateway:

| Layer | Enforced by | In the Go server |
|---|---|---|
| HTTP/2 conn + stream windows (`WINDOW_UPDATE`, RFC 7540 §6.9) | `x/net/http2` server; the client on its side | Server advertises **1 MiB** conn + **1 MiB** per-stream receive windows (`config.go` `MaxUploadBufferPerConnection`/`PerStream` default `1<<20`); `MaxConcurrentStreams` 250 |
| TCP backpressure on the tunnel socket | the kernel | `Conn.Read` is pull-based; `Conn.Write` writes straight to the socket |

**Inbound (client→server upload):** the h2 server pulls request bytes via `Conn.Read` as
fast as the handler consumes them. A slow handler → the stream's 1 MiB window fills → the
server withholds `WINDOW_UPDATE` → the client stops; if it ignores that, the TCP recv
buffer fills and TCP stalls it. Doubly bounded.

**Outbound (server→client download):** the h2 write loop calls `Conn.Write` only within the
*client's* advertised window; each call emits one WS binary frame straight to the socket. A
slow client → full TCP send buffer → `Conn.Write` blocks → the h2 write loop blocks.
Correct backpressure.

### What's correct (with evidence)

- **No hidden buffer.** The Go `Conn` reads/writes the socket directly (`c.r.Read(p)`
  streams payload into the caller's buffer; `Write` does `net.Buffers{hdr, p}.WriteTo`),
  vs. Rust's 64 KiB duplex between wslay and hyper. Backpressure is *tighter*.
- **Incremental, never buffers a frame** (`TestConnStreamsFrameIncrementally`) — spec §4.
- **Windows are generous** (1 MiB), so the conformance 512 KiB upload / 256 KiB download
  flow without stalling, validated against both clients.
- **WS frame integrity under concurrent writers:** `Conn.Write` holds `writeMu` for the
  whole frame (header+payload), and every control write takes the same lock, so a
  ping/pong/close can only land *between* complete data frames — never mid-frame.

### The one genuine gap — control-write starvation behind a blocked data write

`Conn.Write` and `writeControl` share one `writeMu`. If a client stops reading,
`net.Buffers.WriteTo` blocks on a full TCP send buffer **while holding `writeMu`**, so the
keepalive ping can't be sent (its `writeControl` waits on the lock) and an inbound ping
can't be auto-ponged until the write drains. For "client dies *mid-write*," liveness
detection falls back from the keepalive timeout (seconds) to the OS TCP timeout (minutes),
because `&http2.Server{}` sets no `WriteByteTimeout` by default.

- **Shared with the Rust server** (same single-`ws_write`-mutex contention), not a Go
  regression.
- **Real h2ts clients never trigger the auto-pong path** — their `ping()` is an HTTP/2
  PING inside the byte stream, not a WS ping.
- **Mitigation exists:** `ServeConfig.Server = &http2.Server{WriteByteTimeout: 30*time.Second}`
  makes the h2 layer `SetWriteDeadline` our `Conn` (delegated to the TCP socket), so a
  wedged write errors and tears down instead of pinning `writeMu`. Candidate to default in
  `ServeH2` (see P3).

No deadlock / data-loss / unbounded-memory path exists in normal operation. Keepalive
interacts correctly with active transfers: `lastActivity` is refreshed by *every* inbound
frame (including the client's `WINDOW_UPDATE`s during a download and DATA during an upload),
so keepalive only pings on true idle.

---

## 2. Test-coverage gaps

Strong on framing, handshake negotiation, and the happy h2 path; thinner than the Rust
suite on flow-control/lifecycle/failure edges.

### P1 — flow-control & lifecycle (highest value; mirror the Rust suite)

1. **Backpressure propagation** — Rust `tests/backpressure.rs`. Prove `Conn.Write` stalls
   on a paused consumer (doesn't absorb unbounded data), then drains byte-exact.
2. **Control frame during an active large transfer** — Rust `serve_h2_with.rs`. Ping while
   a large h2 transfer runs; assert byte-exactness (a ping can't corrupt h2 DATA — pins the
   `writeMu` frame-integrity guarantee under load).
3. **Keepalive positive path** — Rust `keepalive_stays_up_while_peer_responds`. A peer that
   pongs stays alive across many intervals; no close.
4. **Large upload in-suite** — `go test` never does a big POST (only the external-client
   conformance does 512 KiB). A ≥1 MiB upload echo exercises the receive-window path
   directly.

### P2 — receive-path robustness (protocol-error arms are code without tests)

5. **Fragmented data message** — `Conn` streams continuation frames as data, but no test
   feeds a `FIN=0` binary + continuation sequence (browsers can fragment), nor a control
   frame interleaved between fragments (RFC 6455 §5.4 allows it).
6. **Protocol-error paths** — only *unmasked* is tested. Untested `readHeader`/dispatch
   rejections → 1002: RSV≠0, reserved opcode, fragmented control frame, control payload
   >125.
7. **Custom keepalive close frame** — Rust `keepalive_uses_a_custom_close_frame`. Assert a
   custom `KeepAlive.Close` code/reason is honored, not just the default 1001.

### P3 — edges / defensive (come back to)

8. Empty (zero-length) data frame skipped without EOF; empty-payload peer close parsed as
   1005; non-hijackable `ResponseWriter` → 500 (the `!ok` arm); `ServeConfig.Server`
   custom settings applied; empty `Write` no-op.
9. **Decision:** default `WriteByteTimeout` in `ServeH2` to close the §1 interaction gap.

None of these indicate a bug — they're unproven paths. #1 and #2 pin the exact
flow-control guarantees above.

---

## Work log

- [x] **P1** — _done 2026-07-10._ Four tests, all green under `-race`:
  - `TestConnBackpressure` — `Conn.Write` stalls with a paused consumer (16 MiB, no reader
    → writer blocks, doesn't absorb it), then drains byte-exact once read.
  - `TestServeH2ControlFramesDoNotCorruptData` — pings every 150 µs during 4× (256 KiB
    download + 512 KiB upload echo); all payloads byte-exact (pins the `writeMu`
    frame-integrity guarantee under load).
  - `TestServeH2KeepAliveStaysUpWhilePeerResponds` — a 20 ms keepalive + a client that
    auto-pongs stays up across ~10 intervals; `OnClose` never fires; still serves after.
  - `TestServeH2LargeUpload` — 1 MiB POST echo byte-exact (in-suite receive-window path).
- [x] **P2** — _done 2026-07-10._ Three tests:
  - `TestConnReadsFragmentedMessage` — a `FIN=0` binary + continuation reassembles, and a
    ping interleaved between the fragments is auto-answered.
  - `TestConnRejectsProtocolViolations` — RSV bits, reserved data opcode, reserved control
    opcode, fragmented control frame, oversized (>125) control frame → each ends with a
    1002 close.
  - `TestKeepAliveUsesCustomCloseFrame` — a custom `KeepAlive.Close` (4020 "custom-bye") is
    the frame the peer receives and the surfaced `CloseReason`.
  - Test frame builder generalized (`clientFrameFin`) to construct fragmented/partial frames.
- [x] **P3** — _done 2026-07-10._ Edge tests + the `WriteByteTimeout` decision:
  - `TestConnSkipsEmptyDataFrame` — a zero-length data frame contributes nothing, no
    spurious EOF.
  - `TestConnEmptyCloseIsNoStatus` — an empty Close → 1005, echoed as an empty close.
  - `TestConnEmptyWriteIsNoop` — `Write(nil)` writes nothing, no error.
  - `TestAcceptNonHijackable` — a non-hijackable `ResponseWriter` → 500 `HandshakeError`.
  - `TestServeH2CustomServer` — the `ServeConfig.Server` path serves end to end.
  - **`WriteByteTimeout` — decided: don't default it; document + recommend the knob.**
    Rationale: (1) _parity_ — the Rust server has no equivalent and also falls back to the
    OS TCP timeout for "client dies mid-write," so not defaulting keeps the two servers
    consistent; (2) _least surprise_ — a blanket write timeout can tear down a legitimately
    slow-but-alive download, and the right value is deployment-specific; (3) the knob
    already exists (`ServeConfig.Server = &http2.Server{WriteByteTimeout: …}`) and is now
    pointed to from the `ServeConfig.Server` doc comment. Flipping to a default (tied to
    keepalive) is a one-line change if we later want Go to exceed the shared baseline.

Suite: **35 tests + subtests, green under `-race`**; conformance still passes under both
clients (`GATEWAY=go`).

## Remaining / not pursued

- **Defaulting `WriteByteTimeout`** — deliberately deferred (see P3); opt-in via
  `ServeConfig.Server` for now.
- **Enforcement-level custom-server test** (e.g. the 6th concurrent stream is refused when
  `MaxConcurrentStreams: 5`) — `TestServeH2CustomServer` only exercises the path; asserting
  the *setting takes effect* needs concurrent long-lived streams. Low value; skipped.
- **`Upgrade`-failure (post-101) arm** — like the Rust server's 500 `Upgrade(_)` case, this
  is only reachable through a live post-handshake transport failure and isn't
  unit-constructible; left uncovered.
