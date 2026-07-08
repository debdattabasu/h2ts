# h2ts hardening audit — 2026-07-07 (round 2)

_Independent re-audit of the conclusion in [`status.md`](status.md) ("no open defects").
Scope: TypeScript client (`typescript/client`), Rust client (`rust/crates/h2ts-client`),
Rust server (`rust/crates/h2ts-server`), conformance suite. Method: full re-read of both
client engines against RFC 7540/9113, two throwaway probes to settle the bidi/flow-control
question empirically (both reverted), plus a mapped coverage matrix of the whole test suite._

## TL;DR

The prior audit's "no open defects" conclusion **did not hold**. Bidirectional streaming
works in both clients and most HTTP/2 semantics are correct, but this pass found and
**proved** one live correctness bug in the TS client plus a cluster of shared
semantic/robustness gaps the conformance battery structurally cannot reach (its origin only
ever drives the happy path — no early responses, 1xx, trailers, padding, or constrained
windows).

A second round (2026-07-08) then landed the keepalive-on-by-default change plus the
unambiguous, low-risk items from §2; receive-side backpressure is deferred to the next pass.

| Area | TS client | Rust client |
|---|---|---|
| Bidi streaming (general) | ✅ | ✅ |
| Bidi + **early complete response under flow control** | ❌→✅ **fixed (round 1)** | ✅ (P1.1 fix was complete) |
| PING / GOAWAY / RST / trailers / flow control | ✅ well-tested | ✅ well-tested |
| 1xx interim responses | ✅ fixed (round 2) | ✅ fixed (round 2) |
| SETTINGS validation / header-block cap | ✅ fixed (round 2) | ✅ fixed (round 2) |
| Max concurrent streams (enforce + pool) | ✅ fixed (round 3) | ✅ fixed (round 3) |
| Server upstream close codes | ✅ nginx-style 1014 (round 3) | ✅ nginx-style 1014 (round 3) |
| Receive-side backpressure | ✅ consumption-driven (round 4) | ✅ consumption-driven (round 4) |
| Server dead-peer liveness (keepalive) | ✅ on by default (round 2) | ✅ on by default (round 2) |

---

## 1. Fixed this pass — TS upload truncation on an early complete response

**The P1.1 fix (`status.md`) was applied to the Rust client but never mirrored into the
TS "reference" client.** The TS client deleted a stream from its map on the peer's
`END_STREAM` while its own send side was still open, so a stream-level `WINDOW_UPDATE`
arriving *after* an early complete response was dropped and `pumpBody` parked forever —
the caller got a clean `200` while the request body was **silently truncated** and the
pump task leaked. A later `RST_STREAM` for that retired stream was likewise unroutable, so
the pump was never told to stop.

**Proven, then fixed:** an identical flow-limited scenario (100 KB body; server completes
the response after 65535 bytes, then grants more window) **hung the TS client (4 s timeout)**
while the **Rust client passed in 0.00 s**.

**Fix** (`connection.ts`, `stream.ts`): track `localClosed`/`remoteClosed` per stream and
retire only once **both** sides have ended (`retireIfFullyClosed`) — a direct port of the
Rust `retire_if_fully_closed`. A stream half-closed(remote) now stays in the map so its send
window and inbound `WINDOW_UPDATE`s keep working until the upload also finishes; the pump's
terminal `END_STREAM` is guarded on liveness; pushed streams start `localClosed = true`.

**Regression tests added in BOTH clients** (the flow-limited variant — previously untested
in *both*, and untested *at all* on the TS side): `receive-path.test.ts` →
"finishes a flow-limited upload after an early complete response"; `connection.rs` →
`finishes_a_flow_limited_upload_after_an_early_complete_response`. **Suites green:**
TS client 42, Rust client connection suite 20; TS typecheck clean.

---

## 2. Round-2 hardening — fixed this round, and what's still open

### Fixed (landed 2026-07-08 · mirrored tests · all suites green — see the work log)

**Server**
- **Keepalive on by default, everywhere (opt-out) [was #7].** `BridgeConfig::default()` now
  carries a keepalive (30s ping / 15s timeout / 1001), so `serve_h2`, the example,
  `WsByteStream::new`, and `bridge` all get dead-peer liveness; opt out with `keepalive: None`.
  The proxy defaults it on with a `--no-keepalive` flag (and `--keepalive-secs 0` still
  disables). Closes the half-open/dead-peer leak.
- **`accept()` loop is non-fatal [was #8].** A transient `EMFILE`/`ECONNABORTED` in the proxy
  and example is logged + retried (50 ms backoff) instead of killing the listener.
- **`accept_with` never echoes an un-offered subprotocol [was #10].** A handler selection
  outside the offered list falls back to the default policy (RFC 6455 §4.2.2); guarded by a
  new test.

**Clients (both TS & Rust)**
- **1xx interim responses [was #1].** A `100 Continue` / `103 Early Hints` HEADERS block
  (no `END_STREAM`, before the final response) is now dropped and the client keeps waiting for
  the real head — instead of surfacing the 1xx as the response and the real response as
  trailers. (Chose drop-and-wait; no API change.)
- **Peer SETTINGS validation [was #4].** `INITIAL_WINDOW_SIZE > 2^31-1` → FLOW_CONTROL_ERROR;
  `MAX_FRAME_SIZE` outside `[2^14, 2^24-1]` → PROTOCOL_ERROR; both tear the connection down
  with a GOAWAY.
- **Header-block size cap [was #5].** A 1 MiB cap across HEADERS + CONTINUATION → ENHANCE_YOUR_CALM
  + GOAWAY, bounding a CONTINUATION flood (CVE-2024-27316 class), tracked with a running counter
  (no O(n²) re-summing).

### Fixed — round 3 (2026-07-08)

- **Server upstream close-code fidelity [was #9] — "whatever nginx does".** The bridge now
  distinguishes a clean upstream EOF (WS `1000`) from an upstream *failure*: a read/write error
  uses a configurable `error_close` (default `1011`; the proxy sets **1014 Bad Gateway**), and
  an upstream *connect* failure sends a real `1014` close instead of a bare `1006` — the WS
  analogue of nginx's `502`. h2 `GOAWAY` codes were always proxied through transparently as
  bytes and are unaffected. Tested (`bridge.rs`: read-error and write-failure paths).
- **Client `SETTINGS_MAX_CONCURRENT_STREAMS` + pool [was #6] — "whatever Go does by default".**
  Both clients now store and enforce the peer's limit: `request()` parks until a stream slot
  frees (§5.1.2) instead of over-opening and getting `REFUSED_STREAM`. On top, a Go-style
  **connection pool** (`connectPool` / `H2Pool`) opens a new WebSocket only when every
  connection is saturated — matching `golang.org/x/net/http2`'s default
  (`StrictMaxConcurrentStreams = false`); real multiplexing first, extra connections only under
  load. Mirrored + tested in both clients (per-connection enforcement + pool routing).

### Fixed — round 4 (2026-07-09)

- **Receive-side backpressure [was #2, #3] — "whatever node:http2 does".** Both clients replaced
  eager, on-receipt `WINDOW_UPDATE` + unbounded buffering with **consumption-driven** flow
  control: DATA is buffered per stream and the stream + connection receive windows are
  replenished only as the application *reads* the body — so an unread body stalls the sender
  instead of growing memory. The connection window is grown at startup past the 65535 spec
  default (config `connectionWindowSize` / `connection_window_size`, default **64 MiB**, kept
  larger than the **1 MiB** stream window so a single unread stream can't stall the whole
  connection). TS: pull-based `ReadableStream` (highWaterMark 0) with cancel-returns-window;
  Rust: `RecvState` + `ResponseBody` (`Stream`) with drop-returns-window. Mirrored tests
  (no replenish before a read; both windows returned on consume; startup growth) + conformance
  16/16 both clients.

### Fixed — round 5 (2026-07-09)

- **Receive-path robustness coverage + a TS hang fix.** Added mirrored tests in both clients for
  the implemented-but-untested edges (§3): stream-level `WINDOW_UPDATE(0)` → RST_STREAM,
  frame > `MAX_FRAME_SIZE` → GOAWAY, padded HEADERS on receive, inbound PUSH_PROMISE refused
  (REFUSED_STREAM), oversized-header split into HEADERS + CONTINUATION on send. Writing the first
  test surfaced a **real bug**: TS `resetStream` removed the stream without failing its pending
  request, so a stream-level `WINDOW_UPDATE(0)` would **hang** `request()` forever. Fixed —
  `resetStream` now fails the stream (Rust `reset_stream` likewise fails it with a proper error
  rather than a generic dropped-sender cancel).

### Still open (deferred / needs a decision)

- **`Origin` allowlist + `Sec-WebSocket-Version` negotiation [#11] — LOW/security.** Needs a
  config/policy decision.
- **Extend the conformance origin** to drive early-complete / 1xx / trailers / constrained-window
  so the shared battery catches these instead of relying on hand-written mirror tests.

---

## 3. Coverage gaps (from the mapped matrix)

The conformance origin only drives the happy path, so these receive-path edges were covered by
mirrored unit tests instead. **Closed in round 5** (both clients): stream-level
`WINDOW_UPDATE(0)` → RST_STREAM (which also surfaced + fixed a real TS hang — see round 5),
frame > `MAX_FRAME_SIZE` → GOAWAY, padded HEADERS on receive, inbound PUSH_PROMISE refused,
and oversized-header split on *send*. (1xx and receive backpressure were closed in rounds 2/4.)
**Still open:** extending the conformance origin itself to drive early-complete / 1xx /
trailers / constrained-window, so the shared battery — not just hand-written mirror tests —
catches this class.

---

## 4. Prioritized task list

- **P0 — DONE (round 1).** Port the two-sided stream-retire fix to the TS client + flow-limited
  regression test in both clients.
- **P1 (breaks real origins):** ~~(a) 1xx handling [#1]~~ **done (round 2)**; (b) receive
  backpressure + connection-window growth [#2, #3] — **deferred to next pass**; ~~(c) server
  keepalive default + non-fatal `accept()` [#7, #8]~~ **done (round 2)**.
- **P2 (robustness/conformance):** ~~SETTINGS validation [#4]~~ **done**; ~~header-block cap
  [#5]~~ **done**; server close-code consistency [#9] — **open**; the untested receive-path
  cases in §3 — **open**.
- **P3 (lower risk):** `MAX_CONCURRENT_STREAMS` queuing [#6] — open; ~~`accept_with` membership
  guard [#10]~~ **done**; `Origin` allowlist, `Sec-WebSocket-Version` [#11] — open; extend the
  conformance origin to drive early-complete/1xx/trailers/constrained-window so the battery
  catches this whole class — open.

## Work log

- [x] **P0 (TS bidi upload-truncation)** — _done 2026-07-07._ Ported `local_closed`/
  `remote_closed` + `retireIfFullyClosed` to the TS client (`connection.ts`, `stream.ts`);
  pushed streams start half-closed(local); pump terminal `END_STREAM` guarded on liveness.
  Proven with a probe (TS hung 4 s pre-fix; Rust passed) then fixed; flow-limited regression
  test added in both clients. TS client 42 tests, Rust client connection 20, typecheck clean.
- [x] **Round 2 — keepalive default + trivial hardening** — _done 2026-07-08._
  **Server:** keepalive is now on by default everywhere (`BridgeConfig::default()` carries
  `KeepAlive::default()` = 30s/15s/1001; opt out with `keepalive: None`; proxy gains
  `--no-keepalive`), closing the dead-peer leak [#7]; `accept()` loops in the proxy + example
  are non-fatal [#8]; `accept_with` refuses to echo an un-offered subprotocol [#10] (+ test).
  **Clients (TS + Rust):** 1xx interim responses dropped-and-waited [#1]; peer SETTINGS
  validated (window/frame-size ranges → GOAWAY) [#4]; header block capped at 1 MiB across
  HEADERS + CONTINUATION [#5]. Mirrored tests added in both clients. **Suites green:** TS
  client 45, Rust client connection 23, Rust server 49 (handshake +1); clippy clean;
  conformance 16/16 for both TS and Rust clients through the keepalive-enabled proxy.
- [x] **Round 3 — upstream close codes + max-concurrent-streams/pool** — _done 2026-07-08._
  **Server [#9]:** the bridge now sends nginx-style close codes — a new configurable
  `error_close` (default `1011`) on an upstream read/write error, `1014` Bad Gateway in the
  proxy (incl. connect failure, previously a bare `1006`), `1000` on a clean EOF; `bridge.rs`
  gained read-error + write-failure tests (bridge 9). **Clients [#6]:** both now enforce the
  peer's `SETTINGS_MAX_CONCURRENT_STREAMS` (`request()` parks on a slot waiter until one frees)
  and gained a Go-default connection pool (`connectPool`/`H2Pool` in TS; `H2Pool`/`connect_pool`
  in Rust, wasm-wired). Mirrored tests: enforcement (`honors_the_peers_max_concurrent_streams`)
  + pool routing (5 each). **Suites green:** TS client 51, Rust client 55 (connection 24 + pool
  5 + frames 11 + hpack 15), Rust server 59; clippy clean (native + wasm32).
- [x] **Round 4 — receive-side backpressure** — _done 2026-07-09._ Replaced eager on-receipt
  `WINDOW_UPDATE` + unbounded body buffering with **consumption-driven** flow control in both
  clients (node:http2-style): DATA is buffered and the stream + connection windows are
  replenished only as the app reads the body; unread bodies apply backpressure. Connection
  window grown at startup (config, default 64 MiB) above the 1 MiB stream window to avoid
  connection-level HOL. TS pull-based `ReadableStream` (HWM 0) + cancel-returns-window; Rust
  `RecvState`/`ResponseBody` + drop-returns-window (public `into_body()` now yields
  `ResponseBody`). Mirrored tests (`replenishes_the_receive_window_only_on_consumption`,
  TS "replenishes … only as the body is consumed" + startup-growth). **Suites green:** TS
  client 53, Rust client 56 (connection 25 + pool 5 + frames 11 + hpack 15), Rust server 50;
  clippy clean (native + wasm32); conformance 16/16 both clients.
- [x] **Round 5 — receive-path robustness coverage + resetStream hang fix** — _done 2026-07-09._
  Mirrored tests in both clients for stream `WINDOW_UPDATE(0)` → RST, frame > MAX_FRAME_SIZE →
  GOAWAY, padded HEADERS on receive, PUSH_PROMISE refused, oversized-header split on send. The
  first test found a real TS hang (`resetStream` didn't fail the pending request on a stream
  reset); fixed in both clients. **Suites green:** TS client 58, Rust client 61 (connection 30 +
  pool 5 + frames 11 + hpack 15); Rust server 20; clippy clean (native + wasm32); conformance
  16/16 both clients.
- [ ] Still open (see §2 "Still open"): `Origin`/`Sec-WebSocket-Version` [#11]; extend the
  conformance origin to drive early-complete / 1xx / trailers / constrained-window.
