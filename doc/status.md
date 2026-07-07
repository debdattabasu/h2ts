# h2ts client/server audit — status

_Audit date: 2026-07-07. Scope: TypeScript client (`typescript/client`), Rust client
(`rust/crates/h2ts-client`), Rust server (`rust/crates/h2ts-server`)._

**Method:** read both clients' full source + tests, the Rust server source + all tests,
and the conformance suite; ran `cargo test -p h2ts-client` (34 pass) and
`npm test -w h2ts` (29 pass); and wrote two temporary probe tests to settle the bidi
question empirically (both removed after use).

## TL;DR

| # | Question | TS client | Rust client |
|---|----------|-----------|-------------|
| 1 | Full bidi streaming | ✅ Works | ⚠️ Works **except** one confirmed bug (early complete response truncates the upload) |
| 2 | HTTP/2 PING | ✅ Works | ✅ Works (minor on-close behavior differs) |
| 3 | Test coverage | Frame/HPACK solid; **no** connection-level unit tests (leans on conformance) | **More** engine unit tests than TS, but **zero** e2e/conformance and **zero** WS-transport tests |

---

## 1. Full bidirectional streaming

**Both clients implement it**, and the architecture is sound in both: a streaming
request body is pumped *concurrently* with receiving the response, and the response
body is delivered incrementally.

- **TS** — `connection.ts:434` `pumpBody` runs un-awaited while `request()` awaits
  `stream.head`; request body and response body are both `ReadableStream`
  (`connection.ts:371`, `stream.ts:68`). Send-side flow control via
  `SendWindow.waitPositive`; receive side replenishes `WINDOW_UPDATE` on receipt.
- **Rust** — `connection.rs:775` `pump_body` is registered as a driver task
  (`connection.rs:701`) so it runs concurrently; response body is an
  `mpsc::UnboundedReceiver<Vec<u8>>`. It even has an explicit bidi unit test
  (`returns_the_response_before_the_upload_finishes`) that the TS client lacks.

### ⚠️ Confirmed bug (Rust only): an early *complete* response truncates the request body

If the server finishes its response (sends `END_STREAM`) **before** the client has
finished uploading, the Rust client silently drops the rest of the request body and
never sends its own `END_STREAM`.

Root cause — on server `END_STREAM` the Rust client removes the stream from its map
even though the client's *send* side is still open (this is legal
`half-closed (remote)`, not closed):
- DATA path: `connection.rs:451-453`
- HEADERS path: `connection.rs:538-540`

The upload pump then aborts because the stream is gone from the map —
`connection.rs:806` (`!st.streams.contains_key(&id)` → returns `false` → `pump_body`
returns without the terminal `END_STREAM`).

TS avoids this: `retireStream` deletes from the map but `pumpBody` holds its own
`stream` reference and only stops on `closedFlag`/`sendWindow.isClosed`
(`connection.ts:438`, `connection.ts:490`).

**Evidence (throwaway probes, identical scenario):**
- Rust probe → **SIGABRT via 8s watchdog** — the upload never completes, client hangs.
- TS probe → **passes** — client sends `part2` + empty `END_STREAM` after the
  server's early complete response.

Real-world trigger: any origin that responds before draining the request body (early
`401`/`413`/redirect, streaming RPC, etc.). The conformance `/echo` reads fully first,
so it *doesn't* catch it — and the Rust client isn't in conformance anyway.

**Fix direction:** keep the stream in the map on remote `END_STREAM`; only remove once
*both* sides have ended (track a `half_closed_remote` flag on `StreamState` and retire
when the local pump has also sent `END_STREAM`).

---

## 2. HTTP/2 PING

**Both work.** `ping()` sends an 8-byte opaque PING and resolves with RTT on ACK; an
inbound non-ACK PING is auto-answered.
- TS — `connection.ts:383` (send/RTT), `connection.ts:260-271` (auto-ACK).
- Rust — `connection.rs:724` (send/RTT), `connection.rs:480-492` (auto-ACK).

**~~Minor divergence~~ (resolved, P1.3):** on connection teardown TS used to resolve
pending pings with `-1` while Rust returned an error. Both now **fail an in-flight
ping with the close error**, guarded by tests in each client.

**~~Test-coverage asymmetry~~ (resolved, P1.3):** the TS client now has ping unit
tests (`typescript/client/test/ping.test.ts`), matching Rust's.

---

## 3. Test coverage differences & gaps

### Client unit tests (what exists)

| Area | TS | Rust |
|---|---|---|
| Frame codec | 11 tests | 11 tests (near-exact port) — **parity** |
| HPACK (RFC 7541 vectors) | 15 tests | 15 tests (near-exact port) — **parity** |
| Connection: preface / first-request / req-resp | 3 | 3 |
| Connection: **ping RTT** | ❌ | ✅ |
| Connection: **streaming upload** | ❌ | ✅ |
| Connection: **upload flow control** | ❌ | ✅ |
| Connection: **bidi (resp before upload)** | ❌ | ✅ |
| Connection: **incremental download** | ❌ | ✅ |

Counter-intuitive result: the "reference" **TS client is the *less* unit-tested** of
the two at the connection level. It gets away with it because conformance exercises
those paths — but **only for TS**.

### Structural gaps (highest-impact first)

1. ~~**The Rust client has zero end-to-end coverage.**~~ **Resolved (P1.2).** The
   Rust client now runs the shared conformance battery against a real `h2ts-proxy`
   → h2c origin via the `h2ts-conformance` crate. (Before: conformance drove only
   `typescript/client/dist` and the Rust engine's only validation was hand-rolled
   mock-server tests — exactly the blind spot that hid the §1 bug.)
2. ~~**Rust WebSocket transport is completely untested.**~~ **Resolved (P3 item 11).**
   `web.rs` (`connect_websocket` / `websocket_transport` / `WsSink`) — the real
   browser transport that ships to a wasm frontend — now runs the full shared battery
   **compiled to wasm32** under Node (which supplies the global `WebSocket`), against a
   live `h2ts-proxy` → h2c origin. New crate `h2ts-wasm-conformance` (cdylib) +
   `conformance/wasm-run.mjs`; `run.sh CLIENT=wasm|all`. **16/16 pass**, and the real
   `WsSink::poll_close` produces a clean `1000` WS close (the native fastwebsockets
   driver logs `1006`). This exercises subprotocol offering, `arraybuffer` handling,
   the `onmessage`/`onclose`→EOF plumbing, and the sink — the code no other test touched.
3. **Neither client tests receive-path robustness:** GOAWAY handling, RST_STREAM
   mid-stream, `WINDOW_UPDATE(0)` / flow-control errors, CONTINUATION reassembly on
   *receive*, oversized-header split on *send*, `SETTINGS INITIAL_WINDOW_SIZE`
   mid-stream re-adjust, malformed/oversized frames.
4. **Feature-parity gaps in the Rust client** (deferred TODOs — each is also a coverage
   gap vs TS): ~~trailers not surfaced~~ _(done — P2 Round 2)_, server push refused with
   no callback (`connection.rs`), no abort/cancel (TS has `signal`), and **no `protocol`
   accessor** for the negotiated subprotocol — the spec says clients *SHOULD* expose it
   (`spec/protocol.md:35`); TS does, Rust doesn't.

### Rust server (48 tests — strong; the P3 gaps below are now closed)

Well covered: handshake negotiation/rejection, `allow_implicit_codec`, bridge
full-duplex, sub-frame streaming, large payloads, 16 concurrent streams, keepalive
(both outcomes), proxy binary, `serve_h2`. The lifecycle-edge gaps below were closed
in **P3 (A+B+C)**:
- ~~**`serve_h2_with` has zero coverage**~~ **(done)** — `tests/serve_h2_with.rs`
  drives real h2 traffic (100 KiB `/big` + 32 KiB `/echo`) while a 5ms keepalive pings
  in the background, asserting byte-exactness (a ping can't corrupt the h2 stream) and
  that `on_close` fires on teardown.
- ~~**Abnormal closure (1006)**~~ **(done)** — `bridge_reports_1006_on_transport_drop_without_close`:
  a WS transport EOF with no Close frame surfaces to `on_close` as 1006, empty reason.
- ~~**Bridge error/teardown**~~ **(done)** — `bridge_reports_1006_with_reason_on_peer_write_failure`
  uses a `FailingPeer` (writes always `Err`, reads park) to deterministically hit the
  write-failure path → `on_close(1006, <io error text>)`.
- ~~**Handshake 426 (`NotUpgradeRequest`) arm**~~ **(done)** —
  `accept_rejects_a_non_upgrade_request_with_426` + `rejection_response_maps_constructible_errors_to_status`.
  The **500 (`Upgrade`) arm** wraps a `hyper::Error` with no public constructor, so it's
  only reachable through a live post-101 upgrade failure — left uncovered (documented in
  the test).
- ~~custom `KeepAlive.close`~~ **(done)** — `keepalive_uses_a_custom_close_frame`.
  ~~`WsControl` after bridge end (`BrokenPipe`)~~ **(done)** —
  `wscontrol_send_fails_after_the_bridge_ends`.
- ~~item 11: the client's wasm `web.rs` WS transport~~ **(done — see structural gap #2
  above).** **Item 12 — backpressure: done** (`tests/backpressure.rs`, both
  directions). **Still open:** GOAWAY / graceful h2 shutdown under active traffic.

---

## 4. Suggested tests to harden the surface

**Priority 1 — catch the real bugs**
1. **Rust regression for §1**: server sends full response before upload completes →
   assert client still sends remaining DATA + `END_STREAM`. Add the mirror to TS too.
2. **Put the Rust client in conformance.** Add a host driver (drive it against
   `h2ts-proxy`) or a native example client so `conformance/run.mjs`'s battery runs
   against Rust too. Single highest-leverage addition.
3. **Ping-on-close contract** in both clients — pin the divergent `-1` vs `Err`
   behavior deliberately (and align them, or document the difference).

**Priority 2 — receive-path robustness (both clients, mirror the same cases)**
4. GOAWAY: streams above `lastStreamId` fail; non-zero error code tears down the
   connection.
5. RST_STREAM mid-download → response body errors/ends; mid-upload → pump stops cleanly.
6. Protocol errors: `WINDOW_UPDATE(0)`, frame > `maxFrameSize`, CONTINUATION on the
   wrong stream → GOAWAY.
7. CONTINUATION *reassembly on receive* + oversized-header split *on send*
   (`connection.ts:424` / `connection.rs:588` — untested).
8. Trailers (HEADERS after DATA) — TS surfaces them but doesn't test it; Rust should
   implement + test.
9. `SETTINGS INITIAL_WINDOW_SIZE` change mid-stream re-adjusts live send windows
   (`connection.rs:557`).

**Priority 3 — server + transport**
10. Rust server: `serve_h2_with` with keepalive+hooks; abnormal-close→1006; bridge
    write-failure→`Err`; 426/500 handshake arms; GOAWAY under active traffic.
11. Rust WS transport: `wasm-bindgen-test` for `websocket_transport` framing +
    subprotocol offering (or a non-wasm unit test of `WsSink` over a fake `WebSocket`).
12. ~~True backpressure assertion in the server (blocked writer, not just byte-exactness).~~
    **Done** — `tests/backpressure.rs` proves both directions propagate backpressure (a
    producer stays blocked while its consumer is paused, then drains byte-exact).

---

## P2 — receive-path reconciliation (detail)

The **receive path** is `dispatch()` reacting to inbound frames from the peer
([connection.ts:182](../typescript/client/src/connection.ts) /
[connection.rs:363](../rust/crates/h2ts-client/src/connection.rs)) — the least-tested
area in both clients, and where the P1.1 bug lived. A real correctness bug sat
undetected in this path, so the whole neighborhood needs mirrored tests that pin
behavior and reconcile the two clients. The items below are untested gaps (plausible
code, unproven), except trailers which is a real divergence.

Goal: symmetric tests in both clients — *"peer sends `<frame>` → assert the client
does the right thing"* — using the same mock-server harness as the P1.1 test.

1. **GOAWAY** (ts:276 / rs:509). Streams with `id > lastStreamId` fail; a non-zero
   error code tears the connection down. *Assert:* graceful GOAWAY (code 0) fails only
   higher-id streams and lets lower ones finish; non-zero tears everything down.
2. ~~**RST_STREAM mid-stream**~~ **(done).** mid-upload → request errors, pump stops, no
   hang; mid-download → body **errors** (Rust reshaped to carry `Result` so it no longer
   silently truncates). Reconciled + tested in both clients.
3. **Protocol errors → GOAWAY + teardown** — `WINDOW_UPDATE(0)` (rs:485), frame >
   `maxFrameSize`, CONTINUATION on the wrong stream (ts:186). *Assert:* client emits
   GOAWAY and destroys cleanly via `connectionError` (ts:501 / rs:649) — no panic/hang.
4. **SETTINGS INITIAL_WINDOW_SIZE mid-stream** (ts:308 / rs:576). The peer changing the
   initial window retroactively adjusts every existing stream's send window, possibly
   **negative** (§6.9.2). *Assert:* a stream mid-upload whose window shrank does not
   over-send. *(highest value — data-integrity risk)*
5. **CONTINUATION reassembly on receive** (ts:206 / rs:401). *Assert:* a header block
   split across HEADERS + CONTINUATION reassembles; an intervening non-CONTINUATION
   frame is rejected.
6. ~~**Trailers**~~ **(done).** Implemented in the Rust client (`Response::trailers()`
   via a shared cell); both clients now surface a post-body HEADERS block and are
   tested (`surfaces_response_trailers`).

## Work log

- [x] **P1.1** Fix Rust bidi upload-truncation bug + regression test — _done 2026-07-07._
  `connection.rs` now tracks per-stream `local_closed`/`remote_closed` and only
  retires a stream via `retire_if_fully_closed` once both sides have ended, so an
  early complete response no longer truncates the upload. Regression test
  `finishes_upload_after_an_early_complete_response` (tests/connection.rs). Full
  Rust client suite green (35 tests), no hang.
- [x] **P1.2** Wire the Rust client into the conformance suite — _done 2026-07-07._
  New workspace crate `rust/crates/h2ts-conformance` (a native binary, `publish =
  false`) adapts a `fastwebsockets` client WebSocket to `h2ts_client::Transport`
  and runs the same 10-check battery as `run.mjs`. `conformance/run.sh` now runs
  **both** clients against one `h2ts-proxy` (`CLIENT=ts|rust|both`, default `both`)
  and gained an `ORIGIN_PORT` override (default 8000) so a busy :8000 doesn't block
  it. Verified end-to-end: **Rust client 16/16 ✅** and TS 16/16 ✅ against a live
  `h2ts-proxy` → Node h2c origin — the first real-gateway coverage the Rust engine
  has ever had. (Kept tokio/fastwebsockets out of the `h2ts-client` crate by
  isolating them in the new crate.)
  Minor: the Rust driver exits right after `conn.close()`, so the gateway logs a
  1006 (abnormal) WS close vs the TS transport's clean 1000 — cosmetic, harness-only.
- [x] **P1.3** Pin ping-on-close behavior in both clients — _done 2026-07-07._
  Aligned both clients: a PING in flight when the connection tears down now
  **fails with the close error** (never a bogus RTT), matching the already-closed
  path. TS `destroy` rejects the pending ping promises with `closeError` (was
  `resolve(-1)`); the `pings` map now stores `reject`. Rust's `PingWaiter` channel
  carries `Result<f64, H2Error>` so `destroy` fails in-flight pings with the real
  close error (was a silent drop → generic error). Pinned by tests: new
  `typescript/client/test/ping.test.ts` (happy path + in-flight-close reject +
  already-closed reject — the TS client had **no** ping unit tests before) and Rust
  `ping_errors_when_the_connection_closes_in_flight` (tests/connection.rs). Suites
  green: TS 32, Rust client 36. **Behavior change:** TS `ping()` now rejects on
  close instead of resolving `-1` (pre-1.0, and `-1` was an undocumented footgun).
- [x] **P2** Receive-path reconciliation — _done 2026-07-07._ All 6 items reconciled +
  tested in both clients; one shared bug (dropped GOAWAY on connection error) fixed in
  both. See the P2 detail section.
  - _Round 1 (done):_ **RST_STREAM mid-upload** and **GOAWAY(error) teardown** pinned
    in both clients (Rust `rst_stream_mid_upload_fails_the_request_without_hanging`,
    `goaway_with_error_tears_down_and_fails_in_flight_requests`; TS
    `test/receive-path.test.ts`). Both correct & consistent — the pump-hang risk is
    **not** a bug.
  - _Round 2 (done): #2 RST mid-download + #6 trailers reconciled (Rust API change)._
    Rust `Response` body now carries `Result<Vec<u8>, H2Error>`; `bytes()`/`text()`
    return `Result` and take `&mut self`; `StreamState::fail` sends `Err` on the body
    channel, so a reset/failed download **errors** instead of silently truncating —
    matching TS. Trailers implemented: a post-body HEADERS block fills a shared cell
    exposed via `Response::trailers()` (was "not surfaced yet"). Tests added in both
    clients (`rst_stream_mid_download_errors_the_response_body`,
    `surfaces_response_trailers`; TS mirrors). Call sites updated (client tests +
    `h2ts-conformance`). Rust client tests 40, TS 36 (+ typecheck).
  - _Round 3 (done): #4 window-shrink, #1 GOAWAY graceful boundary, #3 protocol-error
    teardown, #5 CONTINUATION reassembly._ Mirrored tests in both clients (Rust
    `honors_a_retroactively_shrunk_send_window`,
    `graceful_goaway_fails_higher_streams_but_lets_lower_finish`,
    `connection_window_update_zero_tears_down_with_goaway`,
    `reassembles_a_header_block_split_across_continuation`,
    `an_unterminated_header_block_interrupted_by_data_is_a_protocol_error`; TS mirrors).
    #4 proves both clients honor a retroactively-**negative** send window (release
    exactly the granted bytes, no over-send).
  - _Bug found + fixed in BOTH clients: dropped GOAWAY on a connection error._ The #3
    and #5 protocol-error tests revealed that neither client actually flushed the
    GOAWAY it queues on a connection error before tearing the transport down — the
    peer just saw an abrupt close (RFC 7540 §5.4.1/§6.8 say SHOULD send GOAWAY). Same
    class of bug in both: **Rust** `drive` used `select`, so the read loop completing
    dropped the write loop before it flushed; **TS** `destroy` called
    `writer.close()` without waiting for the queued (un-awaited) GOAWAY write. Fixed
    both: Rust `destroy` drops `out_tx` and the driver's write loop is now its
    lifetime (flushes the queue, then ends); TS `destroy` chains `close()` after
    `writeQueue`. Now both send the GOAWAY, asserted in both.
  - _Verified:_ Rust client 45 tests, TS 41, conformance 16/16 both clients.
- [x] **P3 (A+B+C)** Server lifecycle-edge coverage — _done 2026-07-07._ Seven new
  server tests, all pure-Rust (no new deps), closing the compose/failure-path gaps —
  the same "edge nobody drives" class that hid the P1.1 client bug. **A (failure
  lifecycle):** `serve_h2_with` compose (h2 + 5ms keepalive + `on_close`, asserting a
  ping can't corrupt h2 DATA — `tests/serve_h2_with.rs`); abnormal-close 1006 on a
  transport drop with no Close frame; write-failure teardown → `on_close(1006, <error>)`
  via a `FailingPeer` that forces the write-error branch deterministically (a plain
  duplex races read-EOF vs write-error). **B (handshake):** the 426 `NotUpgradeRequest`
  arm + the `rejection_response` status map (the 500 `Upgrade` arm isn't unit-
  constructible — `hyper::Error` has no public ctor — so it's noted, not tested).
  **C (control edges):** custom `KeepAlive.close` code/reason; `WsControl` send →
  `BrokenPipe` after the bridge ends. Suite: **h2ts-server 46 tests** (was 39), clippy
  clean, rustfmt clean.
- [x] **P3 (item 11)** Wasm WebSocket transport e2e — _done 2026-07-07._ The Rust
  client's real browser transport (`web.rs`: `connect_websocket` / `websocket_transport`
  / `WsSink`) had **zero** coverage — the native `h2ts-conformance` driver exercises the
  engine over a *fastwebsockets* transport, never the wasm `web-sys` one that actually
  ships. New crate **`h2ts-wasm-conformance`** (cdylib, `#![cfg(target_arch = "wasm32")]`
  so the native workspace build sees an empty lib) runs the same 10-check battery via
  the real `connect_websocket`; `conformance/wasm-run.mjs` loads the
  `wasm-bindgen --target nodejs` glue and drives it under Node (global `WebSocket`).
  `run.sh` gained `CLIENT=wasm` and `CLIENT=all` (build wasm32 + wasm-bindgen, then run).
  **16/16 pass** against a live proxy → h2c origin, with a clean `1000` WS close from
  the real sink. Needs the `wasm32-unknown-unknown` target + `wasm-bindgen-cli`
  (pinned 0.2.126).
- [x] **P3 (item 12)** Bridge backpressure — _done 2026-07-07._ `tests/backpressure.rs`
  proves the byte pump **propagates** backpressure end to end rather than absorbing an
  unbounded amount when the far consumer stalls (the other bridge tests only asserted
  byte-exactness under normal flow). Both directions: a producer's `write` stays
  unfinished (`JoinHandle::is_finished()` is `false`) while its consumer is paused —
  so a regression to unbounded buffering would fail — then the full 1 MiB drains
  byte-exact once the consumer resumes. **h2ts-server 48 tests.** The only original-
  audit item now left open is **GOAWAY / graceful h2 shutdown under active traffic**.
