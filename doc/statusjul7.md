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
| Receive-side backpressure | ⚠️ open (deferred) | ⚠️ open (deferred) |
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

### Still open (deferred / needs a decision)

- **Receive-side backpressure + connection-window growth [#2, #3] — MEDIUM (deferred by
  request).** Both clients still replenish `WINDOW_UPDATE` eagerly and buffer unbounded, and
  the connection receive window stays pinned at 64 KiB — a real body-delivery design change
  (gate replenishment on app consumption; grow the connection window), planned for the next pass.
- **`Origin` allowlist + `Sec-WebSocket-Version` negotiation [#11] — LOW/security.** Needs a
  config/policy decision.

---

## 3. Coverage gaps (from the mapped matrix)

The conformance origin never sends early responses, 1xx, trailers, padded frames, deliberate
GOAWAY/RST/SETTINGS changes, or a constrained window — so the battery is happy-path only.
Untested in **both** clients (beyond the early-complete case now fixed): 1xx; receive
backpressure; stream-level `WINDOW_UPDATE(0)`; frame > `MAX_FRAME_SIZE`; padded HEADERS on
receive; inbound PUSH_PROMISE at the connection layer; oversized-header split on *send*.

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
- [ ] Still open (see §2 "Still open"): receive-side backpressure [#2, #3] (deferred by
  request), `Origin`/`Sec-WebSocket-Version` [#11].
