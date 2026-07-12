# CLAUDE.md

Guidance for working in this repo. Read this first; then read
[`spec/protocol.md`](spec/protocol.md) — it is the contract everything else obeys.

## What h2ts is

Real HTTP/2 (h2c, prior-knowledge) tunneled inside a WebSocket. A frontend gets a
genuine multiplexed HTTP/2 client (HPACK, flow control, server push) by carrying
HTTP/2 frames as WebSocket **binary** messages; a gateway terminates the WebSocket
and either forwards the raw bytes to an upstream h2c server (**proxy** shape) or
serves an HTTP/2 service in-process (**serve** shape).

```
frontend (client)  --ws-->  gateway (server)  --h2c/TCP-->  HTTP/2 origin
real HTTP/2         frames   WS <-> raw bytes   TCP bytes    (or served in-process)
```

## The one rule that governs everything

**The per-language implementations share NO code.** They stay interoperable by
(a) conforming to `spec/protocol.md` and (b) passing the `conformance/` suite.
Consequences for any change:

- Behavior is defined in the spec **first** (or in the same change). Never fork
  behavior silently between stacks — extend the spec, then make each stack match.
- A change to any client or gateway must still pass `conformance/` — the executable
  form of the spec. Cross-stack (any client × any gateway) is the real test; a
  stack's own unit tests are necessary but not sufficient.
- When you add a capability to one server, check whether its sibling should match,
  and record the decision (see `doc/`). Parity is deliberate, not assumed.

## Auditing for drift

Conformance proves wire-level interop, but it can't catch *subtle* divergence — a
receive-path branch one implementation handles and another silently doesn't, an edge the
spec implies but no test exercises, a behavior that has quietly drifted from the spec's
intent. Two practices catch that:

- **Fresh-context audits.** Periodically a fresh agent — no prior conversation, no
  assumptions carried in — is given the broad **system invariants** (the spec's
  guarantees, the share-no-code/stay-interoperable rule, the intended behavior of each
  layer) and asked to evaluate the **current** state of the spec, the tests, and the code
  against them. It reads the full source and tests across all stacks and reports two
  things: **logic drift** — where implementations diverge from each other or from the spec
  — and **test-coverage gaps** — paths that are plausible but unproven. Each audit is a
  dated document under `doc/` with a work log that turns every finding into a fix or a
  recorded decision. `doc/status.md` (TS/Rust client + Rust server) and
  `doc/StatusJul10.md` (Go server flow-control + coverage) are the current examples; a
  real correctness bug — the Rust client's early-complete-response upload truncation — was
  found exactly this way, in the least-tested neighborhood the audit flagged.
- **Author coverage review.** The author also audits test coverage by hand on a regular
  basis, specifically hunting the edge cases that conformance and automated tooling tend
  to miss — receive-path robustness, lifecycle/teardown, and flow-control corners that
  only surface under an adversarial reading.

Assume any change will be read this way. Leave the spec, the tests, and the code mutually
consistent, and prefer pinning an edge with a test over trusting that it's "obviously
correct" — the audits exist precisely because obvious-looking code is where the drift
hides.

## Layout

```
spec/protocol.md          the language-neutral wire contract
conformance/              cross-stack e2e — run.mjs (TS client), run.sh (harness), origin.mjs
typescript/               npm workspace: client/ (@debdattabasu/h2ts, shipped), server/ (planned)
rust/  crates/
  h2ts-client/            wasm32, no-hyper HTTP/2 client for Rust frontends
  h2ts-server/            serve + proxy; the h2ts-proxy binary; src/idle.rs (idle TTL)
  wslay-sys/              FFI to vendored wslay C (sub-frame WS streaming)
  h2ts-conformance/ h2ts-wasm-conformance/   dev-only conformance drivers
go/                       module github.com/debdattabasu/h2ts/go, package server (serve shape only)
doc/                      status.md (client/server audit), StatusJul10.md (Go server + idle-TTL audit)
Makefile                  fans out across stacks
```

## Build & test

```bash
make test            # rust + typescript + go + conformance (proxy gateway)
make test-rust       # cd rust && cargo test
make test-ts         # npm test + typecheck for @debdattabasu/h2ts
make test-go         # cd go && go vet ./... && go test ./...
make conformance     # client(s) -> Rust h2ts-proxy -> Node h2c origin
make conformance-go  # same battery, but against the Go serve gateway (GATEWAY=go)
```

Per stack, directly:

- **Rust:** `cd rust && cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
  (`tests/bridge.rs` shows a rustfmt-version diff under `--check`; it is pre-existing —
  don't reformat it as part of an unrelated change.)
- **Go:** `cd go && go test ./...`, `go test -race ./...` (the `Conn` has concurrent
  writers — always run `-race` on server changes), `gofmt -w server/`, `go vet ./...`.
- **TypeScript:** `cd typescript && npm test -w @debdattabasu/h2ts` (workspace is the
  scoped name, **not** `-w h2ts`), `npm run typecheck -w @debdattabasu/h2ts`.

Conformance selectors (env vars to `conformance/run.sh`):
`GATEWAY=proxy|go` (default `proxy`), `CLIENT=ts|rust|wasm|both|all` (default `both`),
`WS_URL=…`, `ORIGIN_PORT=…`. The battery is defined in `conformance/run.mjs` and every
gateway/client combination must pass it identically.

## Architecture notes worth carrying between sessions

- **Two server shapes.** *Proxy* pumps raw bytes to an upstream h2c server (below the
  h2 layer, no stream visibility). *Serve* terminates HTTP/2 in-process. The proxy is a
  **single** implementation — the Rust `h2ts-proxy` binary; other languages implement
  serve only. The Go server is **serve-only** by design.
- **HTTP/2 lib per stack.** Rust serve uses **hyper**; Go serve uses
  **`golang.org/x/net/http2`** (`ServeConn` for prior-knowledge h2c). WebSocket framing:
  Rust drives **wslay** (C via `wslay-sys`) for incremental sub-frame streaming; Go
  hand-rolls RFC 6455 framing in pure Go (`go/server/conn.go`). Both present the WS as a
  byte stream (`WsByteStream` in Rust, `Conn` implementing `net.Conn` in Go).
- **Liveness vs. idle — two different jobs, don't conflate them.**
  - *Keepalive* (WS ping/pong) detects a **dead** client; on by default in both servers.
  - *Idle TTL* reaps a **healthy but idle** connection — no open HTTP/2 streams for a
    timeout — with a graceful `GOAWAY`. It resets **only on streams**; WS and HTTP/2
    pings never count. It needs the h2 layer, so it exists in **serve** only, never the
    proxy (there it's the upstream's job). Go: `ServeConfig.IdleTimeout` (wired to
    `http2.Server.IdleTimeout`). Rust: `ServeConfig.idle_timeout` via
    `serve_h2_with_config` — hyper has no built-in idle timeout, so `src/idle.rs` counts
    open streams (a guard per request riding the response body) and drives
    `graceful_shutdown`.
- **Backpressure.** The byte pumps propagate backpressure rather than absorbing it; the
  Go `Conn` writes straight to the socket (no intermediate buffer), the Rust bridge uses
  a 64 KiB duplex. Control-frame writes share one write lock with data writes, so a
  frame lands only between whole frames (integrity under a concurrent ping).

## Conventions

- **Commits:** `type: summary — detail` (`feat`/`test`/`doc`/`release`/`chore`). Work
  lands on `main`.
- **Releases** (Rust → crates.io, TS → npm; no git tags): bump the version, update the
  crate/package README + `doc/`, `cargo publish --dry-run` (or `npm pack`), commit
  `release: <name> <version> — …`, then publish, then push. Only republish a crate whose
  own code changed (e.g. the idle-TTL change touched only `h2ts-server` → 0.1.2; the
  client crates and `wslay-sys` were untouched, so they stayed put). crates.io publishes
  are irreversible.
- **Docs:** substantial audits/decisions go in `doc/` (e.g. the flow-control and
  test-coverage analysis in `StatusJul10.md`), with a work log kept current.
- **Match the surrounding code** — comment density, naming, and idiom differ per stack;
  read a neighboring file before writing a new one.

## A note on how this was built

h2ts is developed in collaboration with **Claude (Anthropic's Claude Code)** — design
and direction by the author, with Claude implementing, testing, and documenting across
the TypeScript, Rust, and Go stacks. Keeping this `CLAUDE.md` is the author's chosen way
to acknowledge that, plainly and up front, rather than burying it. AI-assisted authorship
is still met with suspicion in parts of the open-source world; this is a small, honest
statement that it happened and that it works well and that the work stands on its tests and its rigorous human in the loop audit process.
