# h2ts monorepo — top-level tasks. Each stack also builds/tests on its own;
# see the per-directory READMEs. Requires: node + npm, cargo, go.
.PHONY: test test-rust test-ts test-go conformance conformance-go docs build clean

# Run everything.
test: test-rust test-ts test-go conformance

# Rust workspace (server, client scaffold, wslay-sys).
test-rust:
	cd rust && cargo test

# TypeScript workspace (client tests + strict typecheck).
test-ts:
	cd typescript && npm install && npm test -w @debdattabasu/h2ts && npm run typecheck -w @debdattabasu/h2ts

# Go module (serve gateway: framing, handshake, keepalive, h2-over-WS).
test-go:
	cd go && go vet ./... && go test ./...

# Cross-stack end-to-end: client -> h2ts-proxy -> h2c origin.
conformance:
	bash conformance/run.sh

# Cross-stack end-to-end against the Go serve gateway (in-process h2c).
conformance-go:
	GATEWAY=go bash conformance/run.sh

# Rustdoc for the three published crates, gated the way docs.rs gates them.
# Catches broken intra-doc links locally; catching the *other* docs.rs failure mode
# (vendored C that only a modern toolchain rejects) needs CC=gcc-14 on Linux, which
# is why CI runs this with that pinned — see doc/DocsRsJul30.md.
docs:
	cd rust && RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p h2ts-server -p wslay-sys -p h2ts-client

# Build the client bundle, the Rust workspace, and the Go module.
build:
	cd typescript && npm install && npm run build -w @debdattabasu/h2ts
	cd rust && cargo build
	cd go && go build ./...

clean:
	cd rust && cargo clean
	rm -rf typescript/node_modules typescript/client/dist go/bin
