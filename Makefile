# h2ts monorepo — top-level tasks. Each stack also builds/tests on its own;
# see the per-directory READMEs. Requires: node + npm, cargo, go.
.PHONY: test test-rust test-ts test-go conformance conformance-go build clean

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

# Build the client bundle, the Rust workspace, and the Go module.
build:
	cd typescript && npm install && npm run build -w @debdattabasu/h2ts
	cd rust && cargo build
	cd go && go build ./...

clean:
	cd rust && cargo clean
	rm -rf typescript/node_modules typescript/client/dist go/bin
