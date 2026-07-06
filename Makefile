# h2ts monorepo — top-level tasks. Each stack also builds/tests on its own;
# see the per-directory READMEs. Requires: node + npm, cargo (and go, once used).
.PHONY: test test-rust test-ts conformance build clean

# Run everything.
test: test-rust test-ts conformance

# Rust workspace (server, client scaffold, wslay-sys).
test-rust:
	cd rust && cargo test

# TypeScript workspace (client tests + strict typecheck).
test-ts:
	cd typescript && npm install && npm test -w h2ts && npm run typecheck -w h2ts

# Cross-stack end-to-end: client -> h2ts-proxy -> h2c origin.
conformance:
	bash conformance/run.sh

# Build the client bundle + the Rust workspace.
build:
	cd typescript && npm install && npm run build -w h2ts
	cd rust && cargo build

clean:
	cd rust && cargo clean
	rm -rf typescript/node_modules typescript/client/dist
