#!/usr/bin/env bash
# Run the conformance suite against the default stack:
#   client(s) --ws--> h2ts-proxy --tcp--> Node h2c origin
# Builds what it needs, starts the stack, runs the checks, tears down.
#
# By default BOTH clients run against the same proxy (the TypeScript `h2ts` and
# the Rust `h2ts-client`, native transport). CLIENT selects which:
#   ts | rust | wasm | both (=ts+rust, default) | all (=ts+rust+wasm)
# `wasm` compiles the Rust client to wasm32 and drives its REAL browser WebSocket
# transport (src/web.rs) under Node — needs the wasm32 target + wasm-bindgen CLI.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLIENT="${CLIENT:-both}"
run_ts() { [ "$CLIENT" = "ts" ] || [ "$CLIENT" = "both" ] || [ "$CLIENT" = "all" ]; }
run_rust() { [ "$CLIENT" = "rust" ] || [ "$CLIENT" = "both" ] || [ "$CLIENT" = "all" ]; }
run_wasm() { [ "$CLIENT" = "wasm" ] || [ "$CLIENT" = "all" ]; }

if run_ts; then
  echo "==> building the TypeScript client"
  ( cd typescript && npm install --silent && npm run build -w h2ts --silent )
fi

if run_rust; then
  echo "==> building the Rust conformance client"
  ( cd rust && cargo build --quiet -p h2ts-conformance )
fi

if run_wasm; then
  echo "==> building the Rust wasm client (real WebSocket transport)"
  command -v wasm-bindgen >/dev/null || {
    echo "wasm-bindgen CLI not found — install the matching version:" >&2
    echo "  rustup target add wasm32-unknown-unknown" >&2
    echo "  cargo install wasm-bindgen-cli --version 0.2.126" >&2
    exit 1
  }
  ( cd rust && cargo build --quiet -p h2ts-wasm-conformance --target wasm32-unknown-unknown )
  wasm-bindgen --target nodejs \
    --out-dir rust/target/wasm-conformance \
    --out-name h2ts_wasm_conformance \
    rust/target/wasm32-unknown-unknown/debug/h2ts_wasm_conformance.wasm
fi

echo "==> building h2ts-proxy"
( cd rust && cargo build --quiet -p h2ts-server --bin h2ts-proxy )

# The h2c origin port (override if :8000 is taken locally); the proxy forwards here.
ORIGIN_PORT="${ORIGIN_PORT:-8000}"
echo "==> starting origin (:$ORIGIN_PORT) and h2ts-proxy (:8091 -> :$ORIGIN_PORT)"
ORIGIN_PORT="$ORIGIN_PORT" node conformance/origin.mjs & ORIGIN=$!
rust/target/debug/h2ts-proxy 127.0.0.1:8091 "127.0.0.1:$ORIGIN_PORT" & PROXY=$!
cleanup() { kill "$ORIGIN" "$PROXY" 2>/dev/null || true; }
trap cleanup EXIT

# Wait until the proxy is accepting connections.
node --input-type=commonjs -e 'const net=require("net");let n=0;const t=setInterval(()=>{const s=net.connect(8091,"127.0.0.1");s.on("connect",()=>{s.end();clearInterval(t);process.exit(0)});s.on("error",()=>{s.destroy();if(++n>150){clearInterval(t);process.exit(1)}})},40)'

WS_URL="${WS_URL:-ws://127.0.0.1:8091}"
rc=0

if run_ts; then
  echo "==> running checks (TypeScript client)"
  WS_URL="$WS_URL" node conformance/run.mjs || rc=1
fi

if run_rust; then
  echo "==> running checks (Rust client)"
  WS_URL="$WS_URL" rust/target/debug/h2ts-conformance || rc=1
fi

if run_wasm; then
  echo "==> running checks (Rust wasm client, real WebSocket)"
  WS_URL="$WS_URL" node conformance/wasm-run.mjs || rc=1
fi

exit "$rc"
