#!/usr/bin/env bash
# Run the conformance suite against the default stack:
#   client(s) --ws--> h2ts-proxy --tcp--> Node h2c origin
# Builds what it needs, starts the stack, runs the checks, tears down.
#
# By default BOTH clients run against the same proxy (the TypeScript `h2ts` and
# the Rust `h2ts-client`). Select one with CLIENT=ts|rust|both (default both).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLIENT="${CLIENT:-both}"
run_ts() { [ "$CLIENT" = "ts" ] || [ "$CLIENT" = "both" ]; }
run_rust() { [ "$CLIENT" = "rust" ] || [ "$CLIENT" = "both" ]; }

if run_ts; then
  echo "==> building the TypeScript client"
  ( cd typescript && npm install --silent && npm run build -w h2ts --silent )
fi

if run_rust; then
  echo "==> building the Rust conformance client"
  ( cd rust && cargo build --quiet -p h2ts-conformance )
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

exit "$rc"
