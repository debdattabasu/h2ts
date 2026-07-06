#!/usr/bin/env bash
# Run the conformance suite against the default stack:
#   h2ts client (built) --ws--> h2ts-proxy --tcp--> Node h2c origin
# Builds what it needs, starts the stack, runs the checks, tears down.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> building the TypeScript client"
( cd typescript && npm install --silent && npm run build -w h2ts --silent )

echo "==> building h2ts-proxy"
( cd rust && cargo build --quiet -p h2ts-server --bin h2ts-proxy )

echo "==> starting origin (:8000) and h2ts-proxy (:8091 -> :8000)"
node conformance/origin.mjs & ORIGIN=$!
rust/target/debug/h2ts-proxy 127.0.0.1:8091 127.0.0.1:8000 & PROXY=$!
cleanup() { kill "$ORIGIN" "$PROXY" 2>/dev/null || true; }
trap cleanup EXIT

# Wait until the proxy is accepting connections.
node --input-type=commonjs -e 'const net=require("net");let n=0;const t=setInterval(()=>{const s=net.connect(8091,"127.0.0.1");s.on("connect",()=>{s.end();clearInterval(t);process.exit(0)});s.on("error",()=>{s.destroy();if(++n>150){clearInterval(t);process.exit(1)}})},40)'

echo "==> running checks"
WS_URL="${WS_URL:-ws://127.0.0.1:8091}" node conformance/run.mjs
