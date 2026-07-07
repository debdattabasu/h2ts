// Conformance driver for the Rust client's REAL browser WebSocket transport.
//
// Loads the wasm-compiled `h2ts-client` (built by `conformance/run.sh`, which runs
// `cargo build --target wasm32-unknown-unknown` + `wasm-bindgen --target nodejs`)
// and runs the shared battery under Node — which provides the global `WebSocket`
// the wasm `web.rs` transport binds to — against the gateway at WS_URL.
//
// The wasm side (`run_battery`) logs each check line and the summary via
// `console.log`, exactly like the TS (`run.mjs`) and native (`h2ts-conformance`)
// drivers; this loader only forwards WS_URL and maps the returned failure count to
// the process exit code.
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const gluePath = resolve(here, "../rust/target/wasm-conformance/h2ts_wasm_conformance.js");

let wasm;
try {
  // `--target nodejs` glue is CommonJS and loads the .wasm synchronously.
  wasm = require(gluePath);
} catch (err) {
  console.error(`failed to load wasm glue at ${gluePath}`);
  console.error("build it first (conformance/run.sh does this): cargo build -p h2ts-wasm-conformance --target wasm32-unknown-unknown && wasm-bindgen --target nodejs ...");
  console.error(err?.message ?? err);
  process.exit(2);
}

const WS_URL = process.env.WS_URL || "ws://127.0.0.1:8091";

let failures;
try {
  failures = await wasm.run_battery(WS_URL);
} catch (err) {
  console.error("wasm run_battery rejected (connection failed?):", err?.message ?? err);
  process.exit(1);
}

await new Promise((r) => setTimeout(r, 50)); // let console output flush
// An open WebSocket + the spawned driver keep the event loop alive; exit explicitly.
process.exit(failures ? 1 : 0);
