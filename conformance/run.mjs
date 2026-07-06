// Conformance: h2ts client (built dist) --ws--> h2ts-proxy --tcp--> Node h2c origin.
// Start the stack first (see README): the h2c origin (conformance/origin.mjs on
// :8000) and h2ts-proxy (WS gateway on :8091). Point WS_URL at another gateway to
// test it instead — e.g. the in-process `h2-server` serve_h2 example.
import { connectWebSocket } from "../typescript/client/dist/index.js";

const WS_URL = process.env.WS_URL || "ws://127.0.0.1:8091";
const AUTH = "127.0.0.1:8000";

let failures = 0;
function check(name, cond, extra = "") {
  const status = cond ? "ok  " : "FAIL";
  if (!cond) failures++;
  console.log(`[${status}] ${name}${extra ? "  (" + extra + ")" : ""}`);
}

// Offers the `h2ts` subprotocol by default; h2ts-proxy accepts and echoes it.
const client = await connectWebSocket(WS_URL);
console.log("connected\n");

// 1. Basic GET
const r1 = await client.request({ path: "/hello", authority: AUTH });
check("GET /hello -> 200", r1.status === 200, `status=${r1.status}`);
check("GET /hello body", (await r1.text()).toLowerCase().includes("hello"));

// 2. JSON
const r2 = await client.request({ path: "/json", authority: AUTH });
const j = await r2.json();
check("GET /json parsed", j.ok === true && j.path === "/json");
check("GET /json content-type", r2.headers["content-type"] === "application/json");

// 3. Small POST echo
const r3 = await client.request({ method: "POST", path: "/echo", authority: AUTH, body: "ping-pong" });
check("POST /echo -> 200", r3.status === 200);
check("POST /echo echoes body", (await r3.text()) === "ping-pong");

// 4. Large download (256 KiB) — inbound flow control across many DATA frames
const r4 = await client.request({ path: "/big", authority: AUTH });
const big = await r4.bytes();
check("GET /big size", big.length === 256 * 1024, `got ${big.length}`);
check("GET /big x-size header", r4.headers["x-size"] === String(256 * 1024));

// 5. Concurrent multiplexing (8 streams at once)
const many = await Promise.all(
  Array.from({ length: 8 }, () => client.request({ path: "/json", authority: AUTH }).then((r) => r.json())),
);
check("8 concurrent streams", many.length === 8 && many.every((x) => x.ok));

// 6. Large upload (512 KiB) — outbound flow control + content integrity
const payload = new Uint8Array(512 * 1024);
for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
const r6 = await client.request({ method: "POST", path: "/echo", authority: AUTH, body: payload });
const echoed = await r6.bytes();
check("512KiB upload echo size", echoed.length === payload.length, `got ${echoed.length}`);
let identical = echoed.length === payload.length;
for (let i = 0; identical && i < payload.length; i++) if (echoed[i] !== payload[i]) identical = false;
check("512KiB upload echo content", identical);
check("echo x-echo-bytes header", r6.headers["x-echo-bytes"] === String(payload.length));

// 7. Custom request header round-trips
const r7 = await client.request({ path: "/headers", authority: AUTH, headers: { "x-custom": "h2ts-rocks" } });
check("custom header reflected", r7.headers["x-saw-custom"] === "h2ts-rocks");

// 8. PING RTT
const rtt = await client.ping();
check("ping rtt >= 0", rtt >= 0, `rtt=${rtt.toFixed(2)}ms`);

// 9. Streaming body read via ReadableStream
const r9 = await client.request({ path: "/big", authority: AUTH });
let streamed = 0;
const reader = r9.body.getReader();
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  streamed += value.length;
}
check("streamed /big size", streamed === 256 * 1024, `got ${streamed}`);

// 10. 404 handling
const r10 = await client.request({ path: "/nope", authority: AUTH });
check("GET /nope -> 404", r10.status === 404, `status=${r10.status}`);

// Print the summary BEFORE teardown so results aren't lost if close() races.
console.log(failures === 0 ? "\n✅ ALL E2E PASSED" : `\n❌ ${failures} E2E FAILURE(S)`);

try {
  await client.close();
  console.log("closed cleanly");
} catch (err) {
  console.log("close() error (non-fatal):", err?.message ?? err);
}

await new Promise((r) => setTimeout(r, 50)); // let stdout flush
process.exit(failures ? 1 : 0);
