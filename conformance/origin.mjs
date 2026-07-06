// h2c (cleartext HTTP/2, prior-knowledge) echo origin for the e2e: the upstream
// that `h2ts-proxy` forwards raw bytes to. Routes mirror the Rust `h2-server`
// example so the same run.mjs checks pass whichever gateway is in front.
//
//   run.mjs (h2ts client) --ws--> h2ts-proxy --tcp--> this origin (:8000)
import http2 from "node:http2";

const PORT = Number(process.env.ORIGIN_PORT || 8000);
const HOST = "127.0.0.1";

const server = http2.createServer();

server.on("stream", (stream, headers) => {
  // A gateway teardown resets streams abruptly; swallow those so Node doesn't
  // throw on an unhandled stream 'error'.
  stream.on("error", () => {});
  const method = headers[":method"];
  const path = headers[":path"];

  if (path === "/hello") {
    stream.respond({ "content-type": "text/plain; charset=utf-8", ":status": 200 });
    stream.end("hello from the h2c origin, tunneled over websocket!\n");
    return;
  }

  if (path === "/json") {
    stream.respond({ "content-type": "application/json", ":status": 200 });
    stream.end(JSON.stringify({ ok: true, method, path, ts: 1234567890 }));
    return;
  }

  if (path === "/big") {
    // 256 KiB body -> multiple DATA frames, exercises client WINDOW_UPDATE.
    const size = 256 * 1024;
    stream.respond({ "content-type": "application/octet-stream", "x-size": String(size), ":status": 200 });
    stream.end(Buffer.alloc(size, "x"));
    return;
  }

  if (path === "/echo") {
    // Echo the request body back (tests client upload / outbound flow control).
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => {
      const body = Buffer.concat(chunks);
      stream.respond({ "content-type": "application/octet-stream", "x-echo-bytes": String(body.length), ":status": 200 });
      stream.end(body);
    });
    return;
  }

  if (path === "/headers") {
    // Reflect a custom request header so the client can verify round-tripping.
    stream.respond({ "content-type": "text/plain", "x-saw-custom": headers["x-custom"] || "", ":status": 200 });
    stream.end("ok");
    return;
  }

  stream.respond({ ":status": 404 });
  stream.end("not found\n");
});

server.on("sessionError", (err) => console.error("[origin] sessionError:", err.message));
server.on("error", (err) => console.error("[origin] server error:", err.message));
server.listen(PORT, HOST, () => {
  console.error(`[origin] h2c echo server on tcp://${HOST}:${PORT}`);
});
