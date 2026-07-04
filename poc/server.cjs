// Backend: Node built-in HTTP/2 cleartext (h2c) echo server, prior-knowledge.
// Exercises the h2ts client: routing, JSON, body echo (uploads), a large
// response (inbound flow control), and concurrent multiplexed streams.
const http2 = require("http2");

const server = http2.createServer();

server.on("stream", (stream, headers) => {
  const method = headers[":method"];
  const path = headers[":path"];
  console.error(`[server] ${method} ${path}`);

  if (path === "/hello") {
    stream.respond({ "content-type": "text/plain; charset=utf-8", ":status": 200 });
    stream.end("hello from h2c server, tunneled over websocket!\n");
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
    const buf = Buffer.alloc(size, "x");
    stream.respond({ "content-type": "application/octet-stream", "x-size": String(size), ":status": 200 });
    stream.end(buf);
    return;
  }

  if (path === "/echo") {
    // Echo request body back (tests client upload / outbound flow control).
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
    // Reflect a custom request header so we can verify header round-tripping.
    stream.respond({ "content-type": "text/plain", "x-saw-custom": headers["x-custom"] || "", ":status": 200 });
    stream.end("ok");
    return;
  }

  stream.respond({ ":status": 404 });
  stream.end("not found\n");
});

server.on("sessionError", (err) => console.error("[server] sessionError:", err.message));
server.listen(8000, "127.0.0.1", () => {
  console.error("[server] h2c echo server on tcp://127.0.0.1:8000");
});
