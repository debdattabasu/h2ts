// The client must speak HTTP/2 with **prior knowledge** (RFC 7540 §3.4): open by
// sending the connection preface + SETTINGS and start issuing requests
// immediately, *without* an HTTP/1.1 `Upgrade: h2c` negotiation and without
// waiting for the server's preface. These tests drive the connection over an
// in-memory transport and assert that opening flight.
import { describe, expect, it } from "vitest";
import { concatBytes, decodeUtf8, encodeUtf8 } from "../src/bytes.js";
import { connect } from "../src/client.js";
import { FrameDecoder, serializeFrame } from "../src/frames/codec.js";
import { FrameType, type Frame } from "../src/frames/types.js";
import { HpackEncoder } from "../src/hpack/hpack.js";
import type { Transport } from "../src/transport/transport.js";

const PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/** An in-memory transport whose two directions the test drives directly. */
function mockTransport() {
  const c2s = new TransformStream<Uint8Array, Uint8Array>(); // client -> test
  const s2c = new TransformStream<Uint8Array, Uint8Array>(); // test -> client
  const transport: Transport = { readable: s2c.readable, writable: c2s.writable };
  return {
    transport,
    clientReader: c2s.readable.getReader(),
    serverWriter: s2c.writable.getWriter(),
  };
}

/** Read the client's opening bytes: the 24-byte preface, then `wantFrames` frames. */
async function readStartup(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  wantFrames: number,
): Promise<{ preface: string; frames: Frame[] }> {
  let buf: Uint8Array = new Uint8Array(0);
  while (buf.length < PREFACE.length) {
    const { value, done } = await reader.read();
    if (done) throw new Error("client stream closed before the preface");
    buf = concatBytes([buf, value!]);
  }
  const preface = decodeUtf8(buf.subarray(0, PREFACE.length));
  const dec = new FrameDecoder();
  const frames = dec.push(buf.subarray(PREFACE.length));
  while (frames.length < wantFrames) {
    const { value, done } = await reader.read();
    if (done) throw new Error("client stream closed before its opening frames");
    frames.push(...dec.push(value!));
  }
  return { preface, frames };
}

/** Read and discard everything the client writes, until the stream ends. */
async function drainRest(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
  try {
    for (;;) {
      const { done } = await reader.read();
      if (done) return;
    }
  } catch {
    /* reader released when the connection closes */
  }
}

describe("client connects via HTTP/2 prior knowledge (RFC 7540 §3.4)", () => {
  it("opens with the connection preface + SETTINGS, not an HTTP/1.1 Upgrade", async () => {
    const { transport, clientReader } = mockTransport();
    connect(transport);

    const { preface, frames } = await readStartup(clientReader, 1);
    // The literal h2 magic — not a `GET / HTTP/1.1 … Upgrade: h2c` request that
    // would await a `101 Switching Protocols` before HTTP/2 could begin.
    expect(preface).toBe(PREFACE);
    const settings = frames[0]!;
    expect(settings.type).toBe(FrameType.SETTINGS);
    if (settings.type === FrameType.SETTINGS) expect(settings.ack).toBe(false);
  });

  it("sends the first request before receiving any server bytes (no round-trip)", async () => {
    const { transport, clientReader } = mockTransport();
    const conn = connect(transport);

    // Fire a request. The test never feeds the client a single server byte — no
    // server SETTINGS, nothing. If the client gated its first request on the
    // server's preface (a round-trip), this HEADERS frame would never be written
    // and `readStartup` would hang until the test times out.
    const req = conn.request({ method: "GET", path: "/hello", authority: "example.com" });
    req.catch(() => {}); // never resolved (no response fed); swallow on teardown

    // Opening flight: SETTINGS, the connection-window WINDOW_UPDATE, then the
    // request HEADERS — find the HEADERS rather than assume its position.
    const { preface, frames } = await readStartup(clientReader, 3);
    expect(preface).toBe(PREFACE);
    expect(frames[0]!.type).toBe(FrameType.SETTINGS);
    const headers = frames.find((f) => f.type === FrameType.HEADERS);
    expect(headers?.type).toBe(FrameType.HEADERS);
    if (headers?.type === FrameType.HEADERS) expect(headers.streamId).toBe(1);
  });

  it("completes an optimistically-sent request when the server replies afterwards", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);

    const reqP = conn.request({ method: "GET", path: "/hello", authority: "example.com" });

    // Wait until the client has actually written its request HEADERS (stream 1)
    // so the response we feed lands on a stream that exists.
    const { frames } = await readStartup(clientReader, 3);
    expect(frames.some((f) => f.type === FrameType.HEADERS)).toBe(true);
    void drainRest(clientReader); // SETTINGS ack, WINDOW_UPDATEs, …

    // The server's preface and response arrive *after* the request was already
    // sent — the prior-knowledge, no-round-trip ordering.
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
    );
    const block = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    await serverWriter.write(
      serializeFrame({
        type: FrameType.HEADERS,
        streamId: 1,
        headerBlockFragment: block,
        endStream: false,
        endHeaders: true,
      }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("ok"), endStream: true }),
    );

    const res = await reqP;
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("ok");

    await conn.close();
  });
});
