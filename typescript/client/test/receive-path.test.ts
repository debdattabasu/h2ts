// Receive-path (dispatch) robustness: how the client reacts to inbound control
// frames from the peer — RST_STREAM mid-transfer, GOAWAY. Mirrors the Rust
// client's tests/connection.rs cases so the two clients stay reconciled.
import { describe, expect, it } from "vitest";
import { decodeUtf8, encodeUtf8 } from "../src/bytes.js";
import { connect } from "../src/client.js";
import { FrameDecoder, serializeFrame } from "../src/frames/codec.js";
import { FrameType, type Frame } from "../src/frames/types.js";
import { HpackEncoder } from "../src/hpack/hpack.js";
import type { Transport } from "../src/transport/transport.js";

/** Server helper: SETTINGS + response HEADERS(:status 200, no END_STREAM). */
async function sendResponseHead(
  serverWriter: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  await serverWriter.write(
    serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
  );
  const head = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
  await serverWriter.write(
    serializeFrame({
      type: FrameType.HEADERS,
      streamId: 1,
      headerBlockFragment: head,
      endStream: false,
      endHeaders: true,
    }),
  );
}

const PREFACE_LEN = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".length;

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

/** Decodes the client's outbound frames, skipping the preface. */
class FrameReader {
  private dec = new FrameDecoder();
  private queue: Frame[] = [];
  private prefaceLeft = PREFACE_LEN;
  constructor(private reader: ReadableStreamDefaultReader<Uint8Array>) {}

  async next(): Promise<Frame> {
    for (;;) {
      if (this.queue.length) return this.queue.shift()!;
      const { value, done } = await this.reader.read();
      if (done) throw new Error("client stream closed");
      let bytes = value!;
      if (this.prefaceLeft > 0) {
        const take = Math.min(this.prefaceLeft, bytes.length);
        this.prefaceLeft -= take;
        bytes = bytes.subarray(take);
        if (bytes.length === 0) continue;
      }
      this.queue.push(...this.dec.push(bytes));
    }
  }

  /** The first frame matching `pred`. */
  async until(pred: (f: Frame) => boolean): Promise<Frame> {
    for (;;) {
      const f = await this.next();
      if (pred(f)) return f;
    }
  }

  /** The next DATA frame (skipping SETTINGS acks, WINDOW_UPDATEs, …). */
  async nextData(): Promise<{ data: Uint8Array; endStream: boolean }> {
    for (;;) {
      const f = await this.next();
      if (f.type === FrameType.DATA) return { data: f.data, endStream: f.endStream };
    }
  }

  /** Keep consuming (discarding) so the transport never back-pressures. */
  async drain(): Promise<void> {
    try {
      for (;;) await this.next();
    } catch {
      /* stream closed */
    }
  }
}

describe("receive path: RST_STREAM", () => {
  it("mid-upload rejects the request without hanging", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    // A body that yields one chunk then blocks forever (upload "in progress").
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        controller.enqueue(encodeUtf8("part1"));
        await new Promise(() => {}); // never enqueue more, never close
      },
    });
    const reqP = conn.request({ method: "POST", path: "/upload", authority: "e", body });
    reqP.catch(() => {}); // benign: also asserted below

    // Wait until the client has actually sent the first body DATA, then reset.
    await frames.until((f) => f.type === FrameType.DATA);
    void frames.drain();
    await serverWriter.write(
      serializeFrame({ type: FrameType.RST_STREAM, streamId: 1, errorCode: 8 }),
    );

    await expect(reqP).rejects.toThrow();
  }, 8000);
});

describe("receive path: GOAWAY", () => {
  it("with a non-zero error code fails the request and closes the connection", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();

    // last_stream_id 0 (so our stream 1 > 0 is doomed), PROTOCOL_ERROR.
    await serverWriter.write(
      serializeFrame({
        type: FrameType.GOAWAY,
        streamId: 0,
        lastStreamId: 0,
        errorCode: 1,
        debugData: new Uint8Array(0),
      }),
    );

    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);
});

describe("receive path: RST mid-download + trailers", () => {
  it("RST_STREAM mid-download errors the response body", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();

    await sendResponseHead(serverWriter);
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("one"), endStream: false }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.RST_STREAM, streamId: 1, errorCode: 8 }),
    );

    const res = await reqP;
    expect(res.status).toBe(200);
    // The head resolved, but buffering the truncated body must reject.
    await expect(res.bytes()).rejects.toThrow();
  }, 8000);

  it("surfaces response trailers", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/rpc", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();

    await sendResponseHead(serverWriter);
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("data"), endStream: false }),
    );
    // Trailers: a second HEADERS block carrying END_STREAM.
    const trailers = new HpackEncoder().encode([{ name: "grpc-status", value: "0" }]);
    await serverWriter.write(
      serializeFrame({
        type: FrameType.HEADERS,
        streamId: 1,
        headerBlockFragment: trailers,
        endStream: true,
        endHeaders: true,
      }),
    );

    const res = await reqP;
    expect(res.status).toBe(200);
    expect(decodeUtf8(await res.bytes())).toBe("data");
    expect(res.trailers()).toEqual({ "grpc-status": "0" });
  }, 8000);
});

describe("receive path: flow control + GOAWAY + framing", () => {
  it("honors a retroactively-shrunk send window", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const total = 65535 + 1000;
    const body = new Uint8Array(total);
    const reqP = conn.request({ method: "POST", path: "/upload", authority: "e", body });
    reqP.catch(() => {});

    await frames.until((f) => f.type === FrameType.HEADERS);
    // Take the connection window out of the picture — only the stream window gates.
    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 0, windowSizeIncrement: 1_000_000 }),
    );

    // The client sends exactly the initial 65535-byte window, then parks.
    let sent = 0;
    while (sent < 65535) {
      const d = await frames.nextData();
      expect(d.endStream).toBe(false);
      sent += d.data.length;
    }
    expect(sent).toBe(65535);

    // Shrink the initial window to 100 → the live stream window becomes -65435.
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: { initialWindowSize: 100 } }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 1, windowSizeIncrement: 65445 }),
    );

    // Exactly 10 bytes released — proof the client tracked the negative window.
    const first = await frames.nextData();
    expect(first.data.length).toBe(10);
    sent += first.data.length;

    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 1, windowSizeIncrement: 2000 }),
    );
    for (;;) {
      const d = await frames.nextData();
      sent += d.data.length;
      if (d.endStream) break;
    }
    expect(sent).toBe(total);
  }, 8000);

  it("graceful GOAWAY fails higher streams but lets lower ones finish", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const r1 = conn.request({ path: "/a", authority: "e" });
    const r3 = conn.request({ path: "/b", authority: "e" });
    r3.catch(() => {});

    await frames.until((f) => f.type === FrameType.HEADERS && f.streamId === 3);
    void frames.drain();

    // Graceful GOAWAY: lastStreamId 1 dooms stream 3 but not stream 1.
    await serverWriter.write(
      serializeFrame({
        type: FrameType.GOAWAY,
        streamId: 0,
        lastStreamId: 1,
        errorCode: 0,
        debugData: new Uint8Array(0),
      }),
    );
    await sendResponseHead(serverWriter);
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("ok"), endStream: true }),
    );

    await expect(r3).rejects.toThrow();
    const res1 = await r1;
    expect(res1.status).toBe(200);
    expect(decodeUtf8(await res1.bytes())).toBe("ok");
  }, 8000);

  it("connection-level WINDOW_UPDATE(0) tears down with a GOAWAY", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);

    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 0, windowSizeIncrement: 0 }),
    );

    // The client answers a connection error with a GOAWAY, then closes.
    const goaway = await frames.until((f) => f.type === FrameType.GOAWAY);
    expect(goaway.type).toBe(FrameType.GOAWAY);
    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);

  it("reassembles a header block split across CONTINUATION", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();

    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
    );
    const block = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    const mid = Math.floor(block.length / 2);
    await serverWriter.write(
      serializeFrame({
        type: FrameType.HEADERS,
        streamId: 1,
        headerBlockFragment: block.subarray(0, mid),
        endStream: false,
        endHeaders: false,
      }),
    );
    await serverWriter.write(
      serializeFrame({
        type: FrameType.CONTINUATION,
        streamId: 1,
        headerBlockFragment: block.subarray(mid),
        endHeaders: true,
      }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: new Uint8Array(0), endStream: true }),
    );

    const res = await reqP;
    expect(res.status).toBe(200);
  }, 8000);

  it("an unterminated header block interrupted by DATA tears down with a GOAWAY", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);

    const block = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    // HEADERS without END_HEADERS...
    await serverWriter.write(
      serializeFrame({
        type: FrameType.HEADERS,
        streamId: 1,
        headerBlockFragment: block,
        endStream: false,
        endHeaders: false,
      }),
    );
    // ...then DATA instead of the required CONTINUATION.
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("nope"), endStream: false }),
    );

    const goaway = await frames.until((f) => f.type === FrameType.GOAWAY);
    expect(goaway.type).toBe(FrameType.GOAWAY);
    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);
});
