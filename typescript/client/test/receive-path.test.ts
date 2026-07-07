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
