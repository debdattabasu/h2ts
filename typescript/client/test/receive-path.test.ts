// Receive-path (dispatch) robustness: how the client reacts to inbound control
// frames from the peer — RST_STREAM mid-transfer, GOAWAY. Mirrors the Rust
// client's tests/connection.rs cases so the two clients stay reconciled.
import { describe, expect, it } from "vitest";
import { concatBytes, decodeUtf8, encodeUtf8 } from "../src/bytes.js";
import { connect } from "../src/client.js";
import { FrameDecoder, serializeFrame } from "../src/frames/codec.js";
import { FrameType, type Frame } from "../src/frames/types.js";
import { HpackDecoder, HpackEncoder } from "../src/hpack/hpack.js";
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

describe("receive path: early complete response vs in-flight upload", () => {
  it("finishes a flow-limited upload after an early complete response", async () => {
    // Regression: the server completes its response (END_STREAM) before the
    // upload finishes, AND the body is larger than the flow-control window — so
    // the client must honor a stream-level WINDOW_UPDATE that arrives AFTER the
    // early response. A prior bug retired the stream on the peer's END_STREAM,
    // dropping that WINDOW_UPDATE and hanging the pump (silent upload truncation).
    // Mirrors the Rust client's flow-limited early-complete test.
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const total = 100_000; // > the default 65535 send window: the pump WILL park
    const body = new Uint8Array(total);
    const reqP = conn.request({ method: "POST", path: "/upload", authority: "e", body });
    reqP.catch(() => {});

    await frames.until((f) => f.type === FrameType.HEADERS);

    // Drain exactly the initial 65535-byte window (conn + stream both start there).
    let sent = 0;
    while (sent < 65535) {
      const d = await frames.nextData();
      expect(d.endStream).toBe(false);
      sent += d.data.length;
    }
    expect(sent).toBe(65535);

    // A COMPLETE response NOW (headers-only, END_STREAM) — while still uploading.
    const head = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    await serverWriter.write(
      serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: head, endStream: true, endHeaders: true }),
    );

    // The caller sees a clean 200 while the send side is still open.
    const res = await reqP;
    expect(res.status).toBe(200);

    // The server keeps the request side open and grants more window (a well-behaved
    // origin draining the body) so the client can finish uploading.
    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 0, windowSizeIncrement: 1_000_000 }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 1, windowSizeIncrement: 1_000_000 }),
    );

    // The client must upload the remainder and send its own END_STREAM.
    for (;;) {
      const d = await frames.nextData();
      sent += d.data.length;
      if (d.endStream) break;
    }
    expect(sent).toBe(total);
  }, 8000);
});

describe("receive path: interim responses, settings validation, header cap", () => {
  it("treats a 1xx interim response as non-final and keeps the real response", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();

    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
    );
    // Interim 103 Early Hints (no END_STREAM) — must NOT be surfaced as the response.
    const early = new HpackEncoder().encode([
      { name: ":status", value: "103" },
      { name: "link", value: "</a.css>; rel=preload" },
    ]);
    await serverWriter.write(
      serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: early, endStream: false, endHeaders: true }),
    );
    // The real final response follows.
    const final = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    await serverWriter.write(
      serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: final, endStream: false, endHeaders: true }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("body"), endStream: true }),
    );

    const res = await reqP;
    expect(res.status).toBe(200); // the 103 was not mistaken for the final response
    expect(decodeUtf8(await res.bytes())).toBe("body");
    expect(res.trailers()).toBeUndefined(); // ...nor was the real response filed as trailers
  }, 8000);

  it("tears down with a GOAWAY on an out-of-range SETTINGS_MAX_FRAME_SIZE", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);

    // maxFrameSize below the 16384 floor is a PROTOCOL_ERROR (§6.5.2).
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: { maxFrameSize: 100 } }),
    );

    const goaway = await frames.until((f) => f.type === FrameType.GOAWAY);
    expect(goaway.type).toBe(FrameType.GOAWAY);
    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);

  it("tears down when a header block exceeds the size cap (CONTINUATION flood)", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
    );

    // HEADERS (no END_HEADERS) then CONTINUATION frames whose total exceeds the
    // 1 MiB cap — the client must bail rather than buffer unboundedly.
    const frag = new Uint8Array(16000);
    void serverWriter
      .write(serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: frag, endStream: false, endHeaders: false }))
      .catch(() => {});
    for (let i = 0; i < 70; i++) {
      void serverWriter
        .write(serializeFrame({ type: FrameType.CONTINUATION, streamId: 1, headerBlockFragment: frag, endHeaders: false }))
        .catch(() => {});
    }

    const goaway = await frames.until((f) => f.type === FrameType.GOAWAY);
    expect(goaway.type).toBe(FrameType.GOAWAY);
    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);
});

describe("receive path: coverage gaps", () => {
  it("resets only the stream on a stream-level WINDOW_UPDATE(0)", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);
    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);

    // §6.9.1: a stream WINDOW_UPDATE with a 0 increment is a stream error.
    await serverWriter.write(
      serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 1, windowSizeIncrement: 0 }),
    );

    const rst = await frames.until((f) => f.type === FrameType.RST_STREAM && f.streamId === 1);
    expect(rst.type).toBe(FrameType.RST_STREAM);
    await expect(reqP).rejects.toThrow(); // request fails (not hang)
    expect(conn.isClosed).toBe(false); // ...but the connection survives
  }, 8000);

  it("tears down with a GOAWAY on a frame larger than the advertised max", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);
    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS);

    // 20000 bytes exceeds the default 16384 SETTINGS_MAX_FRAME_SIZE.
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: new Uint8Array(20000), endStream: false }),
    );

    const goaway = await frames.until((f) => f.type === FrameType.GOAWAY);
    expect(goaway.type).toBe(FrameType.GOAWAY);
    await expect(reqP).rejects.toThrow();
    expect(conn.isClosed).toBe(true);
  }, 8000);

  it("strips padding from a padded HEADERS frame on receive", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);
    const reqP = conn.request({ path: "/x", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);
    void frames.drain();
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: {} }),
    );

    // Hand-build a PADDED HEADERS frame (serializeFrame doesn't emit padding).
    const block = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    const padLen = 4;
    const payload = new Uint8Array(1 + block.length + padLen);
    payload[0] = padLen; // pad length
    payload.set(block, 1); // ...then the block, then `padLen` zero bytes
    const header = new Uint8Array(9);
    const len = payload.length;
    header[0] = (len >> 16) & 0xff;
    header[1] = (len >> 8) & 0xff;
    header[2] = len & 0xff;
    header[3] = 0x1; // HEADERS
    header[4] = 0x4 | 0x8; // END_HEADERS | PADDED
    header[8] = 1; // stream 1
    await serverWriter.write(concatBytes([header, payload]));
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: new Uint8Array(0), endStream: true }),
    );

    const res = await reqP;
    expect(res.status).toBe(200); // padding stripped, headers decoded
  }, 8000);

  it("refuses an inbound PUSH_PROMISE with RST_STREAM (REFUSED_STREAM)", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport); // no onPush → pushes refused
    const frames = new FrameReader(clientReader);
    const reqP = conn.request({ path: "/x", authority: "e" });
    reqP.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS && f.streamId === 1);

    const pushHeaders = new HpackEncoder().encode([
      { name: ":method", value: "GET" },
      { name: ":scheme", value: "http" },
      { name: ":authority", value: "e" },
      { name: ":path", value: "/pushed" },
    ]);
    await serverWriter.write(
      serializeFrame({
        type: FrameType.PUSH_PROMISE,
        streamId: 1,
        promisedStreamId: 2,
        headerBlockFragment: pushHeaders,
        endHeaders: true,
      }),
    );

    const rst = await frames.until((f) => f.type === FrameType.RST_STREAM && f.streamId === 2);
    if (rst.type === FrameType.RST_STREAM) expect(rst.errorCode).toBe(7); // REFUSED_STREAM
  }, 8000);

  it("splits an oversized request header block into HEADERS + CONTINUATION on send", async () => {
    const { transport, clientReader } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    // Big enough that even Huffman-coded (~5 bits/char) the block exceeds the
    // default 16384 max frame size, forcing a HEADERS + CONTINUATION split.
    const bigValue = "a".repeat(40000);
    const reqP = conn.request({ path: "/x", authority: "e", headers: { "x-big": bigValue } });
    reqP.catch(() => {});

    const h = await frames.until((f) => f.type === FrameType.HEADERS);
    const fragments: Uint8Array[] = [];
    if (h.type === FrameType.HEADERS) {
      expect(h.endHeaders).toBe(false); // didn't fit one frame
      fragments.push(h.headerBlockFragment);
    }
    for (;;) {
      const c = await frames.next();
      if (c.type === FrameType.CONTINUATION) {
        fragments.push(c.headerBlockFragment);
        if (c.endHeaders) break;
      }
    }
    const decoded = new HpackDecoder().decode(concatBytes(fragments));
    expect(decoded.some((hd) => hd.name === "x-big" && hd.value === bigValue)).toBe(true);
  }, 8000);
});

describe("receive path: backpressure", () => {
  it("grows the connection receive window at startup", async () => {
    const { transport, clientReader } = mockTransport();
    connect(transport, { connectionWindowSize: 1_000_000 });
    const frames = new FrameReader(clientReader);
    const wu = await frames.until((f) => f.type === FrameType.WINDOW_UPDATE && f.streamId === 0);
    if (wu.type === FrameType.WINDOW_UPDATE) {
      expect(wu.windowSizeIncrement).toBe(1_000_000 - 65535); // grown past the spec default
    }
  }, 8000);

  it("replenishes the receive window only as the body is consumed", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const reqP = conn.request({ path: "/download", authority: "e" });
    await frames.until((f) => f.type === FrameType.HEADERS);

    // Record outbound frames from here (skips the startup connection WINDOW_UPDATE).
    const seen: Frame[] = [];
    void (async () => {
      try {
        for (;;) seen.push(await frames.next());
      } catch {
        /* closed */
      }
    })();
    const tick = () => new Promise((r) => setTimeout(r, 15));
    const streamWU = () => seen.filter((f) => f.type === FrameType.WINDOW_UPDATE && f.streamId === 1);
    const connWU5 = () =>
      seen.filter((f) => f.type === FrameType.WINDOW_UPDATE && f.streamId === 0 && f.windowSizeIncrement === 5);

    await sendResponseHead(serverWriter); // SETTINGS + HEADERS(200, no END_STREAM)
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("hello"), endStream: false }),
    );

    const res = await reqP;
    expect(res.status).toBe(200);

    // Backpressure: nobody has read the body, so NO window has been returned yet
    // (the old client replenished eagerly on receipt).
    await tick();
    expect(streamWU().length).toBe(0);
    expect(connWU5().length).toBe(0);

    // Read one chunk → the client returns exactly those 5 bytes to both windows.
    const reader = res.body.getReader();
    const { value } = await reader.read();
    expect(decodeUtf8(value!)).toBe("hello");

    const start = Date.now();
    while (streamWU().length === 0) {
      if (Date.now() - start > 2000) throw new Error("no WINDOW_UPDATE after consumption");
      await tick();
    }
    const wu = streamWU()[0]!;
    if (wu.type === FrameType.WINDOW_UPDATE) expect(wu.windowSizeIncrement).toBe(5);
    expect(connWU5().length).toBe(1); // connection window replenished for the same bytes
  }, 8000);
});

describe("receive path: max concurrent streams", () => {
  it("honors the peer's SETTINGS_MAX_CONCURRENT_STREAMS", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    // req1 (bodyless): its stream stays open until we complete it below.
    const r1 = conn.request({ path: "/a", authority: "e" });
    r1.catch(() => {});
    await frames.until((f) => f.type === FrameType.HEADERS && f.streamId === 1);

    // Advertise a limit of 1; the client's SETTINGS ack means it has applied it.
    await serverWriter.write(
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: { maxConcurrentStreams: 1 } }),
    );
    await frames.until((f) => f.type === FrameType.SETTINGS && f.ack === true);

    // Record every subsequent outbound frame so we can watch for a stream opening.
    const seen: Frame[] = [];
    void (async () => {
      try {
        for (;;) seen.push(await frames.next());
      } catch {
        /* connection closed */
      }
    })();
    const tick = () => new Promise((r) => setTimeout(r, 15));
    const openedStream3 = () => seen.some((f) => f.type === FrameType.HEADERS && f.streamId === 3);

    // req2 must PARK — stream 1 is open, so we're at the limit of 1.
    const r2 = conn.request({ path: "/b", authority: "e" });
    r2.catch(() => {});
    await tick();
    expect(openedStream3()).toBe(false); // parked: no second stream opened

    // Complete stream 1 → frees the slot.
    const head1 = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    await serverWriter.write(
      serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: head1, endStream: false, endHeaders: true }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: encodeUtf8("a"), endStream: true }),
    );

    // The parked request now opens stream 3.
    const start = Date.now();
    while (!openedStream3()) {
      if (Date.now() - start > 2000) throw new Error("parked request never opened stream 3");
      await tick();
    }

    const head3 = new HpackEncoder().encode([{ name: ":status", value: "200" }]);
    await serverWriter.write(
      serializeFrame({ type: FrameType.HEADERS, streamId: 3, headerBlockFragment: head3, endStream: false, endHeaders: true }),
    );
    await serverWriter.write(
      serializeFrame({ type: FrameType.DATA, streamId: 3, data: encodeUtf8("b"), endStream: true }),
    );
    const res2 = await r2;
    expect(res2.status).toBe(200);
  }, 8000);
});

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
