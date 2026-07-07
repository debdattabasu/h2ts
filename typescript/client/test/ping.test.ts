// PING behavior: a successful round-trip resolves with the RTT, and a PING that
// is in flight when the connection closes REJECTS with the close error (never
// resolves a bogus RTT). This mirrors the Rust client, which returns `Err`.
import { describe, expect, it } from "vitest";
import { connect } from "../src/client.js";
import { FrameDecoder, serializeFrame } from "../src/frames/codec.js";
import { FrameType, type Frame } from "../src/frames/types.js";
import type { Transport } from "../src/transport/transport.js";

const PREFACE_LEN = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".length;

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

/** Decodes the client's outbound frame stream, transparently skipping the preface. */
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

  /** The next PING frame the client sends (skipping SETTINGS etc.). */
  async nextPing(): Promise<Uint8Array> {
    for (;;) {
      const f = await this.next();
      if (f.type === FrameType.PING && !f.ack) return f.opaqueData;
    }
  }

  /**
   * Keep consuming the client's outbound stream (discarding it) until it ends.
   * Without this, an unread TransformStream back-pressures and a later
   * `conn.close()` (which awaits a GOAWAY write) would hang.
   */
  async drain(): Promise<void> {
    try {
      for (;;) await this.next();
    } catch {
      /* stream closed */
    }
  }
}

describe("PING", () => {
  it("resolves with a round-trip time when the peer ACKs", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const rttP = conn.ping();
    const opaque = await frames.nextPing();
    void frames.drain(); // keep the outbound flowing so close() doesn't back up
    // Echo the client's PING back as an ACK carrying the same opaque payload.
    await serverWriter.write(
      serializeFrame({ type: FrameType.PING, streamId: 0, ack: true, opaqueData: opaque }),
    );

    const rtt = await rttP;
    expect(rtt).toBeGreaterThanOrEqual(0);
    await conn.close();
  });

  it("rejects when the connection closes with the ping in flight", async () => {
    const { transport, clientReader, serverWriter } = mockTransport();
    const conn = connect(transport);
    const frames = new FrameReader(clientReader);

    const rttP = conn.ping();
    await frames.nextPing(); // ensure the PING was actually sent (registered in flight)
    void frames.drain();

    // Close the transport from the peer side -> the client tears down.
    await serverWriter.close();
    await expect(rttP).rejects.toThrow();
  });

  it("rejects immediately when the connection is already closed", async () => {
    const { transport, clientReader } = mockTransport();
    const conn = connect(transport);
    void new FrameReader(clientReader).drain(); // drain so close() can flush GOAWAY
    await conn.close();
    await expect(conn.ping()).rejects.toThrow();
  });
});
