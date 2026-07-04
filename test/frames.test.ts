import { describe, expect, it } from "vitest";
import { FrameDecoder, serializeFrame } from "../src/frames/codec.js";
import { FrameType, type Frame } from "../src/frames/types.js";

function roundTrip(frame: Frame): Frame {
  const bytes = serializeFrame(frame);
  const frames = new FrameDecoder().push(bytes);
  expect(frames).toHaveLength(1);
  return frames[0]!;
}

const bytesOf = (n: number[]) => Uint8Array.from(n);
const toHex = (b: Uint8Array) => [...b].map((x) => x.toString(16).padStart(2, "0")).join("");

describe("frame serialization (known wire bytes)", () => {
  it("SETTINGS ack is a 9-byte empty frame", () => {
    const b = serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: true, settings: {} });
    expect(toHex(b)).toBe("000000040100000000");
  });

  it("WINDOW_UPDATE encodes increment", () => {
    const b = serializeFrame({ type: FrameType.WINDOW_UPDATE, streamId: 0, windowSizeIncrement: 5 });
    expect(toHex(b)).toBe("000004080000000000" + "00000005");
  });

  it("SETTINGS encodes parameters in order", () => {
    const b = serializeFrame({
      type: FrameType.SETTINGS,
      streamId: 0,
      ack: false,
      settings: { enablePush: false, initialWindowSize: 65535 },
    });
    // header: len=12,type=4,flags=0,stream=0 ; then (id=2 -> 0)(id=4 -> 65535)
    expect(toHex(b)).toBe("00000c04" + "00" + "00000000" + "000200000000" + "00040000ffff");
  });
});

describe("frame round-trips", () => {
  it("DATA with END_STREAM", () => {
    const f = roundTrip({ type: FrameType.DATA, streamId: 3, data: bytesOf([1, 2, 3, 4]), endStream: true });
    expect(f.type).toBe(FrameType.DATA);
    if (f.type === FrameType.DATA) {
      expect([...f.data]).toEqual([1, 2, 3, 4]);
      expect(f.endStream).toBe(true);
    }
  });

  it("HEADERS with priority + flags", () => {
    const f = roundTrip({
      type: FrameType.HEADERS,
      streamId: 5,
      headerBlockFragment: bytesOf([0x82, 0x84]),
      endStream: false,
      endHeaders: true,
      priority: { streamDependency: 3, weight: 16, exclusive: true },
    });
    expect(f.type).toBe(FrameType.HEADERS);
    if (f.type === FrameType.HEADERS) {
      expect([...f.headerBlockFragment]).toEqual([0x82, 0x84]);
      expect(f.endHeaders).toBe(true);
      expect(f.endStream).toBe(false);
      expect(f.priority).toEqual({ streamDependency: 3, weight: 16, exclusive: true });
    }
  });

  it("RST_STREAM / PING / GOAWAY / WINDOW_UPDATE / PRIORITY", () => {
    const rst = roundTrip({ type: FrameType.RST_STREAM, streamId: 7, errorCode: 8 });
    expect(rst.type === FrameType.RST_STREAM && rst.errorCode).toBe(8);

    const ping = roundTrip({ type: FrameType.PING, streamId: 0, ack: true, opaqueData: bytesOf([1, 2, 3, 4, 5, 6, 7, 8]) });
    expect(ping.type === FrameType.PING && ping.ack).toBe(true);
    if (ping.type === FrameType.PING) expect([...ping.opaqueData]).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);

    const goaway = roundTrip({ type: FrameType.GOAWAY, streamId: 0, lastStreamId: 9, errorCode: 1, debugData: bytesOf([0x61]) });
    if (goaway.type === FrameType.GOAWAY) {
      expect(goaway.lastStreamId).toBe(9);
      expect(goaway.errorCode).toBe(1);
      expect([...goaway.debugData]).toEqual([0x61]);
    }

    const wu = roundTrip({ type: FrameType.WINDOW_UPDATE, streamId: 1, windowSizeIncrement: 1000 });
    expect(wu.type === FrameType.WINDOW_UPDATE && wu.windowSizeIncrement).toBe(1000);

    const prio = roundTrip({ type: FrameType.PRIORITY, streamId: 1, priority: { streamDependency: 0, weight: 256, exclusive: false } });
    if (prio.type === FrameType.PRIORITY) expect(prio.priority.weight).toBe(256);
  });

  it("SETTINGS round-trips values incl. boolean", () => {
    const f = roundTrip({
      type: FrameType.SETTINGS,
      streamId: 0,
      ack: false,
      settings: { headerTableSize: 4096, enablePush: false, maxConcurrentStreams: 100, initialWindowSize: 65535, maxFrameSize: 16384 },
    });
    if (f.type === FrameType.SETTINGS) {
      expect(f.settings).toEqual({ headerTableSize: 4096, enablePush: false, maxConcurrentStreams: 100, initialWindowSize: 65535, maxFrameSize: 16384 });
    }
  });

  it("PUSH_PROMISE round-trips", () => {
    const f = roundTrip({ type: FrameType.PUSH_PROMISE, streamId: 1, promisedStreamId: 2, headerBlockFragment: bytesOf([0x82]), endHeaders: true });
    if (f.type === FrameType.PUSH_PROMISE) {
      expect(f.promisedStreamId).toBe(2);
      expect([...f.headerBlockFragment]).toEqual([0x82]);
    }
  });
});

describe("padding", () => {
  it("strips DATA padding", () => {
    // payload(5) = padLength=2, data=[9,9], pad=[0,0]; type=0(DATA), flags=PADDED(0x8), stream=1
    const bytes = bytesOf([0, 0, 5, 0, 0x08, 0, 0, 0, 1, 2, 9, 9, 0, 0]);
    const frames = new FrameDecoder().push(bytes);
    expect(frames).toHaveLength(1);
    const f = frames[0]!;
    if (f.type === FrameType.DATA) expect([...f.data]).toEqual([9, 9]);
    else throw new Error("expected DATA");
  });
});

describe("streaming decoder", () => {
  it("reassembles frames split at every byte boundary", () => {
    const wire = [
      serializeFrame({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: { initialWindowSize: 65535 } }),
      serializeFrame({ type: FrameType.HEADERS, streamId: 1, headerBlockFragment: bytesOf([0x82, 0x86, 0x84]), endStream: true, endHeaders: true }),
      serializeFrame({ type: FrameType.DATA, streamId: 1, data: bytesOf([1, 2, 3, 4, 5]), endStream: true }),
    ];
    const all = Uint8Array.from(wire.flatMap((f) => [...f]));

    const dec = new FrameDecoder();
    const got: Frame[] = [];
    for (let i = 0; i < all.length; i++) {
      got.push(...dec.push(all.subarray(i, i + 1)));
    }
    expect(got.map((f) => f.type)).toEqual([FrameType.SETTINGS, FrameType.HEADERS, FrameType.DATA]);
  });

  it("skips unknown frame types", () => {
    // type 0x63 unknown, len 1, then a valid PING
    const unknown = bytesOf([0, 0, 1, 0x63, 0, 0, 0, 0, 0, 0xff]);
    const ping = serializeFrame({ type: FrameType.PING, streamId: 0, ack: false, opaqueData: new Uint8Array(8) });
    const frames = new FrameDecoder().push(Uint8Array.from([...unknown, ...ping]));
    expect(frames.map((f) => f.type)).toEqual([FrameType.PING]);
  });
});
