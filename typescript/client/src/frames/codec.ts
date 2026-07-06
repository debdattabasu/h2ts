// Frame serialization and a streaming frame decoder (RFC 7540 §6).
import { ByteReader, ByteWriter, concatBytes } from "../bytes.js";
import { H2Error } from "../errors.js";
import {
  DEFAULT_MAX_FRAME_SIZE,
  Flags,
  FRAME_HEADER_SIZE,
  FrameType,
  SETTINGS_IDS,
  type Frame,
  type Settings,
} from "./types.js";

const EMPTY = new Uint8Array(0);

/** Build the payload bytes and header fields for a frame. */
function encodeBody(frame: Frame): {
  typeId: number;
  flags: number;
  streamId: number;
  payload: Uint8Array;
} {
  switch (frame.type) {
    case FrameType.DATA: {
      return {
        typeId: FrameType.DATA,
        flags: frame.endStream ? Flags.DATA_END_STREAM : 0,
        streamId: frame.streamId,
        payload: frame.data,
      };
    }
    case FrameType.HEADERS: {
      let flags = frame.endHeaders ? Flags.HEADERS_END_HEADERS : 0;
      if (frame.endStream) flags |= Flags.HEADERS_END_STREAM;
      let payload = frame.headerBlockFragment;
      if (frame.priority) {
        flags |= Flags.HEADERS_PRIORITY;
        const w = new ByteWriter(5 + payload.length);
        const dep = frame.priority.streamDependency & 0x7fffffff;
        w.u32(frame.priority.exclusive ? dep | 0x80000000 : dep);
        w.u8((frame.priority.weight - 1) & 0xff);
        w.bytes(payload);
        payload = w.take().slice();
      }
      return { typeId: FrameType.HEADERS, flags, streamId: frame.streamId, payload };
    }
    case FrameType.PRIORITY: {
      const w = new ByteWriter(5);
      const dep = frame.priority.streamDependency & 0x7fffffff;
      w.u32(frame.priority.exclusive ? dep | 0x80000000 : dep);
      w.u8((frame.priority.weight - 1) & 0xff);
      return { typeId: FrameType.PRIORITY, flags: 0, streamId: frame.streamId, payload: w.take().slice() };
    }
    case FrameType.RST_STREAM: {
      const w = new ByteWriter(4);
      w.u32(frame.errorCode);
      return { typeId: FrameType.RST_STREAM, flags: 0, streamId: frame.streamId, payload: w.take().slice() };
    }
    case FrameType.SETTINGS: {
      const w = new ByteWriter(48);
      const s = frame.settings;
      for (const key of Object.keys(SETTINGS_IDS) as (keyof Settings)[]) {
        const value = s[key];
        if (value === undefined) continue;
        w.u16(SETTINGS_IDS[key]);
        w.u32(typeof value === "boolean" ? (value ? 1 : 0) : value);
      }
      return {
        typeId: FrameType.SETTINGS,
        flags: frame.ack ? Flags.SETTINGS_ACK : 0,
        streamId: 0,
        payload: w.take().slice(),
      };
    }
    case FrameType.PUSH_PROMISE: {
      const w = new ByteWriter(4 + frame.headerBlockFragment.length);
      w.u32(frame.promisedStreamId & 0x7fffffff);
      w.bytes(frame.headerBlockFragment);
      return {
        typeId: FrameType.PUSH_PROMISE,
        flags: frame.endHeaders ? Flags.PUSH_PROMISE_END_HEADERS : 0,
        streamId: frame.streamId,
        payload: w.take().slice(),
      };
    }
    case FrameType.PING: {
      const opaque = frame.opaqueData.length === 8 ? frame.opaqueData : new Uint8Array(8);
      return {
        typeId: FrameType.PING,
        flags: frame.ack ? Flags.PING_ACK : 0,
        streamId: 0,
        payload: opaque,
      };
    }
    case FrameType.GOAWAY: {
      const w = new ByteWriter(8 + frame.debugData.length);
      w.u32(frame.lastStreamId & 0x7fffffff);
      w.u32(frame.errorCode);
      w.bytes(frame.debugData);
      return { typeId: FrameType.GOAWAY, flags: 0, streamId: 0, payload: w.take().slice() };
    }
    case FrameType.WINDOW_UPDATE: {
      const w = new ByteWriter(4);
      w.u32(frame.windowSizeIncrement & 0x7fffffff);
      return { typeId: FrameType.WINDOW_UPDATE, flags: 0, streamId: frame.streamId, payload: w.take().slice() };
    }
    case FrameType.CONTINUATION: {
      return {
        typeId: FrameType.CONTINUATION,
        flags: frame.endHeaders ? Flags.CONTINUATION_END_HEADERS : 0,
        streamId: frame.streamId,
        payload: frame.headerBlockFragment,
      };
    }
  }
}

/** Serialize a frame to bytes (9-byte header + payload). */
export function serializeFrame(frame: Frame): Uint8Array {
  const { typeId, flags, streamId, payload } = encodeBody(frame);
  const header = new ByteWriter(FRAME_HEADER_SIZE);
  header.u24(payload.length);
  header.u8(typeId);
  header.u8(flags);
  header.u32(streamId & 0x7fffffff);
  return concatBytes([header.take(), payload]);
}

// --- Per-type payload parsing ---

function readPadded(r: ByteReader, length: number, padded: boolean): Uint8Array {
  if (!padded) return r.bytes(length);
  const padLength = r.u8();
  const dataLength = length - 1 - padLength;
  if (dataLength < 0) {
    throw new H2Error("PROTOCOL_ERROR", "pad length exceeds frame payload");
  }
  const data = r.bytes(dataLength);
  r.bytes(padLength); // discard padding
  return data;
}

function parsePayload(
  typeId: number,
  flags: number,
  streamId: number,
  payload: Uint8Array,
): Frame {
  const r = new ByteReader(payload);
  const len = payload.length;

  switch (typeId) {
    case FrameType.DATA: {
      const data = readPadded(r, len, (flags & Flags.DATA_PADDED) !== 0);
      return { type: FrameType.DATA, streamId, data: data.slice(), endStream: (flags & Flags.DATA_END_STREAM) !== 0 };
    }
    case FrameType.HEADERS: {
      const padded = (flags & Flags.HEADERS_PADDED) !== 0;
      const padLength = padded ? r.u8() : 0;
      let priority;
      if (flags & Flags.HEADERS_PRIORITY) {
        const dep = r.u32();
        const weight = r.u8() + 1;
        priority = { streamDependency: dep & 0x7fffffff, exclusive: (dep & 0x80000000) !== 0, weight };
      }
      const fragLength = len - (padded ? 1 : 0) - (flags & Flags.HEADERS_PRIORITY ? 5 : 0) - padLength;
      if (fragLength < 0) throw new H2Error("PROTOCOL_ERROR", "invalid HEADERS padding");
      const fragment = r.bytes(fragLength).slice();
      return {
        type: FrameType.HEADERS,
        streamId,
        headerBlockFragment: fragment,
        endStream: (flags & Flags.HEADERS_END_STREAM) !== 0,
        endHeaders: (flags & Flags.HEADERS_END_HEADERS) !== 0,
        ...(priority ? { priority } : {}),
      };
    }
    case FrameType.PRIORITY: {
      if (len !== 5) throw new H2Error("FRAME_SIZE_ERROR", "PRIORITY must be 5 bytes", streamId);
      const dep = r.u32();
      const weight = r.u8() + 1;
      return {
        type: FrameType.PRIORITY,
        streamId,
        priority: { streamDependency: dep & 0x7fffffff, exclusive: (dep & 0x80000000) !== 0, weight },
      };
    }
    case FrameType.RST_STREAM: {
      if (len !== 4) throw new H2Error("FRAME_SIZE_ERROR", "RST_STREAM must be 4 bytes");
      return { type: FrameType.RST_STREAM, streamId, errorCode: r.u32() };
    }
    case FrameType.SETTINGS: {
      const ack = (flags & Flags.SETTINGS_ACK) !== 0;
      if (ack && len !== 0) throw new H2Error("FRAME_SIZE_ERROR", "SETTINGS ACK must be empty");
      if (len % 6 !== 0) throw new H2Error("FRAME_SIZE_ERROR", "SETTINGS length not multiple of 6");
      const settings: Settings = {};
      for (let i = 0; i < len / 6; i++) {
        const id = r.u16();
        const value = r.u32();
        switch (id) {
          case 0x1: settings.headerTableSize = value; break;
          case 0x2: settings.enablePush = value !== 0; break;
          case 0x3: settings.maxConcurrentStreams = value; break;
          case 0x4: settings.initialWindowSize = value; break;
          case 0x5: settings.maxFrameSize = value; break;
          case 0x6: settings.maxHeaderListSize = value; break;
          // Unknown settings are ignored (RFC 7540 §6.5.2).
        }
      }
      return { type: FrameType.SETTINGS, streamId: 0, ack, settings };
    }
    case FrameType.PUSH_PROMISE: {
      const padded = (flags & Flags.PUSH_PROMISE_PADDED) !== 0;
      const padLength = padded ? r.u8() : 0;
      const promised = r.u32() & 0x7fffffff;
      const fragLength = len - (padded ? 1 : 0) - 4 - padLength;
      if (fragLength < 0) throw new H2Error("PROTOCOL_ERROR", "invalid PUSH_PROMISE padding");
      return {
        type: FrameType.PUSH_PROMISE,
        streamId,
        promisedStreamId: promised,
        headerBlockFragment: r.bytes(fragLength).slice(),
        endHeaders: (flags & Flags.PUSH_PROMISE_END_HEADERS) !== 0,
      };
    }
    case FrameType.PING: {
      if (len !== 8) throw new H2Error("FRAME_SIZE_ERROR", "PING must be 8 bytes");
      return { type: FrameType.PING, streamId: 0, ack: (flags & Flags.PING_ACK) !== 0, opaqueData: r.bytes(8).slice() };
    }
    case FrameType.GOAWAY: {
      if (len < 8) throw new H2Error("FRAME_SIZE_ERROR", "GOAWAY too short");
      const lastStreamId = r.u32() & 0x7fffffff;
      const errorCode = r.u32();
      return { type: FrameType.GOAWAY, streamId: 0, lastStreamId, errorCode, debugData: r.bytes(len - 8).slice() };
    }
    case FrameType.WINDOW_UPDATE: {
      if (len !== 4) throw new H2Error("FRAME_SIZE_ERROR", "WINDOW_UPDATE must be 4 bytes");
      return { type: FrameType.WINDOW_UPDATE, streamId, windowSizeIncrement: r.u32() & 0x7fffffff };
    }
    case FrameType.CONTINUATION: {
      return {
        type: FrameType.CONTINUATION,
        streamId,
        headerBlockFragment: r.bytes(len).slice(),
        endHeaders: (flags & Flags.CONTINUATION_END_HEADERS) !== 0,
      };
    }
    default:
      // Unknown frame type: caller decides. We surface null via the decoder.
      throw new UnknownFrameType(typeId);
  }
}

class UnknownFrameType extends Error {
  constructor(readonly typeId: number) {
    super(`unknown frame type ${typeId}`);
  }
}

/**
 * Streaming frame decoder. Feed it arbitrary byte chunks; it returns whatever
 * complete frames are now available, buffering any partial frame internally.
 * Unknown frame types are silently skipped (RFC 7540 §4.1).
 */
export class FrameDecoder {
  private pending: Uint8Array = EMPTY;
  private readonly maxFrameSize: number;

  constructor(maxFrameSize = DEFAULT_MAX_FRAME_SIZE) {
    this.maxFrameSize = maxFrameSize;
  }

  push(chunk: Uint8Array): Frame[] {
    const buf = this.pending.length === 0 ? chunk : concatBytes([this.pending, chunk]);
    const frames: Frame[] = [];
    let offset = 0;

    while (buf.length - offset >= FRAME_HEADER_SIZE) {
      const length = (buf[offset]! << 16) | (buf[offset + 1]! << 8) | buf[offset + 2]!;
      if (length > this.maxFrameSize) {
        throw new H2Error("FRAME_SIZE_ERROR", `frame length ${length} exceeds max ${this.maxFrameSize}`);
      }
      const total = FRAME_HEADER_SIZE + length;
      if (buf.length - offset < total) break; // wait for more bytes

      const typeId = buf[offset + 3]!;
      const flags = buf[offset + 4]!;
      const streamId =
        ((buf[offset + 5]! << 24) | (buf[offset + 6]! << 16) | (buf[offset + 7]! << 8) | buf[offset + 8]!) &
        0x7fffffff;
      const payload = buf.subarray(offset + FRAME_HEADER_SIZE, offset + total);

      try {
        frames.push(parsePayload(typeId, flags, streamId, payload));
      } catch (err) {
        if (!(err instanceof UnknownFrameType)) throw err;
        // ignore unknown frame types
      }
      offset += total;
    }

    this.pending = offset === buf.length ? EMPTY : buf.subarray(offset).slice();
    return frames;
  }
}
