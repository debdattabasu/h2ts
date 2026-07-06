// Byte primitives over Uint8Array / DataView. No Node Buffer, no Node streams.
// These are the only binary I/O helpers the library uses.

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: false });

export function encodeUtf8(s: string): Uint8Array {
  return textEncoder.encode(s);
}

export function decodeUtf8(b: Uint8Array): string {
  return textDecoder.decode(b);
}

/** Concatenate byte chunks into a single Uint8Array. */
export function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

/**
 * Growable big-endian byte writer. Backed by a single Uint8Array that doubles
 * on demand, so building a frame never allocates per-field.
 */
export class ByteWriter {
  private buf: Uint8Array;
  private view: DataView;
  private len = 0;

  constructor(initialCapacity = 64) {
    this.buf = new Uint8Array(initialCapacity);
    this.view = new DataView(this.buf.buffer);
  }

  get length(): number {
    return this.len;
  }

  private ensure(extra: number): void {
    const need = this.len + extra;
    if (need <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < need) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
    this.view = new DataView(this.buf.buffer);
  }

  u8(v: number): this {
    this.ensure(1);
    this.view.setUint8(this.len, v & 0xff);
    this.len += 1;
    return this;
  }

  u16(v: number): this {
    this.ensure(2);
    this.view.setUint16(this.len, v & 0xffff, false);
    this.len += 2;
    return this;
  }

  u24(v: number): this {
    this.ensure(3);
    this.view.setUint8(this.len, (v >>> 16) & 0xff);
    this.view.setUint8(this.len + 1, (v >>> 8) & 0xff);
    this.view.setUint8(this.len + 2, v & 0xff);
    this.len += 3;
    return this;
  }

  u32(v: number): this {
    this.ensure(4);
    this.view.setUint32(this.len, v >>> 0, false);
    this.len += 4;
    return this;
  }

  bytes(b: Uint8Array): this {
    this.ensure(b.length);
    this.buf.set(b, this.len);
    this.len += b.length;
    return this;
  }

  /** Return a view of the written bytes (no copy). Valid until the next write. */
  take(): Uint8Array {
    return this.buf.subarray(0, this.len);
  }
}

/** Cursor-based big-endian reader over a Uint8Array. */
export class ByteReader {
  readonly buf: Uint8Array;
  private readonly view: DataView;
  cursor: number;

  constructor(buf: Uint8Array) {
    this.buf = buf;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    this.cursor = 0;
  }

  get remaining(): number {
    return this.buf.length - this.cursor;
  }

  u8(): number {
    const v = this.view.getUint8(this.cursor);
    this.cursor += 1;
    return v;
  }

  u16(): number {
    const v = this.view.getUint16(this.cursor, false);
    this.cursor += 2;
    return v;
  }

  u24(): number {
    const a = this.view.getUint8(this.cursor);
    const b = this.view.getUint8(this.cursor + 1);
    const c = this.view.getUint8(this.cursor + 2);
    this.cursor += 3;
    return (a << 16) | (b << 8) | c;
  }

  u32(): number {
    const v = this.view.getUint32(this.cursor, false);
    this.cursor += 4;
    return v >>> 0;
  }

  /** Read `n` bytes as a subarray view (no copy). */
  bytes(n: number): Uint8Array {
    const out = this.buf.subarray(this.cursor, this.cursor + n);
    this.cursor += n;
    return out;
  }

  /** Peek the current byte without advancing. */
  peek(): number {
    return this.view.getUint8(this.cursor);
  }
}
