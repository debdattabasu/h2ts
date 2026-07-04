// The HTTP/2 connection: owns the transport, drives the read/write loops,
// multiplexes streams, and implements the client request/response flow.
// RFC 7540 §3.5 (preface), §5 (streams), §6 (frames), §6.9 (flow control).
import { concatBytes, decodeUtf8, encodeUtf8 } from "./bytes.js";
import { errorCodeName, errorCodeValue, H2Error } from "./errors.js";
import { SendWindow } from "./flow.js";
import { FrameDecoder, serializeFrame } from "./frames/codec.js";
import { DEFAULT_MAX_FRAME_SIZE, FrameType, type Frame, type Settings } from "./frames/types.js";
import { HpackDecoder, HpackEncoder, type Header } from "./hpack/hpack.js";
import { H2Stream, type HeadHead } from "./stream.js";
import type { Transport } from "./transport/transport.js";
import type {
  BodyInit,
  ConnectOptions,
  H2RequestInit,
  H2Response,
  HeadersInit,
  PushedRequest,
} from "./types.js";

const CONNECTION_PREFACE = encodeUtf8("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
const SPEC_INITIAL_WINDOW = 65535;
const EMPTY = new Uint8Array(0);

// Headers that are connection-specific and MUST NOT appear in HTTP/2 (§8.1.2.2).
const FORBIDDEN_HEADERS = new Set([
  "connection",
  "host",
  "keep-alive",
  "proxy-connection",
  "transfer-encoding",
  "upgrade",
]);

interface PendingHeaderBlock {
  streamId: number;
  kind: "response" | "push";
  endStream: boolean;
  promisedStreamId?: number;
  fragments: Uint8Array[];
}

interface RemoteSettings {
  initialWindowSize: number;
  maxFrameSize: number;
  headerTableSize: number;
  enablePush: boolean;
}

export class H2Connection {
  private readonly transport: Transport;
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;
  private readonly encoder = new HpackEncoder();
  private readonly decoder: HpackDecoder;
  private readonly frameDecoder: FrameDecoder;
  private readonly onPush: ((push: PushedRequest) => void) | undefined;

  private readonly streams = new Map<number, H2Stream>();
  private nextStreamId = 1;

  /** Connection-level send window (peer's inbound window for us). */
  private readonly connSendWindow = new SendWindow(SPEC_INITIAL_WINDOW);
  private readonly localMaxFrameSize: number;
  private readonly localInitialWindow: number;
  private readonly remote: RemoteSettings = {
    initialWindowSize: SPEC_INITIAL_WINDOW,
    maxFrameSize: DEFAULT_MAX_FRAME_SIZE,
    headerTableSize: 4096,
    enablePush: true,
  };

  private pendingHeaderBlock: PendingHeaderBlock | undefined;
  private writeQueue: Promise<void> = Promise.resolve();
  private readonly pings = new Map<string, { resolve: (rtt: number) => void; sentAt: number }>();
  private pingCounter = 0;

  private closedFlag = false;
  private closeError: unknown;
  private goawayReceived = false;
  private highestPromised = 0;

  readonly ready: Promise<void>;
  readonly closed: Promise<void>;
  private resolveClosed!: () => void;

  constructor(transport: Transport, options: ConnectOptions = {}) {
    this.transport = transport;
    this.writer = transport.writable.getWriter();
    this.onPush = options.onPush;

    const s = options.settings ?? {};
    this.localMaxFrameSize = s.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
    this.localInitialWindow = s.initialWindowSize ?? 1024 * 1024;
    const headerTableSize = s.headerTableSize ?? 4096;
    const enablePush = s.enablePush ?? true;

    this.decoder = new HpackDecoder(headerTableSize);
    this.frameDecoder = new FrameDecoder(this.localMaxFrameSize);

    this.closed = new Promise<void>((resolve) => {
      this.resolveClosed = resolve;
    });

    // Send the client connection preface followed by our SETTINGS (§3.5).
    void this.writeRaw(CONNECTION_PREFACE);
    const localSettings: Settings = {
      headerTableSize,
      enablePush,
      initialWindowSize: this.localInitialWindow,
      maxFrameSize: this.localMaxFrameSize,
    };
    this.ready = this.send({ type: FrameType.SETTINGS, streamId: 0, ack: false, settings: localSettings });

    void this.readLoop();
  }

  get isClosed(): boolean {
    return this.closedFlag;
  }

  // --- write path (ordered, with optional backpressure) ---

  private writeRaw(bytes: Uint8Array): Promise<void> {
    if (this.closedFlag) return Promise.resolve();
    const p = this.writeQueue.then(() => this.writer.write(bytes));
    this.writeQueue = p.catch(() => {});
    p.catch((err) => this.destroy(err));
    return p;
  }

  private send(frame: Frame): Promise<void> {
    return this.writeRaw(serializeFrame(frame));
  }

  // --- read path ---

  private async readLoop(): Promise<void> {
    const reader = this.transport.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          this.destroy(new H2Error("NO_ERROR", "transport closed by peer"));
          return;
        }
        if (value && value.length > 0) this.onBytes(value);
      }
    } catch (err) {
      this.destroy(err);
    } finally {
      reader.releaseLock?.();
    }
  }

  private onBytes(chunk: Uint8Array): void {
    let frames: Frame[];
    try {
      frames = this.frameDecoder.push(chunk);
    } catch (err) {
      this.connectionError(err);
      return;
    }
    for (const frame of frames) {
      try {
        this.dispatch(frame);
      } catch (err) {
        this.connectionError(err);
        return;
      }
    }
  }

  private dispatch(frame: Frame): void {
    // A pending (un-terminated) header block only allows CONTINUATION on the
    // same stream (§6.2).
    if (this.pendingHeaderBlock && frame.type !== FrameType.CONTINUATION) {
      throw new H2Error("PROTOCOL_ERROR", "expected CONTINUATION frame");
    }

    switch (frame.type) {
      case FrameType.SETTINGS:
        if (frame.ack) return;
        this.applyRemoteSettings(frame.settings);
        void this.send({ type: FrameType.SETTINGS, streamId: 0, ack: true, settings: {} });
        return;

      case FrameType.HEADERS:
        this.pendingHeaderBlock = {
          streamId: frame.streamId,
          kind: "response",
          endStream: frame.endStream,
          fragments: [frame.headerBlockFragment],
        };
        if (frame.endHeaders) this.completeHeaderBlock();
        return;

      case FrameType.CONTINUATION: {
        const pb = this.pendingHeaderBlock;
        if (!pb || pb.streamId !== frame.streamId) {
          throw new H2Error("PROTOCOL_ERROR", "unexpected CONTINUATION");
        }
        pb.fragments.push(frame.headerBlockFragment);
        if (frame.endHeaders) this.completeHeaderBlock();
        return;
      }

      case FrameType.PUSH_PROMISE:
        this.pendingHeaderBlock = {
          streamId: frame.streamId,
          kind: "push",
          endStream: false,
          promisedStreamId: frame.promisedStreamId,
          fragments: [frame.headerBlockFragment],
        };
        if (frame.endHeaders) this.completeHeaderBlock();
        return;

      case FrameType.DATA: {
        const stream = this.streams.get(frame.streamId);
        if (frame.data.length > 0) {
          // Replenish on receipt (simple, constant receive window).
          void this.send({ type: FrameType.WINDOW_UPDATE, streamId: 0, windowSizeIncrement: frame.data.length });
          if (stream && !frame.endStream) {
            void this.send({ type: FrameType.WINDOW_UPDATE, streamId: frame.streamId, windowSizeIncrement: frame.data.length });
          }
        }
        if (stream) {
          stream.receiveData(frame.data, frame.endStream);
          if (frame.endStream) this.retireStream(frame.streamId);
        }
        return;
      }

      case FrameType.RST_STREAM: {
        const stream = this.streams.get(frame.streamId);
        if (stream) {
          stream.receiveReset(frame.errorCode);
          this.streams.delete(frame.streamId);
        }
        return;
      }

      case FrameType.WINDOW_UPDATE: {
        if (frame.windowSizeIncrement === 0) {
          if (frame.streamId === 0) throw new H2Error("PROTOCOL_ERROR", "zero WINDOW_UPDATE");
          this.resetStream(frame.streamId, "PROTOCOL_ERROR");
          return;
        }
        if (frame.streamId === 0) this.connSendWindow.update(frame.windowSizeIncrement);
        else this.streams.get(frame.streamId)?.sendWindow.update(frame.windowSizeIncrement);
        return;
      }

      case FrameType.PING:
        if (frame.ack) {
          const key = hex(frame.opaqueData);
          const waiter = this.pings.get(key);
          if (waiter) {
            this.pings.delete(key);
            waiter.resolve(Date.now() - waiter.sentAt);
          }
        } else {
          void this.send({ type: FrameType.PING, streamId: 0, ack: true, opaqueData: frame.opaqueData });
        }
        return;

      case FrameType.GOAWAY: {
        this.goawayReceived = true;
        const err = new H2Error(errorCodeName(frame.errorCode) ?? "NO_ERROR", "peer sent GOAWAY");
        for (const [id, stream] of this.streams) {
          if (id > frame.lastStreamId) {
            stream.fail(err);
            this.streams.delete(id);
          }
        }
        if (frame.errorCode !== 0) this.destroy(err);
        return;
      }

      case FrameType.PRIORITY:
        return; // prioritization not implemented
    }
  }

  private completeHeaderBlock(): void {
    const pb = this.pendingHeaderBlock!;
    this.pendingHeaderBlock = undefined;
    const block = pb.fragments.length === 1 ? pb.fragments[0]! : concatBytes(pb.fragments);
    const headers = this.decoder.decode(block); // HPACK decode (connection-global)

    if (pb.kind === "response") {
      this.streams.get(pb.streamId)?.receiveHeaders(headers, pb.endStream);
      if (pb.endStream) this.retireStream(pb.streamId);
    } else {
      this.handlePush(pb.streamId, pb.promisedStreamId!, headers);
    }
  }

  private applyRemoteSettings(s: Settings): void {
    if (s.initialWindowSize !== undefined) {
      const delta = s.initialWindowSize - this.remote.initialWindowSize;
      this.remote.initialWindowSize = s.initialWindowSize;
      for (const stream of this.streams.values()) stream.sendWindow.adjust(delta);
    }
    if (s.maxFrameSize !== undefined) this.remote.maxFrameSize = s.maxFrameSize;
    if (s.headerTableSize !== undefined) this.remote.headerTableSize = s.headerTableSize;
    if (s.enablePush !== undefined) this.remote.enablePush = s.enablePush;
  }

  private handlePush(parentId: number, promisedId: number, requestHeaders: Header[]): void {
    if (promisedId > this.highestPromised) this.highestPromised = promisedId;

    if (!this.onPush) {
      void this.send({ type: FrameType.RST_STREAM, streamId: promisedId, errorCode: errorCodeValue("REFUSED_STREAM") });
      return;
    }

    const stream = new H2Stream(promisedId, this.remote.initialWindowSize, "reservedRemote");
    this.streams.set(promisedId, stream);

    const req: HeadHead = collectRequestHead(requestHeaders);
    const push: PushedRequest = {
      method: pseudo(requestHeaders, ":method") ?? "GET",
      path: pseudo(requestHeaders, ":path") ?? "/",
      authority: pseudo(requestHeaders, ":authority") ?? "",
      scheme: pseudo(requestHeaders, ":scheme") ?? "http",
      headers: req.headers,
      response: stream.head.then((head) => this.buildResponse(stream, head)),
      cancel: () => this.resetStream(promisedId, "CANCEL"),
    };
    this.onPush(push);
  }

  // --- public API ---

  async request(init: H2RequestInit): Promise<H2Response> {
    if (this.closedFlag) throw this.closeError ?? new H2Error("INTERNAL_ERROR", "connection closed");
    if (this.goawayReceived) throw new H2Error("REFUSED_STREAM", "connection is going away");
    await this.ready;

    const id = this.nextStreamId;
    this.nextStreamId += 2;
    const stream = new H2Stream(id, this.remote.initialWindowSize, "open");
    this.streams.set(id, stream);

    const headers = buildRequestHeaders(init);
    const hasBody = !bodyIsEmpty(init.body);

    if (init.signal) {
      if (init.signal.aborted) {
        this.resetStream(id, "CANCEL");
        throw abortError(init.signal);
      }
      init.signal.addEventListener("abort", () => {
        if (this.streams.has(id)) {
          this.resetStream(id, "CANCEL");
          stream.fail(abortError(init.signal!));
        }
      }, { once: true });
    }

    this.sendHeaders(id, headers, !hasBody);
    stream.state = hasBody ? "open" : "halfClosedLocal";

    if (hasBody) {
      this.pumpBody(stream, init.body!).catch((err) => {
        if (this.streams.has(id)) this.resetStream(id, "CANCEL");
        stream.fail(err);
      });
    }

    const head = await stream.head;
    return this.buildResponse(stream, head);
  }

  /** Send a PING and resolve with the round-trip time in milliseconds. */
  ping(): Promise<number> {
    const opaque = new Uint8Array(8);
    const n = ++this.pingCounter;
    new DataView(opaque.buffer).setUint32(4, n >>> 0, false);
    const key = hex(opaque);
    return new Promise<number>((resolve, reject) => {
      if (this.closedFlag) {
        reject(this.closeError ?? new H2Error("INTERNAL_ERROR", "connection closed"));
        return;
      }
      this.pings.set(key, { resolve, sentAt: Date.now() });
      void this.send({ type: FrameType.PING, streamId: 0, ack: false, opaqueData: opaque });
    });
  }

  /** Gracefully close: send GOAWAY, then tear down. */
  async close(): Promise<void> {
    if (this.closedFlag) return;
    try {
      await this.send({
        type: FrameType.GOAWAY,
        streamId: 0,
        lastStreamId: this.highestPromised,
        errorCode: 0,
        debugData: EMPTY,
      });
    } catch {
      // ignore
    }
    this.destroy(new H2Error("NO_ERROR", "connection closed by client"));
  }

  // --- request send helpers ---

  private sendHeaders(id: number, headers: Header[], endStream: boolean): void {
    const block = this.encoder.encode(headers);
    const max = this.remote.maxFrameSize;
    if (block.length <= max) {
      void this.send({ type: FrameType.HEADERS, streamId: id, headerBlockFragment: block, endStream, endHeaders: true });
      return;
    }
    // Split oversized header block into HEADERS + CONTINUATION frames.
    void this.send({ type: FrameType.HEADERS, streamId: id, headerBlockFragment: block.subarray(0, max), endStream, endHeaders: false });
    let offset = max;
    while (offset < block.length) {
      const next = Math.min(offset + max, block.length);
      void this.send({ type: FrameType.CONTINUATION, streamId: id, headerBlockFragment: block.subarray(offset, next), endHeaders: next >= block.length });
      offset = next;
    }
  }

  private async pumpBody(stream: H2Stream, body: BodyInit): Promise<void> {
    for await (const chunk of iterateBody(body)) {
      let offset = 0;
      while (offset < chunk.length) {
        if (this.closedFlag || stream.sendWindow.isClosed) return;
        await this.connSendWindow.waitPositive();
        await stream.sendWindow.waitPositive();
        if (this.closedFlag || stream.sendWindow.isClosed) return;
        const grant = Math.min(
          chunk.length - offset,
          this.connSendWindow.value,
          stream.sendWindow.value,
          this.remote.maxFrameSize,
        );
        if (grant <= 0) continue;
        const slice = chunk.subarray(offset, offset + grant);
        this.connSendWindow.consume(grant);
        stream.sendWindow.consume(grant);
        await this.send({ type: FrameType.DATA, streamId: stream.id, data: slice, endStream: false });
        offset += grant;
      }
    }
    await this.send({ type: FrameType.DATA, streamId: stream.id, data: EMPTY, endStream: true });
    stream.state = "halfClosedLocal";
  }

  private buildResponse(stream: H2Stream, head: HeadHead): H2Response {
    let consumed: Promise<Uint8Array> | undefined;
    const consume = (): Promise<Uint8Array> => (consumed ??= drain(stream.body));
    return {
      status: head.status,
      headers: head.headers,
      rawHeaders: head.raw,
      body: stream.body,
      trailers: () => stream.trailers,
      bytes: () => consume(),
      arrayBuffer: async () => {
        const u8 = await consume();
        return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength) as ArrayBuffer;
      },
      text: async () => decodeUtf8(await consume()),
      json: async () => JSON.parse(decodeUtf8(await consume())),
    };
  }

  // --- teardown ---

  private resetStream(id: number, code: Parameters<typeof errorCodeValue>[0]): void {
    const stream = this.streams.get(id);
    void this.send({ type: FrameType.RST_STREAM, streamId: id, errorCode: errorCodeValue(code) });
    if (stream) {
      stream.state = "closed";
      this.streams.delete(id);
    }
  }

  private retireStream(id: number): void {
    const stream = this.streams.get(id);
    if (!stream) return;
    stream.state = "closed";
    this.streams.delete(id);
  }

  /** A connection-level protocol error: GOAWAY then tear down. */
  private connectionError(err: unknown): void {
    const code = err instanceof H2Error ? err.code : "PROTOCOL_ERROR";
    try {
      void this.send({
        type: FrameType.GOAWAY,
        streamId: 0,
        lastStreamId: this.highestPromised,
        errorCode: errorCodeValue(code),
        debugData: EMPTY,
      });
    } catch {
      // ignore
    }
    this.destroy(err);
  }

  private destroy(err: unknown): void {
    if (this.closedFlag) return;
    this.closedFlag = true;
    this.closeError = err;
    this.connSendWindow.close();
    for (const stream of this.streams.values()) stream.fail(err);
    this.streams.clear();
    for (const { resolve } of this.pings.values()) resolve(-1);
    this.pings.clear();
    // Best-effort transport close.
    this.writer.close().catch(() => {
      this.writer.abort?.(err).catch(() => {});
    });
    this.resolveClosed();
  }
}

// --- helpers ---

function hex(b: Uint8Array): string {
  let s = "";
  for (const x of b) s += x.toString(16).padStart(2, "0");
  return s;
}

function pseudo(headers: Header[], name: string): string | undefined {
  for (const h of headers) if (h.name === name) return h.value;
  return undefined;
}

function collectRequestHead(raw: Header[]): HeadHead {
  const headers: Record<string, string> = {};
  for (const { name, value } of raw) {
    if (name.startsWith(":")) continue;
    const existing = headers[name];
    headers[name] = existing === undefined ? value : `${existing}, ${value}`;
  }
  return { status: Number.NaN, headers, raw };
}

function normalizeHeaders(init?: HeadersInit): Array<[string, string]> {
  if (!init) return [];
  if (Array.isArray(init)) return init.map(([k, v]) => [k, v]);
  return Object.entries(init as Record<string, string>);
}

function buildRequestHeaders(init: H2RequestInit): Header[] {
  const method = (init.method ?? "GET").toUpperCase();
  const scheme = init.scheme ?? "http";
  const path = init.path ?? "/";
  const headers: Header[] = [
    { name: ":method", value: method },
    { name: ":scheme", value: scheme },
  ];
  if (init.authority) headers.push({ name: ":authority", value: init.authority });
  headers.push({ name: ":path", value: path });

  for (const [rawName, value] of normalizeHeaders(init.headers)) {
    const name = rawName.toLowerCase();
    if (name.startsWith(":")) continue; // pseudo-headers are set above
    if (FORBIDDEN_HEADERS.has(name)) continue;
    const sensitive = name === "authorization" || name === "cookie";
    headers.push(sensitive ? { name, value, neverIndex: true } : { name, value });
  }
  return headers;
}

function bodyIsEmpty(body: BodyInit): boolean {
  if (body === null || body === undefined) return true;
  if (typeof body === "string") return body.length === 0;
  if (body instanceof Uint8Array) return body.length === 0;
  return false; // ReadableStream: assume non-empty
}

async function* iterateBody(body: BodyInit): AsyncGenerator<Uint8Array> {
  if (body === null || body === undefined) return;
  if (typeof body === "string") {
    yield encodeUtf8(body);
    return;
  }
  if (body instanceof Uint8Array) {
    yield body;
    return;
  }
  // ReadableStream<Uint8Array>
  const reader = body.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      if (value && value.length > 0) yield value;
    }
  } finally {
    reader.releaseLock?.();
  }
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  return concatBytes(chunks);
}

function abortError(signal: AbortSignal): Error {
  const reason = (signal as { reason?: unknown }).reason;
  if (reason instanceof Error) return reason;
  return new H2Error("CANCEL", "request aborted");
}
