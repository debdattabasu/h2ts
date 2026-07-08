// Per-stream receive-side state + response/body assembly (RFC 7540 §5.1, §8.1).
// The send side (request headers/body) is driven by the connection, which owns
// the write path and the connection-level flow window.
import { H2Error, errorCodeName } from "./errors.js";
import { SendWindow } from "./flow.js";
import type { Header } from "./hpack/hpack.js";

export type StreamState =
  | "idle"
  | "reservedRemote"
  | "open"
  | "halfClosedLocal"
  | "halfClosedRemote"
  | "closed";

/** Parsed head of a response (or of a pushed request). */
export interface HeadHead {
  /** `:status` as a number (responses). NaN if absent. */
  status: number;
  /** Regular (non-pseudo) headers, lower-cased names. Repeated names joined per RFC. */
  headers: Record<string, string>;
  /** All pseudo + regular headers, in wire order. */
  raw: Header[];
}

function collectHeaders(raw: Header[]): HeadHead {
  const headers: Record<string, string> = {};
  let status = Number.NaN;
  for (const { name, value } of raw) {
    if (name === ":status") {
      status = Number.parseInt(value, 10);
      continue;
    }
    if (name.startsWith(":")) continue; // other pseudo-headers not surfaced here
    // Header field concatenation (RFC 7540 §8.1.2.5 uses "," for most; cookie uses "; ").
    const existing = headers[name];
    if (existing === undefined) headers[name] = value;
    else headers[name] = `${existing}${name === "cookie" ? "; " : ", "}${value}`;
  }
  return { status, headers, raw };
}

export class H2Stream {
  readonly id: number;
  readonly sendWindow: SendWindow;
  state: StreamState;

  /** Our send side has ended (we sent END_STREAM: bodyless HEADERS or the
   * body pump's terminal DATA). Until then the stream is at most half-closed. */
  localClosed = false;
  /** The peer's send side has ended (we received END_STREAM). Likewise. */
  remoteClosed = false;

  /**
   * Response body as a byte stream. **Backpressured**: DATA is buffered on
   * receipt and the receive-flow window is replenished (`onConsume`) only as the
   * consumer *reads* — so an unread body eventually stalls the sender rather than
   * buffering unbounded (consumption-driven flow control, à la `node:http2`).
   */
  readonly body: ReadableStream<Uint8Array>;

  readonly head: Promise<HeadHead>;
  private resolveHead!: (h: HeadHead) => void;
  private rejectHead!: (e: unknown) => void;

  private gotHead = false;
  /** Trailers (a HEADERS block received after DATA). */
  trailers?: Record<string, string>;

  /** For pushed streams: the promised request headers. */
  pushRequest?: HeadHead;

  // --- receive buffer (pull-based body) ---
  /** Received chunks awaiting the consumer. */
  private readonly recvQueue: Uint8Array[] = [];
  /** Bytes buffered in `recvQueue` — received but not yet returned to the peer's
   * flow window (via consumption or, on cancel, discard). */
  private recvBuffered = 0;
  private recvEnded = false;
  private recvError: unknown;
  /** Resolves a parked `pull` once data / end / error arrives. */
  private recvNotify: (() => void) | undefined;
  /** True once the body has been closed, errored, or cancelled. */
  private bodyDone = false;
  /** Return `n` consumed/abandoned bytes to the receive-flow windows. */
  private readonly onConsume: (n: number) => void;

  constructor(
    id: number,
    initialSendWindow: number,
    state: StreamState = "idle",
    onConsume: (n: number) => void = () => {},
  ) {
    this.id = id;
    this.state = state;
    this.sendWindow = new SendWindow(initialSendWindow);
    this.onConsume = onConsume;
    this.body = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.pullBody(controller),
        cancel: () => this.cancelBody(),
      },
      // highWaterMark 0: never read ahead, so `pull` (and thus replenishment)
      // happens exactly when the consumer asks for the next chunk.
      { highWaterMark: 0 },
    );
    this.head = new Promise<HeadHead>((resolve, reject) => {
      this.resolveHead = resolve;
      this.rejectHead = reject;
    });
    // Avoid unhandled-rejection noise if nobody awaits head before an error.
    this.head.catch(() => {});
  }

  // --- called by the connection on inbound frames ---

  receiveHeaders(raw: Header[], endStream: boolean): void {
    if (!this.gotHead) {
      const head = collectHeaders(raw);
      // An interim 1xx response (100 Continue, 103 Early Hints) is NOT the final
      // response (RFC 7540 §8.1): keep waiting for the real head, and don't let a
      // following HEADERS block be mistaken for trailers.
      if (head.status >= 100 && head.status < 200) return;
      this.gotHead = true;
      this.resolveHead(head);
    } else {
      // Second HEADERS block on an open stream = trailers.
      this.trailers = collectHeaders(raw).headers;
    }
    if (endStream) this.endBody();
  }

  receiveData(data: Uint8Array, endStream: boolean): void {
    if (data.length > 0 && !this.bodyDone) {
      this.recvQueue.push(data);
      this.recvBuffered += data.length;
    }
    if (endStream) this.recvEnded = true;
    this.wakeRecv();
  }

  receiveReset(errorCode: number): void {
    const name = errorCodeName(errorCode) ?? "PROTOCOL_ERROR";
    const err = new H2Error(name, `stream ${this.id} reset by peer`, this.id);
    this.fail(err);
  }

  /** Tear down with an error (connection close, GOAWAY, transport failure). */
  fail(err: unknown): void {
    this.state = "closed";
    this.sendWindow.close();
    if (!this.gotHead) {
      this.gotHead = true;
      this.rejectHead(err);
    }
    // Deliver any already-buffered chunks first, then surface the error, so a
    // reset mid-download doesn't look like a clean EOF.
    if (this.recvError === undefined) this.recvError = err;
    this.wakeRecv();
  }

  private endBody(): void {
    this.recvEnded = true;
    this.wakeRecv();
  }

  private wakeRecv(): void {
    const notify = this.recvNotify;
    this.recvNotify = undefined;
    notify?.();
  }

  /** ReadableStream pull: hand the consumer the next buffered chunk (or the
   * end/error), replenishing the receive window for the bytes it takes. */
  private async pullBody(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      if (this.recvQueue.length > 0) {
        const chunk = this.recvQueue.shift()!;
        this.recvBuffered -= chunk.length;
        controller.enqueue(chunk);
        this.onConsume(chunk.length); // consumption-driven WINDOW_UPDATE
        return;
      }
      if (this.recvError !== undefined) {
        this.bodyDone = true;
        controller.error(this.recvError);
        return;
      }
      if (this.recvEnded) {
        this.bodyDone = true;
        controller.close();
        return;
      }
      await new Promise<void>((resolve) => {
        this.recvNotify = resolve;
      });
    }
  }

  /** The consumer cancelled the body: return the window for the bytes still
   * buffered (so the connection window doesn't leak), then discard them. */
  private cancelBody(): void {
    this.bodyDone = true;
    if (this.recvBuffered > 0) {
      this.onConsume(this.recvBuffered);
      this.recvBuffered = 0;
    }
    this.recvQueue.length = 0;
  }
}
