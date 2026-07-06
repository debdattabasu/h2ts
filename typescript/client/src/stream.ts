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

  /** Bytes of response body still to be flow-accounted by the connection. */
  readonly body: ReadableStream<Uint8Array>;
  private bodyController!: ReadableStreamDefaultController<Uint8Array>;

  readonly head: Promise<HeadHead>;
  private resolveHead!: (h: HeadHead) => void;
  private rejectHead!: (e: unknown) => void;

  private gotHead = false;
  private bodyClosed = false;
  /** Trailers (a HEADERS block received after DATA). */
  trailers?: Record<string, string>;

  /** For pushed streams: the promised request headers. */
  pushRequest?: HeadHead;

  constructor(id: number, initialSendWindow: number, state: StreamState = "idle") {
    this.id = id;
    this.state = state;
    this.sendWindow = new SendWindow(initialSendWindow);
    this.body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.bodyController = controller;
      },
    });
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
      this.gotHead = true;
      this.resolveHead(collectHeaders(raw));
    } else {
      // Second HEADERS block on an open stream = trailers.
      this.trailers = collectHeaders(raw).headers;
    }
    if (endStream) this.endBody();
  }

  receiveData(data: Uint8Array, endStream: boolean): void {
    if (!this.bodyClosed && data.length > 0) {
      this.bodyController.enqueue(data);
    }
    if (endStream) this.endBody();
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
    if (!this.bodyClosed) {
      this.bodyClosed = true;
      try {
        this.bodyController.error(err);
      } catch {
        // controller may already be closed
      }
    }
  }

  private endBody(): void {
    if (this.bodyClosed) return;
    this.bodyClosed = true;
    try {
      this.bodyController.close();
    } catch {
      // already closed
    }
  }
}
