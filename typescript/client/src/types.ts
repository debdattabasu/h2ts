// Public request/response types (fetch-like) and client options.
import type { Header } from "./hpack/hpack.js";

export type HeadersInit =
  | Record<string, string>
  | ReadonlyArray<readonly [string, string]>;

export type BodyInit =
  | Uint8Array
  | string
  | ReadableStream<Uint8Array>
  | null
  | undefined;

export interface H2RequestInit {
  /** HTTP method (`:method`). Default "GET". */
  method?: string;
  /** Request target (`:path`). Default "/". */
  path?: string;
  /** Authority (`:authority`), i.e. host[:port]. */
  authority?: string;
  /** Scheme (`:scheme`). Default "http" (h2c tunnel). */
  scheme?: string;
  /** Additional headers. Pseudo-headers are set for you. */
  headers?: HeadersInit;
  /** Request body. */
  body?: BodyInit;
  /** Abort the request. */
  signal?: AbortSignal;
}

export interface H2Response {
  /** `:status`. */
  readonly status: number;
  /** Response headers (lower-cased names; repeated fields joined). */
  readonly headers: Record<string, string>;
  /** All response headers as received, in order (incl. `:status`). */
  readonly rawHeaders: readonly Header[];
  /** Response body as a byte stream. */
  readonly body: ReadableStream<Uint8Array>;
  /** Trailers, available after the body has been fully consumed. */
  trailers(): Record<string, string> | undefined;
  bytes(): Promise<Uint8Array>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json(): Promise<unknown>;
}

export interface PushedRequest {
  /** The promised request the server is pushing a response for. */
  readonly method: string;
  readonly path: string;
  readonly authority: string;
  readonly scheme: string;
  readonly headers: Record<string, string>;
  /** The pushed response. */
  readonly response: Promise<H2Response>;
  /** Refuse the push (RST_STREAM CANCEL). */
  cancel(): void;
}

/** Settings we advertise to the server. */
export interface ClientSettings {
  /** Our HPACK decoder table size (bytes). Default 4096. */
  headerTableSize?: number;
  /** Whether we accept server push. Default true. */
  enablePush?: boolean;
  /** Our per-stream receive window (bytes). Default 1 MiB. */
  initialWindowSize?: number;
  /** Largest frame we accept (bytes). Default 16384. */
  maxFrameSize?: number;
}

export interface ConnectOptions {
  settings?: ClientSettings;
  /**
   * Connection-level receive window in bytes. Default 64 MiB. Grown at startup
   * from the HTTP/2 spec default of 65535 via a `WINDOW_UPDATE(0)`, then
   * replenished as the application consumes response bodies (consumption-driven
   * flow control). Keep it larger than the per-stream {@link ClientSettings.initialWindowSize}
   * so a single unread stream can't stall the whole connection.
   */
  connectionWindowSize?: number;
  /** Called for each server push. If unset, pushes are refused. */
  onPush?: (push: PushedRequest) => void;
}
