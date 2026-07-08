// Public entry points for creating a client.
import { H2Connection } from "./connection.js";
import type { Transport } from "./transport/transport.js";
import { openWebSocket, webSocketTransport, type WebSocketLike } from "./transport/websocket.js";
import type { ConnectOptions, H2RequestInit, H2Response } from "./types.js";

/**
 * Create an HTTP/2 client over any byte-duplex {@link Transport}, speaking HTTP/2
 * with **prior knowledge** (RFC 7540 §3.4): it sends the connection preface +
 * SETTINGS immediately and starts issuing requests right away — no HTTP/1.1
 * `Upgrade: h2c` negotiation, and no waiting for the server's preface (no
 * round-trip). `request()` may be called immediately; it only waits for the
 * client's own opening flight to flush, not for any server response.
 */
export function connect(transport: Transport, options?: ConnectOptions): H2Connection {
  return new H2Connection(transport, options);
}

/** The subprotocol h2ts offers by default; the `h2ts-server` crate echoes it. */
export const DEFAULT_SUBPROTOCOL = "h2ts";

export interface WebSocketConnectOptions extends ConnectOptions {
  /**
   * Extra WebSocket subprotocols to offer. {@link DEFAULT_SUBPROTOCOL} (`h2ts`)
   * is always offered; anything here is appended (e.g. `"binary"` for
   * websockify tunnels). The server chooses which to echo; read it back from
   * {@link H2Connection.protocol}.
   */
  protocols?: string | string[];
  /** Inject a WebSocket implementation (defaults to `globalThis.WebSocket`). */
  WebSocket?: new (url: string, protocols?: string | string[]) => WebSocketLike;
}

/** Offer `h2ts` first, then any extra subprotocols the caller appended. */
function offeredProtocols(extra?: string | string[]): string[] {
  const list = extra === undefined ? [] : Array.isArray(extra) ? extra : [extra];
  const appended = list.filter((p) => p.toLowerCase() !== DEFAULT_SUBPROTOCOL);
  return [DEFAULT_SUBPROTOCOL, ...appended];
}

/**
 * Open a WebSocket to `url`, wait for it to connect, and start an HTTP/2 client
 * tunneled over it. The far end of the WebSocket must forward raw bytes to an
 * h2c server (the `h2ts-server` gateway, or e.g. websockify).
 *
 * Offers the `h2ts` subprotocol (plus any in `options.protocols`); the
 * negotiated one is available as {@link H2Connection.protocol}.
 */
export async function connectWebSocket(
  url: string,
  options: WebSocketConnectOptions = {},
): Promise<H2Connection> {
  const ws = await openWebSocket(url, offeredProtocols(options.protocols), options.WebSocket);
  const connection = connect(webSocketTransport(ws), options);
  connection.protocol = ws.protocol ?? "";
  return connection;
}

// --- connection pool (Go-style multi-connection parallelism) ---

/** The subset of {@link H2Connection} a pool routes over. */
export interface PoolConnection {
  readonly isClosed: boolean;
  /** True if the connection is under the peer's SETTINGS_MAX_CONCURRENT_STREAMS. */
  canOpenStream(): boolean;
  request(init: H2RequestInit): Promise<H2Response>;
  close(): void;
}

export interface PoolOptions extends WebSocketConnectOptions {
  /**
   * Maximum number of WebSocket connections the pool may open. Default:
   * unbounded. When reached, further requests park on an existing connection
   * (which queues internally) instead of opening a new one.
   */
  maxConnections?: number;
}

/**
 * A pool of HTTP/2-over-WebSocket connections. Each request is routed to a
 * connection that still has a free stream slot (per the peer's
 * SETTINGS_MAX_CONCURRENT_STREAMS); when all are saturated a new WebSocket is
 * opened — the default `golang.org/x/net/http2` behaviour
 * (`StrictMaxConcurrentStreams = false`). Build one with {@link connectPool}.
 */
export class H2Pool {
  private conns: PoolConnection[] = [];
  private opening: Promise<void> | undefined;

  constructor(
    private readonly factory: () => Promise<PoolConnection>,
    private readonly maxConnections: number = Number.POSITIVE_INFINITY,
  ) {}

  /** Route a request to a free connection, opening a new one if all are full. */
  async request(init: H2RequestInit): Promise<H2Response> {
    for (;;) {
      this.conns = this.conns.filter((c) => !c.isClosed);
      const ready = this.conns.find((c) => c.canOpenStream());
      if (ready) return ready.request(init);
      if (this.conns.length > 0 && this.conns.length >= this.maxConnections) {
        // At the connection cap: park on an existing connection, which queues the
        // request internally until one of its stream slots frees.
        return this.conns[0]!.request(init);
      }
      await this.openOne();
      const fresh = this.conns[this.conns.length - 1];
      if (fresh && !fresh.isClosed && !fresh.canOpenStream()) {
        // A brand-new connection already at its limit (a server advertising a tiny
        // limit): park on it rather than opening an unbounded number of connections.
        return fresh.request(init);
      }
    }
  }

  /** Open one connection at a time; concurrent callers share the in-flight open. */
  private openOne(): Promise<void> {
    if (!this.opening) {
      this.opening = this.factory()
        .then((conn) => {
          this.conns.push(conn);
        })
        .finally(() => {
          this.opening = undefined;
        });
    }
    return this.opening;
  }

  /** The number of live connections currently in the pool. */
  get connections(): number {
    return this.conns.filter((c) => !c.isClosed).length;
  }

  /** Gracefully close every connection in the pool. */
  close(): void {
    for (const conn of this.conns) conn.close();
    this.conns = [];
  }
}

/**
 * Open a pool of HTTP/2 clients to `url`, each tunneled over its own WebSocket.
 * Like {@link connectWebSocket}, but transparently opens additional connections
 * when concurrent demand exceeds a connection's SETTINGS_MAX_CONCURRENT_STREAMS
 * — real HTTP/2 multiplexing first, extra connections only when saturated.
 */
export function connectPool(url: string, options: PoolOptions = {}): H2Pool {
  const { maxConnections, ...connectOptions } = options;
  return new H2Pool(
    () => connectWebSocket(url, connectOptions),
    maxConnections ?? Number.POSITIVE_INFINITY,
  );
}
