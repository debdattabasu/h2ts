// Public entry points for creating a client.
import { H2Connection } from "./connection.js";
import type { Transport } from "./transport/transport.js";
import { openWebSocket, webSocketTransport, type WebSocketLike } from "./transport/websocket.js";
import type { ConnectOptions } from "./types.js";

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
