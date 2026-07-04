// Public entry points for creating a client.
import { H2Connection } from "./connection.js";
import type { Transport } from "./transport/transport.js";
import { openWebSocket, webSocketTransport, type WebSocketLike } from "./transport/websocket.js";
import type { ConnectOptions } from "./types.js";

/**
 * Create an HTTP/2 client over any byte-duplex {@link Transport}. The client
 * sends the connection preface + SETTINGS immediately; `request()` may be
 * called right away (it internally waits for the preface to flush).
 */
export function connect(transport: Transport, options?: ConnectOptions): H2Connection {
  return new H2Connection(transport, options);
}

export interface WebSocketConnectOptions extends ConnectOptions {
  /** WebSocket subprotocol(s). Use "binary" for websockify tunnels. */
  protocols?: string | string[];
  /** Inject a WebSocket implementation (defaults to `globalThis.WebSocket`). */
  WebSocket?: new (url: string, protocols?: string | string[]) => WebSocketLike;
}

/**
 * Open a WebSocket to `url`, wait for it to connect, and start an HTTP/2 client
 * tunneled over it. The far end of the WebSocket must forward raw bytes to an
 * h2c server (e.g. via websockify).
 */
export async function connectWebSocket(
  url: string,
  options: WebSocketConnectOptions = {},
): Promise<H2Connection> {
  const ws = await openWebSocket(url, options.protocols, options.WebSocket);
  return connect(webSocketTransport(ws), options);
}
