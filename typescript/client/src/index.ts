// h2ts — a tiny HTTP/2 client for the browser, tunneled over a WebSocket (or any
// byte duplex). No Node core dependencies.

export {
  connect,
  connectWebSocket,
  connectPool,
  H2Pool,
  DEFAULT_SUBPROTOCOL,
  type WebSocketConnectOptions,
  type PoolConnection,
  type PoolOptions,
} from "./client.js";
export { webSocketTransport, openWebSocket, type WebSocketLike } from "./transport/websocket.js";
export type { Transport } from "./transport/transport.js";
export { H2Connection } from "./connection.js";
export { H2Error, ERROR_CODES, type ErrorCodeName } from "./errors.js";
export type { Header } from "./hpack/hpack.js";
export type {
  H2RequestInit,
  H2Response,
  BodyInit,
  HeadersInit,
  ConnectOptions,
  ClientSettings,
  PushedRequest,
} from "./types.js";
