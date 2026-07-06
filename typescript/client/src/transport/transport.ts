// The byte-duplex a connection runs over. Anything that can be expressed as a
// pair of Web Streams works: a WebSocket (see ./websocket.ts), a TCP socket, a
// pair of in-memory streams for tests, etc.

export interface Transport {
  /** Bytes arriving from the peer. */
  readable: ReadableStream<Uint8Array>;
  /** Bytes to send to the peer. */
  writable: WritableStream<Uint8Array>;
}
