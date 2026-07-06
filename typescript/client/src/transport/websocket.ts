// Adapts a WebSocket to the byte-duplex Transport. Works with the browser's
// native WebSocket and with Node's global WebSocket (undici).
import type { Transport } from "./transport.js";

/** A minimal structural view of the WebSocket API we depend on. */
export interface WebSocketLike {
  binaryType: string;
  readyState: number;
  /** The subprotocol the server selected (empty string if none). */
  readonly protocol?: string;
  send(data: ArrayBufferView | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

function toBytes(data: unknown): Uint8Array | undefined {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  return undefined; // Blob/string: shouldn't happen with binaryType="arraybuffer"
}

/**
 * Build a {@link Transport} over an already-OPEN WebSocket. Sets
 * `binaryType = "arraybuffer"` so messages arrive as binary.
 */
export function webSocketTransport(ws: WebSocketLike): Transport {
  ws.binaryType = "arraybuffer";

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      ws.onmessage = (ev) => {
        const bytes = toBytes(ev.data);
        if (bytes && bytes.length > 0) controller.enqueue(bytes);
      };
      ws.onclose = () => {
        try {
          controller.close();
        } catch {
          // already closed/errored
        }
      };
      ws.onerror = () => {
        try {
          controller.error(new Error("WebSocket error"));
        } catch {
          // already closed
        }
      };
    },
    cancel() {
      ws.close();
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      ws.send(chunk);
    },
    close() {
      ws.close();
    },
    abort() {
      ws.close();
    },
  });

  return { readable, writable };
}

/** Open a WebSocket and resolve once it's ready, or reject on failure. */
export function openWebSocket(
  url: string,
  protocols?: string | string[],
  WebSocketImpl?: new (url: string, protocols?: string | string[]) => WebSocketLike,
): Promise<WebSocketLike> {
  const WS =
    WebSocketImpl ??
    (globalThis as { WebSocket?: new (url: string, protocols?: string | string[]) => WebSocketLike })
      .WebSocket;
  if (!WS) {
    throw new Error("No WebSocket implementation available (pass one explicitly)");
  }
  const ws = new WS(url, protocols);
  ws.binaryType = "arraybuffer";
  return new Promise<WebSocketLike>((resolve, reject) => {
    ws.onopen = () => resolve(ws);
    ws.onerror = () => reject(new Error(`WebSocket connection to ${url} failed`));
  });
}
