// HTTP/2 frame model (RFC 7540 §6). Frames are a discriminated union keyed by
// `type`. Flags are represented as booleans on each variant.

export const FRAME_HEADER_SIZE = 9;
export const DEFAULT_MAX_FRAME_SIZE = 16384;

export const FrameType = {
  DATA: 0x0,
  HEADERS: 0x1,
  PRIORITY: 0x2,
  RST_STREAM: 0x3,
  SETTINGS: 0x4,
  PUSH_PROMISE: 0x5,
  PING: 0x6,
  GOAWAY: 0x7,
  WINDOW_UPDATE: 0x8,
  CONTINUATION: 0x9,
} as const;

// Flag bits per frame type.
export const Flags = {
  DATA_END_STREAM: 0x1,
  DATA_PADDED: 0x8,
  HEADERS_END_STREAM: 0x1,
  HEADERS_END_HEADERS: 0x4,
  HEADERS_PADDED: 0x8,
  HEADERS_PRIORITY: 0x20,
  SETTINGS_ACK: 0x1,
  PING_ACK: 0x1,
  PUSH_PROMISE_END_HEADERS: 0x4,
  PUSH_PROMISE_PADDED: 0x8,
  CONTINUATION_END_HEADERS: 0x4,
} as const;

/** SETTINGS parameters (RFC 7540 §6.5.2). */
export interface Settings {
  headerTableSize?: number; // 0x1
  enablePush?: boolean; // 0x2
  maxConcurrentStreams?: number; // 0x3
  initialWindowSize?: number; // 0x4
  maxFrameSize?: number; // 0x5
  maxHeaderListSize?: number; // 0x6
}

export const SETTINGS_IDS: Record<keyof Settings, number> = {
  headerTableSize: 0x1,
  enablePush: 0x2,
  maxConcurrentStreams: 0x3,
  initialWindowSize: 0x4,
  maxFrameSize: 0x5,
  maxHeaderListSize: 0x6,
};

export interface Priority {
  streamDependency: number;
  weight: number; // 1..256 (wire value + 1)
  exclusive: boolean;
}

export interface DataFrame {
  type: typeof FrameType.DATA;
  streamId: number;
  data: Uint8Array;
  endStream: boolean;
}

export interface HeadersFrame {
  type: typeof FrameType.HEADERS;
  streamId: number;
  headerBlockFragment: Uint8Array;
  endStream: boolean;
  endHeaders: boolean;
  priority?: Priority;
}

export interface PriorityFrame {
  type: typeof FrameType.PRIORITY;
  streamId: number;
  priority: Priority;
}

export interface RstStreamFrame {
  type: typeof FrameType.RST_STREAM;
  streamId: number;
  errorCode: number;
}

export interface SettingsFrame {
  type: typeof FrameType.SETTINGS;
  streamId: 0;
  ack: boolean;
  settings: Settings;
}

export interface PushPromiseFrame {
  type: typeof FrameType.PUSH_PROMISE;
  streamId: number;
  promisedStreamId: number;
  headerBlockFragment: Uint8Array;
  endHeaders: boolean;
}

export interface PingFrame {
  type: typeof FrameType.PING;
  streamId: 0;
  ack: boolean;
  opaqueData: Uint8Array; // 8 bytes
}

export interface GoawayFrame {
  type: typeof FrameType.GOAWAY;
  streamId: 0;
  lastStreamId: number;
  errorCode: number;
  debugData: Uint8Array;
}

export interface WindowUpdateFrame {
  type: typeof FrameType.WINDOW_UPDATE;
  streamId: number;
  windowSizeIncrement: number;
}

export interface ContinuationFrame {
  type: typeof FrameType.CONTINUATION;
  streamId: number;
  headerBlockFragment: Uint8Array;
  endHeaders: boolean;
}

export type Frame =
  | DataFrame
  | HeadersFrame
  | PriorityFrame
  | RstStreamFrame
  | SettingsFrame
  | PushPromiseFrame
  | PingFrame
  | GoawayFrame
  | WindowUpdateFrame
  | ContinuationFrame;
