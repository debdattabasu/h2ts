// HTTP/2 error codes (RFC 7540 §7) and the error type thrown across the library.

export const ERROR_CODES = [
  "NO_ERROR", // 0x0
  "PROTOCOL_ERROR", // 0x1
  "INTERNAL_ERROR", // 0x2
  "FLOW_CONTROL_ERROR", // 0x3
  "SETTINGS_TIMEOUT", // 0x4
  "STREAM_CLOSED", // 0x5
  "FRAME_SIZE_ERROR", // 0x6
  "REFUSED_STREAM", // 0x7
  "CANCEL", // 0x8
  "COMPRESSION_ERROR", // 0x9
  "CONNECT_ERROR", // 0xa
  "ENHANCE_YOUR_CALM", // 0xb
  "INADEQUATE_SECURITY", // 0xc
  "HTTP_1_1_REQUIRED", // 0xd
] as const;

export type ErrorCodeName = (typeof ERROR_CODES)[number];

export function errorCodeName(code: number): ErrorCodeName | undefined {
  return ERROR_CODES[code];
}

export function errorCodeValue(name: ErrorCodeName): number {
  return ERROR_CODES.indexOf(name);
}

/**
 * An HTTP/2-level error. `code` is the RFC 7540 error code name. When
 * `streamId` is set the error is stream-scoped (RST_STREAM); otherwise it is a
 * connection error (GOAWAY).
 */
export class H2Error extends Error {
  readonly code: ErrorCodeName;
  readonly streamId: number | undefined;

  constructor(code: ErrorCodeName, message?: string, streamId?: number) {
    super(message ? `${code}: ${message}` : code);
    this.name = "H2Error";
    this.code = code;
    this.streamId = streamId;
  }
}
