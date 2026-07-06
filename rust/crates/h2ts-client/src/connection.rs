//! The HTTP/2 connection: owns the [`Transport`](crate::Transport), drives the
//! read/write loops, multiplexes streams, and implements the request/response
//! flow (RFC 7540 §5–6). Opens with the connection preface + SETTINGS and issues
//! the first request immediately (prior knowledge — no `Upgrade` round-trip).
//!
//! TODO (port): from `typescript/client/src/connection.ts` (+ `stream.ts`).
