//! HTTP/2 error codes (RFC 7540 §7) and the error type used across the crate —
//! the Rust analogue of `errors.ts`.

use core::fmt;

/// An HTTP/2 error code (RFC 7540 §7). The discriminant is the wire value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ErrorCode {
    NoError = 0x0,
    ProtocolError = 0x1,
    InternalError = 0x2,
    FlowControlError = 0x3,
    SettingsTimeout = 0x4,
    StreamClosed = 0x5,
    FrameSizeError = 0x6,
    RefusedStream = 0x7,
    Cancel = 0x8,
    CompressionError = 0x9,
    ConnectError = 0xa,
    EnhanceYourCalm = 0xb,
    InadequateSecurity = 0xc,
    Http11Required = 0xd,
}

impl ErrorCode {
    /// The wire value (RFC 7540 §7).
    pub fn value(self) -> u32 {
        self as u32
    }

    /// The code for a wire value, or `None` if unknown.
    pub fn from_value(v: u32) -> Option<Self> {
        use ErrorCode::*;
        Some(match v {
            0x0 => NoError,
            0x1 => ProtocolError,
            0x2 => InternalError,
            0x3 => FlowControlError,
            0x4 => SettingsTimeout,
            0x5 => StreamClosed,
            0x6 => FrameSizeError,
            0x7 => RefusedStream,
            0x8 => Cancel,
            0x9 => CompressionError,
            0xa => ConnectError,
            0xb => EnhanceYourCalm,
            0xc => InadequateSecurity,
            0xd => Http11Required,
            _ => return None,
        })
    }

    /// The RFC name, e.g. `"PROTOCOL_ERROR"`.
    pub fn name(self) -> &'static str {
        use ErrorCode::*;
        match self {
            NoError => "NO_ERROR",
            ProtocolError => "PROTOCOL_ERROR",
            InternalError => "INTERNAL_ERROR",
            FlowControlError => "FLOW_CONTROL_ERROR",
            SettingsTimeout => "SETTINGS_TIMEOUT",
            StreamClosed => "STREAM_CLOSED",
            FrameSizeError => "FRAME_SIZE_ERROR",
            RefusedStream => "REFUSED_STREAM",
            Cancel => "CANCEL",
            CompressionError => "COMPRESSION_ERROR",
            ConnectError => "CONNECT_ERROR",
            EnhanceYourCalm => "ENHANCE_YOUR_CALM",
            InadequateSecurity => "INADEQUATE_SECURITY",
            Http11Required => "HTTP_1_1_REQUIRED",
        }
    }
}

/// An HTTP/2-level error. When `stream_id` is set the error is stream-scoped
/// (RST_STREAM); otherwise it is a connection error (GOAWAY).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct H2Error {
    pub code: ErrorCode,
    pub message: Option<String>,
    pub stream_id: Option<u32>,
}

impl H2Error {
    /// A connection-level error.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
            stream_id: None,
        }
    }

    /// An error carrying only a code (no message).
    pub fn code(code: ErrorCode) -> Self {
        Self {
            code,
            message: None,
            stream_id: None,
        }
    }

    /// A stream-scoped error.
    pub fn stream(code: ErrorCode, message: impl Into<String>, stream_id: u32) -> Self {
        Self {
            code,
            message: Some(message.into()),
            stream_id: Some(stream_id),
        }
    }
}

impl fmt::Display for H2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(m) => write!(f, "{}: {m}", self.code.name()),
            None => f.write_str(self.code.name()),
        }
    }
}

impl std::error::Error for H2Error {}
