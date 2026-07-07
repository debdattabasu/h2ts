//! HTTP/2 frame model + codec (RFC 7540 §6) — the Rust port of `frames/types.ts`
//! and `frames/codec.ts`. A [`Frame`] is a tagged enum; [`serialize_frame`] writes
//! the 9-byte header + payload, and [`FrameDecoder`] streams complete frames out
//! of arbitrary byte chunks, skipping unknown frame types (RFC 7540 §4.1).

use crate::bytes::{ByteReader, ByteWriter};
use crate::errors::{ErrorCode, H2Error};

pub const FRAME_HEADER_SIZE: usize = 9;
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16384;

/// Frame type identifiers (RFC 7540 §6).
pub mod frame_type {
    pub const DATA: u8 = 0x0;
    pub const HEADERS: u8 = 0x1;
    pub const PRIORITY: u8 = 0x2;
    pub const RST_STREAM: u8 = 0x3;
    pub const SETTINGS: u8 = 0x4;
    pub const PUSH_PROMISE: u8 = 0x5;
    pub const PING: u8 = 0x6;
    pub const GOAWAY: u8 = 0x7;
    pub const WINDOW_UPDATE: u8 = 0x8;
    pub const CONTINUATION: u8 = 0x9;
}

/// Flag bits per frame type.
mod flags {
    pub const DATA_END_STREAM: u8 = 0x1;
    pub const DATA_PADDED: u8 = 0x8;
    pub const HEADERS_END_STREAM: u8 = 0x1;
    pub const HEADERS_END_HEADERS: u8 = 0x4;
    pub const HEADERS_PADDED: u8 = 0x8;
    pub const HEADERS_PRIORITY: u8 = 0x20;
    pub const SETTINGS_ACK: u8 = 0x1;
    pub const PING_ACK: u8 = 0x1;
    pub const PUSH_PROMISE_END_HEADERS: u8 = 0x4;
    pub const PUSH_PROMISE_PADDED: u8 = 0x8;
    pub const CONTINUATION_END_HEADERS: u8 = 0x4;
}

/// SETTINGS parameters (RFC 7540 §6.5.2). Absent fields are `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settings {
    pub header_table_size: Option<u32>,      // 0x1
    pub enable_push: Option<bool>,           // 0x2
    pub max_concurrent_streams: Option<u32>, // 0x3
    pub initial_window_size: Option<u32>,    // 0x4
    pub max_frame_size: Option<u32>,         // 0x5
    pub max_header_list_size: Option<u32>,   // 0x6
}

/// Stream priority (RFC 7540 §6.3). `weight` is the human value `1..=256`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Priority {
    pub stream_dependency: u32,
    pub weight: u16,
    pub exclusive: bool,
}

/// An HTTP/2 frame. SETTINGS/PING/GOAWAY always carry stream id 0, so it is implicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Data {
        stream_id: u32,
        data: Vec<u8>,
        end_stream: bool,
    },
    Headers {
        stream_id: u32,
        header_block_fragment: Vec<u8>,
        end_stream: bool,
        end_headers: bool,
        priority: Option<Priority>,
    },
    Priority {
        stream_id: u32,
        priority: Priority,
    },
    RstStream {
        stream_id: u32,
        error_code: u32,
    },
    Settings {
        ack: bool,
        settings: Settings,
    },
    PushPromise {
        stream_id: u32,
        promised_stream_id: u32,
        header_block_fragment: Vec<u8>,
        end_headers: bool,
    },
    Ping {
        ack: bool,
        opaque_data: [u8; 8],
    },
    Goaway {
        last_stream_id: u32,
        error_code: u32,
        debug_data: Vec<u8>,
    },
    WindowUpdate {
        stream_id: u32,
        window_size_increment: u32,
    },
    Continuation {
        stream_id: u32,
        header_block_fragment: Vec<u8>,
        end_headers: bool,
    },
}

struct Encoded {
    type_id: u8,
    flags: u8,
    stream_id: u32,
    payload: Vec<u8>,
}

fn encode_body(frame: &Frame) -> Encoded {
    match frame {
        Frame::Data { stream_id, data, end_stream } => Encoded {
            type_id: frame_type::DATA,
            flags: if *end_stream { flags::DATA_END_STREAM } else { 0 },
            stream_id: *stream_id,
            payload: data.clone(),
        },
        Frame::Headers { stream_id, header_block_fragment, end_stream, end_headers, priority } => {
            let mut f = if *end_headers { flags::HEADERS_END_HEADERS } else { 0 };
            if *end_stream {
                f |= flags::HEADERS_END_STREAM;
            }
            let payload = if let Some(p) = priority {
                f |= flags::HEADERS_PRIORITY;
                let mut w = ByteWriter::with_capacity(5 + header_block_fragment.len());
                let dep = p.stream_dependency & 0x7fff_ffff;
                w.u32(if p.exclusive { dep | 0x8000_0000 } else { dep });
                w.u8((p.weight - 1) as u8);
                w.bytes(header_block_fragment);
                w.into_vec()
            } else {
                header_block_fragment.clone()
            };
            Encoded { type_id: frame_type::HEADERS, flags: f, stream_id: *stream_id, payload }
        }
        Frame::Priority { stream_id, priority } => {
            let mut w = ByteWriter::with_capacity(5);
            let dep = priority.stream_dependency & 0x7fff_ffff;
            w.u32(if priority.exclusive { dep | 0x8000_0000 } else { dep });
            w.u8((priority.weight - 1) as u8);
            Encoded { type_id: frame_type::PRIORITY, flags: 0, stream_id: *stream_id, payload: w.into_vec() }
        }
        Frame::RstStream { stream_id, error_code } => {
            let mut w = ByteWriter::with_capacity(4);
            w.u32(*error_code);
            Encoded { type_id: frame_type::RST_STREAM, flags: 0, stream_id: *stream_id, payload: w.into_vec() }
        }
        Frame::Settings { ack, settings } => {
            let mut w = ByteWriter::with_capacity(48);
            if let Some(v) = settings.header_table_size { w.u16(0x1); w.u32(v); }
            if let Some(v) = settings.enable_push { w.u16(0x2); w.u32(u32::from(v)); }
            if let Some(v) = settings.max_concurrent_streams { w.u16(0x3); w.u32(v); }
            if let Some(v) = settings.initial_window_size { w.u16(0x4); w.u32(v); }
            if let Some(v) = settings.max_frame_size { w.u16(0x5); w.u32(v); }
            if let Some(v) = settings.max_header_list_size { w.u16(0x6); w.u32(v); }
            Encoded {
                type_id: frame_type::SETTINGS,
                flags: if *ack { flags::SETTINGS_ACK } else { 0 },
                stream_id: 0,
                payload: w.into_vec(),
            }
        }
        Frame::PushPromise { stream_id, promised_stream_id, header_block_fragment, end_headers } => {
            let mut w = ByteWriter::with_capacity(4 + header_block_fragment.len());
            w.u32(promised_stream_id & 0x7fff_ffff);
            w.bytes(header_block_fragment);
            Encoded {
                type_id: frame_type::PUSH_PROMISE,
                flags: if *end_headers { flags::PUSH_PROMISE_END_HEADERS } else { 0 },
                stream_id: *stream_id,
                payload: w.into_vec(),
            }
        }
        Frame::Ping { ack, opaque_data } => Encoded {
            type_id: frame_type::PING,
            flags: if *ack { flags::PING_ACK } else { 0 },
            stream_id: 0,
            payload: opaque_data.to_vec(),
        },
        Frame::Goaway { last_stream_id, error_code, debug_data } => {
            let mut w = ByteWriter::with_capacity(8 + debug_data.len());
            w.u32(last_stream_id & 0x7fff_ffff);
            w.u32(*error_code);
            w.bytes(debug_data);
            Encoded { type_id: frame_type::GOAWAY, flags: 0, stream_id: 0, payload: w.into_vec() }
        }
        Frame::WindowUpdate { stream_id, window_size_increment } => {
            let mut w = ByteWriter::with_capacity(4);
            w.u32(window_size_increment & 0x7fff_ffff);
            Encoded { type_id: frame_type::WINDOW_UPDATE, flags: 0, stream_id: *stream_id, payload: w.into_vec() }
        }
        Frame::Continuation { stream_id, header_block_fragment, end_headers } => Encoded {
            type_id: frame_type::CONTINUATION,
            flags: if *end_headers { flags::CONTINUATION_END_HEADERS } else { 0 },
            stream_id: *stream_id,
            payload: header_block_fragment.clone(),
        },
    }
}

/// Serialize a frame to bytes (9-byte header + payload).
pub fn serialize_frame(frame: &Frame) -> Vec<u8> {
    let e = encode_body(frame);
    let mut w = ByteWriter::with_capacity(FRAME_HEADER_SIZE + e.payload.len());
    w.u24(e.payload.len() as u32);
    w.u8(e.type_id);
    w.u8(e.flags);
    w.u32(e.stream_id & 0x7fff_ffff);
    w.bytes(&e.payload);
    w.into_vec()
}

fn truncated() -> H2Error {
    H2Error::new(ErrorCode::ProtocolError, "truncated frame payload")
}

fn read_padded<'a>(r: &mut ByteReader<'a>, len: usize, padded: bool) -> Result<&'a [u8], H2Error> {
    if !padded {
        return r.bytes(len).ok_or_else(truncated);
    }
    if len < 1 {
        return Err(H2Error::new(ErrorCode::ProtocolError, "padded frame missing pad length"));
    }
    let pad_length = r.u8() as usize;
    let data_length = (len - 1)
        .checked_sub(pad_length)
        .ok_or_else(|| H2Error::new(ErrorCode::ProtocolError, "pad length exceeds frame payload"))?;
    let data = r.bytes(data_length).ok_or_else(truncated)?;
    r.bytes(pad_length); // discard padding
    Ok(data)
}

/// Parse one frame payload. `Ok(None)` means an unknown frame type to skip.
fn parse_payload(type_id: u8, flag_bits: u8, stream_id: u32, payload: &[u8]) -> Result<Option<Frame>, H2Error> {
    let mut r = ByteReader::new(payload);
    let len = payload.len();

    let frame = match type_id {
        frame_type::DATA => {
            let data = read_padded(&mut r, len, flag_bits & flags::DATA_PADDED != 0)?;
            Frame::Data {
                stream_id,
                data: data.to_vec(),
                end_stream: flag_bits & flags::DATA_END_STREAM != 0,
            }
        }
        frame_type::HEADERS => {
            let padded = flag_bits & flags::HEADERS_PADDED != 0;
            let has_priority = flag_bits & flags::HEADERS_PRIORITY != 0;
            let pad_length = if padded {
                if len < 1 {
                    return Err(H2Error::new(ErrorCode::ProtocolError, "invalid HEADERS padding"));
                }
                r.u8() as usize
            } else {
                0
            };
            let priority = if has_priority {
                if r.remaining() < 5 {
                    return Err(H2Error::new(ErrorCode::ProtocolError, "HEADERS priority truncated"));
                }
                let dep = r.u32();
                let weight = r.u8() as u16 + 1;
                Some(Priority {
                    stream_dependency: dep & 0x7fff_ffff,
                    exclusive: dep & 0x8000_0000 != 0,
                    weight,
                })
            } else {
                None
            };
            let overhead = usize::from(padded) + if has_priority { 5 } else { 0 } + pad_length;
            let frag_length = len
                .checked_sub(overhead)
                .ok_or_else(|| H2Error::new(ErrorCode::ProtocolError, "invalid HEADERS padding"))?;
            let fragment = r.bytes(frag_length).ok_or_else(truncated)?;
            Frame::Headers {
                stream_id,
                header_block_fragment: fragment.to_vec(),
                end_stream: flag_bits & flags::HEADERS_END_STREAM != 0,
                end_headers: flag_bits & flags::HEADERS_END_HEADERS != 0,
                priority,
            }
        }
        frame_type::PRIORITY => {
            if len != 5 {
                return Err(H2Error::stream(ErrorCode::FrameSizeError, "PRIORITY must be 5 bytes", stream_id));
            }
            let dep = r.u32();
            let weight = r.u8() as u16 + 1;
            Frame::Priority {
                stream_id,
                priority: Priority {
                    stream_dependency: dep & 0x7fff_ffff,
                    exclusive: dep & 0x8000_0000 != 0,
                    weight,
                },
            }
        }
        frame_type::RST_STREAM => {
            if len != 4 {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "RST_STREAM must be 4 bytes"));
            }
            Frame::RstStream { stream_id, error_code: r.u32() }
        }
        frame_type::SETTINGS => {
            let ack = flag_bits & flags::SETTINGS_ACK != 0;
            if ack && len != 0 {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "SETTINGS ACK must be empty"));
            }
            if !len.is_multiple_of(6) {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "SETTINGS length not multiple of 6"));
            }
            let mut settings = Settings::default();
            for _ in 0..len / 6 {
                let id = r.u16();
                let value = r.u32();
                match id {
                    0x1 => settings.header_table_size = Some(value),
                    0x2 => settings.enable_push = Some(value != 0),
                    0x3 => settings.max_concurrent_streams = Some(value),
                    0x4 => settings.initial_window_size = Some(value),
                    0x5 => settings.max_frame_size = Some(value),
                    0x6 => settings.max_header_list_size = Some(value),
                    _ => {} // unknown settings ignored (RFC 7540 §6.5.2)
                }
            }
            Frame::Settings { ack, settings }
        }
        frame_type::PUSH_PROMISE => {
            let padded = flag_bits & flags::PUSH_PROMISE_PADDED != 0;
            let pad_length = if padded {
                if len < 1 {
                    return Err(H2Error::new(ErrorCode::ProtocolError, "invalid PUSH_PROMISE padding"));
                }
                r.u8() as usize
            } else {
                0
            };
            if r.remaining() < 4 {
                return Err(H2Error::new(ErrorCode::ProtocolError, "PUSH_PROMISE truncated"));
            }
            let promised = r.u32() & 0x7fff_ffff;
            let overhead = usize::from(padded) + 4 + pad_length;
            let frag_length = len
                .checked_sub(overhead)
                .ok_or_else(|| H2Error::new(ErrorCode::ProtocolError, "invalid PUSH_PROMISE padding"))?;
            let fragment = r.bytes(frag_length).ok_or_else(truncated)?;
            Frame::PushPromise {
                stream_id,
                promised_stream_id: promised,
                header_block_fragment: fragment.to_vec(),
                end_headers: flag_bits & flags::PUSH_PROMISE_END_HEADERS != 0,
            }
        }
        frame_type::PING => {
            if len != 8 {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "PING must be 8 bytes"));
            }
            let mut opaque = [0u8; 8];
            opaque.copy_from_slice(r.bytes(8).ok_or_else(truncated)?);
            Frame::Ping { ack: flag_bits & flags::PING_ACK != 0, opaque_data: opaque }
        }
        frame_type::GOAWAY => {
            if len < 8 {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "GOAWAY too short"));
            }
            let last_stream_id = r.u32() & 0x7fff_ffff;
            let error_code = r.u32();
            let debug_data = r.bytes(len - 8).ok_or_else(truncated)?.to_vec();
            Frame::Goaway { last_stream_id, error_code, debug_data }
        }
        frame_type::WINDOW_UPDATE => {
            if len != 4 {
                return Err(H2Error::new(ErrorCode::FrameSizeError, "WINDOW_UPDATE must be 4 bytes"));
            }
            Frame::WindowUpdate { stream_id, window_size_increment: r.u32() & 0x7fff_ffff }
        }
        frame_type::CONTINUATION => Frame::Continuation {
            stream_id,
            header_block_fragment: r.bytes(len).ok_or_else(truncated)?.to_vec(),
            end_headers: flag_bits & flags::CONTINUATION_END_HEADERS != 0,
        },
        _ => return Ok(None), // unknown frame type: skip (RFC 7540 §4.1)
    };
    Ok(Some(frame))
}

/// Streaming frame decoder. Feed it arbitrary byte chunks; it returns whatever
/// complete frames are now available, buffering any partial frame internally.
pub struct FrameDecoder {
    pending: Vec<u8>,
    max_frame_size: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_SIZE)
    }
}

impl FrameDecoder {
    pub fn new(max_frame_size: usize) -> Self {
        Self { pending: Vec::new(), max_frame_size }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Frame>, H2Error> {
        let buf: Vec<u8> = if self.pending.is_empty() {
            chunk.to_vec()
        } else {
            let mut v = core::mem::take(&mut self.pending);
            v.extend_from_slice(chunk);
            v
        };

        let mut frames = Vec::new();
        let mut offset = 0;

        while buf.len() - offset >= FRAME_HEADER_SIZE {
            let length = ((buf[offset] as usize) << 16)
                | ((buf[offset + 1] as usize) << 8)
                | (buf[offset + 2] as usize);
            if length > self.max_frame_size {
                return Err(H2Error::new(
                    ErrorCode::FrameSizeError,
                    format!("frame length {length} exceeds max {}", self.max_frame_size),
                ));
            }
            let total = FRAME_HEADER_SIZE + length;
            if buf.len() - offset < total {
                break; // wait for more bytes
            }

            let type_id = buf[offset + 3];
            let flag_bits = buf[offset + 4];
            let stream_id = u32::from_be_bytes([
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
                buf[offset + 8],
            ]) & 0x7fff_ffff;
            let payload = &buf[offset + FRAME_HEADER_SIZE..offset + total];

            if let Some(frame) = parse_payload(type_id, flag_bits, stream_id, payload)? {
                frames.push(frame);
            }
            offset += total;
        }

        self.pending = if offset == buf.len() { Vec::new() } else { buf[offset..].to_vec() };
        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn round_trip(frame: Frame) -> Frame {
        let bytes = serialize_frame(&frame);
        let frames = FrameDecoder::default().push(&bytes).unwrap();
        assert_eq!(frames.len(), 1);
        frames.into_iter().next().unwrap()
    }

    #[test]
    fn settings_ack_is_a_9_byte_empty_frame() {
        let b = serialize_frame(&Frame::Settings { ack: true, settings: Settings::default() });
        assert_eq!(to_hex(&b), "000000040100000000");
    }

    #[test]
    fn window_update_encodes_increment() {
        let b = serialize_frame(&Frame::WindowUpdate { stream_id: 0, window_size_increment: 5 });
        assert_eq!(to_hex(&b), format!("{}{}", "000004080000000000", "00000005"));
    }

    #[test]
    fn settings_encodes_parameters_in_order() {
        let b = serialize_frame(&Frame::Settings {
            ack: false,
            settings: Settings { enable_push: Some(false), initial_window_size: Some(65535), ..Default::default() },
        });
        // header len=12,type=4,flags=0,stream=0 ; then (id=2 -> 0)(id=4 -> 65535)
        assert_eq!(to_hex(&b), "00000c04000000000000020000000000040000ffff");
    }

    #[test]
    fn data_with_end_stream() {
        let f = round_trip(Frame::Data { stream_id: 3, data: vec![1, 2, 3, 4], end_stream: true });
        assert_eq!(f, Frame::Data { stream_id: 3, data: vec![1, 2, 3, 4], end_stream: true });
    }

    #[test]
    fn headers_with_priority_and_flags() {
        let f = round_trip(Frame::Headers {
            stream_id: 5,
            header_block_fragment: vec![0x82, 0x84],
            end_stream: false,
            end_headers: true,
            priority: Some(Priority { stream_dependency: 3, weight: 16, exclusive: true }),
        });
        assert_eq!(
            f,
            Frame::Headers {
                stream_id: 5,
                header_block_fragment: vec![0x82, 0x84],
                end_stream: false,
                end_headers: true,
                priority: Some(Priority { stream_dependency: 3, weight: 16, exclusive: true }),
            }
        );
    }

    #[test]
    fn rst_ping_goaway_window_priority() {
        assert_eq!(
            round_trip(Frame::RstStream { stream_id: 7, error_code: 8 }),
            Frame::RstStream { stream_id: 7, error_code: 8 }
        );
        assert_eq!(
            round_trip(Frame::Ping { ack: true, opaque_data: [1, 2, 3, 4, 5, 6, 7, 8] }),
            Frame::Ping { ack: true, opaque_data: [1, 2, 3, 4, 5, 6, 7, 8] }
        );
        assert_eq!(
            round_trip(Frame::Goaway { last_stream_id: 9, error_code: 1, debug_data: vec![0x61] }),
            Frame::Goaway { last_stream_id: 9, error_code: 1, debug_data: vec![0x61] }
        );
        assert_eq!(
            round_trip(Frame::WindowUpdate { stream_id: 1, window_size_increment: 1000 }),
            Frame::WindowUpdate { stream_id: 1, window_size_increment: 1000 }
        );
        // weight 256 -> wire 255 -> back to 256
        assert_eq!(
            round_trip(Frame::Priority {
                stream_id: 1,
                priority: Priority { stream_dependency: 0, weight: 256, exclusive: false },
            }),
            Frame::Priority {
                stream_id: 1,
                priority: Priority { stream_dependency: 0, weight: 256, exclusive: false },
            }
        );
    }

    #[test]
    fn settings_round_trips_values_incl_boolean() {
        let settings = Settings {
            header_table_size: Some(4096),
            enable_push: Some(false),
            max_concurrent_streams: Some(100),
            initial_window_size: Some(65535),
            max_frame_size: Some(16384),
            max_header_list_size: None,
        };
        assert_eq!(
            round_trip(Frame::Settings { ack: false, settings: settings.clone() }),
            Frame::Settings { ack: false, settings }
        );
    }

    #[test]
    fn push_promise_round_trips() {
        assert_eq!(
            round_trip(Frame::PushPromise {
                stream_id: 1,
                promised_stream_id: 2,
                header_block_fragment: vec![0x82],
                end_headers: true,
            }),
            Frame::PushPromise {
                stream_id: 1,
                promised_stream_id: 2,
                header_block_fragment: vec![0x82],
                end_headers: true,
            }
        );
    }

    #[test]
    fn strips_data_padding() {
        // payload(5) = padLength=2, data=[9,9], pad=[0,0]; type=0(DATA), flags=PADDED(0x8), stream=1
        let bytes = [0, 0, 5, 0, 0x08, 0, 0, 0, 1, 2, 9, 9, 0, 0];
        let frames = FrameDecoder::default().push(&bytes).unwrap();
        assert_eq!(frames, vec![Frame::Data { stream_id: 1, data: vec![9, 9], end_stream: false }]);
    }

    #[test]
    fn reassembles_frames_split_at_every_byte_boundary() {
        let mut wire = Vec::new();
        wire.extend(serialize_frame(&Frame::Settings {
            ack: false,
            settings: Settings { initial_window_size: Some(65535), ..Default::default() },
        }));
        wire.extend(serialize_frame(&Frame::Headers {
            stream_id: 1,
            header_block_fragment: vec![0x82, 0x86, 0x84],
            end_stream: true,
            end_headers: true,
            priority: None,
        }));
        wire.extend(serialize_frame(&Frame::Data { stream_id: 1, data: vec![1, 2, 3, 4, 5], end_stream: true }));

        let mut dec = FrameDecoder::default();
        let mut got = Vec::new();
        for i in 0..wire.len() {
            got.extend(dec.push(&wire[i..i + 1]).unwrap());
        }
        let types: Vec<u8> = got.iter().map(frame_type_of).collect();
        assert_eq!(types, vec![frame_type::SETTINGS, frame_type::HEADERS, frame_type::DATA]);
    }

    #[test]
    fn skips_unknown_frame_types() {
        // type 0x63 unknown, len 1, then a valid PING
        let unknown = [0, 0, 1, 0x63, 0, 0, 0, 0, 0, 0xff];
        let ping = serialize_frame(&Frame::Ping { ack: false, opaque_data: [0; 8] });
        let mut input = unknown.to_vec();
        input.extend(ping);
        let frames = FrameDecoder::default().push(&input).unwrap();
        assert_eq!(frames, vec![Frame::Ping { ack: false, opaque_data: [0; 8] }]);
    }

    fn frame_type_of(f: &Frame) -> u8 {
        match f {
            Frame::Data { .. } => frame_type::DATA,
            Frame::Headers { .. } => frame_type::HEADERS,
            Frame::Priority { .. } => frame_type::PRIORITY,
            Frame::RstStream { .. } => frame_type::RST_STREAM,
            Frame::Settings { .. } => frame_type::SETTINGS,
            Frame::PushPromise { .. } => frame_type::PUSH_PROMISE,
            Frame::Ping { .. } => frame_type::PING,
            Frame::Goaway { .. } => frame_type::GOAWAY,
            Frame::WindowUpdate { .. } => frame_type::WINDOW_UPDATE,
            Frame::Continuation { .. } => frame_type::CONTINUATION,
        }
    }
}
