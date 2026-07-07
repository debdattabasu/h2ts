//! HPACK (RFC 7541) header compression — port of `hpack/hpack.ts`.
//!
//! The decoder is complete (indexed, all literal modes, dynamic table size
//! updates, Huffman) since a server may use any of them. The encoder is
//! deliberately simple and stateless: it indexes exact static-table matches,
//! references static names, and Huffman-encodes strings when shorter, but never
//! inserts into a dynamic table.

// `huffman` is `pub` (but `#[doc(hidden)]`, not part of the stable API) so the
// low-level RFC 7541 Appendix B vectors can be exercised from `tests/hpack.rs`.
#[doc(hidden)]
pub mod huffman;
mod huffman_table;
mod static_table;

use std::collections::HashMap;
use std::sync::OnceLock;

use huffman::{huffman_decode, huffman_encode, huffman_shorter};
use static_table::{STATIC_TABLE, STATIC_TABLE_LENGTH};

use crate::bytes::{decode_utf8, ByteReader, ByteWriter};
use crate::errors::{ErrorCode, H2Error};

const ENTRY_OVERHEAD: usize = 32; // RFC 7541 §4.1
const DEFAULT_TABLE_SIZE: usize = 4096;

/// A decoded/encodable header field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: String,
    pub value: String,
    /// Encode as "never indexed" (`0x10`) — for sensitive values.
    pub never_index: bool,
}

impl Header {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            never_index: false,
        }
    }

    pub fn never_indexed(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            never_index: true,
        }
    }
}

// --- Prefix integer coding (RFC 7541 §5.1) ---

/// Write `value` as a prefix integer; `first_byte_flags` occupies the high bits.
fn write_integer(w: &mut ByteWriter, value: usize, prefix_bits: u32, first_byte_flags: u8) {
    let max = (1usize << prefix_bits) - 1;
    if value < max {
        w.u8(first_byte_flags | value as u8);
        return;
    }
    w.u8(first_byte_flags | max as u8);
    let mut rest = value - max;
    while rest >= 128 {
        w.u8((rest % 128) as u8 + 128);
        rest /= 128;
    }
    w.u8(rest as u8);
}

fn read_integer(r: &mut ByteReader, prefix_bits: u32) -> Result<usize, H2Error> {
    let max = (1usize << prefix_bits) - 1;
    let mut value = (r.u8() & max as u8) as usize;
    if value < max {
        return Ok(value);
    }
    let mut shift = 0u32;
    loop {
        if r.remaining() == 0 {
            return Err(H2Error::new(
                ErrorCode::CompressionError,
                "truncated integer",
            ));
        }
        let byte = r.u8();
        value += ((byte & 0x7f) as usize) << shift;
        shift += 7;
        if shift > 42 {
            return Err(H2Error::new(
                ErrorCode::CompressionError,
                "integer overflow",
            ));
        }
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok(value)
}

// --- String coding (RFC 7541 §5.2) ---

fn write_string(w: &mut ByteWriter, s: &str) {
    let raw = s.as_bytes();
    if huffman_shorter(raw) {
        let encoded = huffman_encode(raw);
        write_integer(w, encoded.len(), 7, 0x80);
        w.bytes(&encoded);
    } else {
        write_integer(w, raw.len(), 7, 0x00);
        w.bytes(raw);
    }
}

fn read_string(r: &mut ByteReader) -> Result<String, H2Error> {
    if r.remaining() == 0 {
        return Err(H2Error::new(
            ErrorCode::CompressionError,
            "truncated string",
        ));
    }
    let huffman = r.peek() & 0x80 != 0;
    let length = read_integer(r, 7)?;
    if length > r.remaining() {
        return Err(H2Error::new(
            ErrorCode::CompressionError,
            "string length exceeds block",
        ));
    }
    let raw = r
        .bytes(length)
        .ok_or_else(|| H2Error::new(ErrorCode::CompressionError, "truncated string"))?;
    let decoded = if huffman {
        huffman_decode(raw)?
    } else {
        raw.to_vec()
    };
    Ok(decode_utf8(&decoded))
}

// --- Static table lookup maps (built once) ---

struct StaticMaps {
    /// header name -> 1-based index (first occurrence).
    name_to_index: HashMap<String, usize>,
    /// "name\0value" -> 1-based index.
    pair_to_index: HashMap<String, usize>,
}

fn pair_key(name: &str, value: &str) -> String {
    format!("{name}\u{0}{value}")
}

fn static_maps() -> &'static StaticMaps {
    static MAPS: OnceLock<StaticMaps> = OnceLock::new();
    MAPS.get_or_init(|| {
        let mut name_to_index = HashMap::new();
        let mut pair_to_index = HashMap::new();
        for (i, &(name, value)) in STATIC_TABLE.iter().enumerate() {
            let index = i + 1;
            name_to_index.entry(name.to_string()).or_insert(index);
            pair_to_index.insert(pair_key(name, value), index);
        }
        StaticMaps {
            name_to_index,
            pair_to_index,
        }
    })
}

/// HPACK decoder with a dynamic table. One instance per connection (inbound
/// compression context).
pub struct HpackDecoder {
    dynamic: Vec<(String, String)>, // newest first
    size: usize,
    max_size: usize,
    protocol_max: usize,
}

impl Default for HpackDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_TABLE_SIZE)
    }
}

impl HpackDecoder {
    pub fn new(max_size: usize) -> Self {
        Self {
            dynamic: Vec::new(),
            size: 0,
            max_size,
            protocol_max: max_size,
        }
    }

    /// The largest table size we've told the peer we can handle (our SETTINGS).
    pub fn set_protocol_max_size(&mut self, n: usize) {
        self.protocol_max = n;
        if self.max_size > n {
            // n <= protocol_max here, so this never errors.
            let _ = self.apply_max_size(n);
        }
    }

    fn entry_at(&self, index: usize) -> Result<(String, String), H2Error> {
        if (1..=STATIC_TABLE_LENGTH).contains(&index) {
            let (n, v) = STATIC_TABLE[index - 1];
            return Ok((n.to_string(), v.to_string()));
        }
        let di = index - STATIC_TABLE_LENGTH - 1;
        self.dynamic.get(di).cloned().ok_or_else(|| {
            H2Error::new(
                ErrorCode::CompressionError,
                format!("invalid HPACK index {index}"),
            )
        })
    }

    fn insert(&mut self, name: String, value: String) {
        let entry_size = name.len() + value.len() + ENTRY_OVERHEAD;
        while self.size + entry_size > self.max_size && !self.dynamic.is_empty() {
            let removed = self.dynamic.pop().unwrap();
            self.size -= removed.0.len() + removed.1.len() + ENTRY_OVERHEAD;
        }
        if entry_size <= self.max_size {
            self.dynamic.insert(0, (name, value));
            self.size += entry_size;
        } else {
            // Entry larger than the whole table: the table ends up empty (§4.4).
            self.dynamic.clear();
            self.size = 0;
        }
    }

    fn apply_max_size(&mut self, new_size: usize) -> Result<(), H2Error> {
        if new_size > self.protocol_max {
            return Err(H2Error::new(
                ErrorCode::CompressionError,
                "dynamic table size update too large",
            ));
        }
        self.max_size = new_size;
        while self.size > self.max_size && !self.dynamic.is_empty() {
            let removed = self.dynamic.pop().unwrap();
            self.size -= removed.0.len() + removed.1.len() + ENTRY_OVERHEAD;
        }
        Ok(())
    }

    /// Decode a complete header block fragment into a list of headers.
    pub fn decode(&mut self, block: &[u8]) -> Result<Vec<Header>, H2Error> {
        let mut r = ByteReader::new(block);
        let mut headers = Vec::new();

        while r.remaining() > 0 {
            let first = r.peek();
            if first & 0x80 != 0 {
                // Indexed header field.
                let index = read_integer(&mut r, 7)?;
                if index == 0 {
                    return Err(H2Error::new(
                        ErrorCode::CompressionError,
                        "indexed field with index 0",
                    ));
                }
                let (name, value) = self.entry_at(index)?;
                headers.push(Header {
                    name,
                    value,
                    never_index: false,
                });
            } else if first & 0x40 != 0 {
                // Literal with incremental indexing.
                let name_index = read_integer(&mut r, 6)?;
                let name = if name_index == 0 {
                    read_string(&mut r)?
                } else {
                    self.entry_at(name_index)?.0
                };
                let value = read_string(&mut r)?;
                self.insert(name.clone(), value.clone());
                headers.push(Header {
                    name,
                    value,
                    never_index: false,
                });
            } else if first & 0x20 != 0 {
                // Dynamic table size update.
                let new_size = read_integer(&mut r, 5)?;
                self.apply_max_size(new_size)?;
            } else {
                // Literal without indexing (0x00) or never indexed (0x10); prefix 4.
                let name_index = read_integer(&mut r, 4)?;
                let name = if name_index == 0 {
                    read_string(&mut r)?
                } else {
                    self.entry_at(name_index)?.0
                };
                let value = read_string(&mut r)?;
                headers.push(Header {
                    name,
                    value,
                    never_index: false,
                });
            }
        }
        Ok(headers)
    }
}

/// HPACK encoder (outbound compression context). Stateless: no dynamic table.
#[derive(Default)]
pub struct HpackEncoder;

impl HpackEncoder {
    pub fn new() -> Self {
        Self
    }

    pub fn encode(&self, headers: &[Header]) -> Vec<u8> {
        let maps = static_maps();
        let mut w = ByteWriter::with_capacity(256);
        for header in headers {
            let name = header.name.to_ascii_lowercase();
            let value = &header.value;

            if let Some(&index) = maps.pair_to_index.get(&pair_key(&name, value)) {
                // Fully indexed static entry.
                write_integer(&mut w, index, 7, 0x80);
                continue;
            }

            // Literal, no dynamic indexing. Never-indexed (0x10) for sensitive
            // headers, otherwise without-indexing (0x00). Both use a 4-bit prefix.
            let flags = if header.never_index { 0x10 } else { 0x00 };
            if let Some(&name_index) = maps.name_to_index.get(&name) {
                write_integer(&mut w, name_index, 4, flags);
            } else {
                write_integer(&mut w, 0, 4, flags);
                write_string(&mut w, &name);
            }
            write_string(&mut w, value);
        }
        w.into_vec()
    }
}
