//! HPACK (RFC 7541) header compression — port of `hpack/hpack.ts`.
//!
//! The decoder is complete (indexed, all literal modes, dynamic table size
//! updates, Huffman) since a server may use any of them. The encoder is
//! deliberately simple and stateless: it indexes exact static-table matches,
//! references static names, and Huffman-encodes strings when shorter, but never
//! inserts into a dynamic table.

mod huffman;
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
        Self { name: name.into(), value: value.into(), never_index: false }
    }

    pub fn never_indexed(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { name: name.into(), value: value.into(), never_index: true }
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
            return Err(H2Error::new(ErrorCode::CompressionError, "truncated integer"));
        }
        let byte = r.u8();
        value += ((byte & 0x7f) as usize) << shift;
        shift += 7;
        if shift > 42 {
            return Err(H2Error::new(ErrorCode::CompressionError, "integer overflow"));
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
        return Err(H2Error::new(ErrorCode::CompressionError, "truncated string"));
    }
    let huffman = r.peek() & 0x80 != 0;
    let length = read_integer(r, 7)?;
    if length > r.remaining() {
        return Err(H2Error::new(ErrorCode::CompressionError, "string length exceeds block"));
    }
    let raw = r
        .bytes(length)
        .ok_or_else(|| H2Error::new(ErrorCode::CompressionError, "truncated string"))?;
    let decoded = if huffman { huffman_decode(raw)? } else { raw.to_vec() };
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
        StaticMaps { name_to_index, pair_to_index }
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
        Self { dynamic: Vec::new(), size: 0, max_size, protocol_max: max_size }
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
        self.dynamic
            .get(di)
            .cloned()
            .ok_or_else(|| H2Error::new(ErrorCode::CompressionError, format!("invalid HPACK index {index}")))
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
            return Err(H2Error::new(ErrorCode::CompressionError, "dynamic table size update too large"));
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
                    return Err(H2Error::new(ErrorCode::CompressionError, "indexed field with index 0"));
                }
                let (name, value) = self.entry_at(index)?;
                headers.push(Header { name, value, never_index: false });
            } else if first & 0x40 != 0 {
                // Literal with incremental indexing.
                let name_index = read_integer(&mut r, 6)?;
                let name = if name_index == 0 { read_string(&mut r)? } else { self.entry_at(name_index)?.0 };
                let value = read_string(&mut r)?;
                self.insert(name.clone(), value.clone());
                headers.push(Header { name, value, never_index: false });
            } else if first & 0x20 != 0 {
                // Dynamic table size update.
                let new_size = read_integer(&mut r, 5)?;
                self.apply_max_size(new_size)?;
            } else {
                // Literal without indexing (0x00) or never indexed (0x10); prefix 4.
                let name_index = read_integer(&mut r, 4)?;
                let name = if name_index == 0 { read_string(&mut r)? } else { self.entry_at(name_index)?.0 };
                let value = read_string(&mut r)?;
                headers.push(Header { name, value, never_index: false });
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

#[cfg(test)]
mod tests {
    use super::huffman::{huffman_decode, huffman_encode};
    use super::{Header, HpackDecoder, HpackEncoder};
    use crate::bytes::decode_utf8;

    fn hex(s: &str) -> Vec<u8> {
        let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..clean.len() / 2)
            .map(|i| u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16).unwrap())
            .collect()
    }

    fn pairs(h: &[Header]) -> Vec<(String, String)> {
        h.iter().map(|x| (x.name.clone(), x.value.clone())).collect()
    }

    // --- Huffman (RFC 7541 §5.2 / App. B) ---

    #[test]
    fn huffman_decodes_www_example_com() {
        assert_eq!(
            decode_utf8(&huffman_decode(&hex("f1e3c2e5f23a6ba0ab90f4ff")).unwrap()),
            "www.example.com"
        );
    }

    #[test]
    fn huffman_decodes_no_cache() {
        assert_eq!(decode_utf8(&huffman_decode(&hex("a8eb10649cbf")).unwrap()), "no-cache");
    }

    #[test]
    fn huffman_round_trips_arbitrary_strings() {
        for s in ["", "a", "Hello, World!", "/some/path?x=1&y=2", "🎉 unicode"] {
            let raw = s.as_bytes();
            assert_eq!(decode_utf8(&huffman_decode(&huffman_encode(raw)).unwrap()), s);
        }
    }

    #[test]
    fn huffman_round_trips_every_byte_value() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(huffman_decode(&huffman_encode(&all)).unwrap(), all);
    }

    #[test]
    fn huffman_rejects_all_ones_eos_padded_stream() {
        assert!(huffman_decode(&hex("ffffffff")).is_err());
    }

    // --- Decoder (RFC 7541 Appendix C) ---

    #[test]
    fn c_2_1_literal_with_indexing() {
        let mut d = HpackDecoder::default();
        let out = d.decode(&hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572")).unwrap();
        assert_eq!(pairs(&out), vec![("custom-key".into(), "custom-header".into())]);
    }

    #[test]
    fn c_2_2_literal_without_indexing() {
        let mut d = HpackDecoder::default();
        let out = d.decode(&hex("040c 2f73 616d 706c 652f 7061 7468")).unwrap();
        assert_eq!(pairs(&out), vec![(":path".into(), "/sample/path".into())]);
    }

    #[test]
    fn c_2_3_literal_never_indexed() {
        let mut d = HpackDecoder::default();
        let out = d.decode(&hex("1008 7061 7373 776f 7264 0673 6563 7265 74")).unwrap();
        assert_eq!(pairs(&out), vec![("password".into(), "secret".into())]);
    }

    #[test]
    fn c_2_4_indexed() {
        let mut d = HpackDecoder::default();
        assert_eq!(pairs(&d.decode(&hex("82")).unwrap()), vec![(":method".into(), "GET".into())]);
    }

    #[test]
    fn c_3_1_request_without_huffman() {
        let mut d = HpackDecoder::default();
        let out = d.decode(&hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d")).unwrap();
        assert_eq!(
            pairs(&out),
            vec![
                (":method".into(), "GET".into()),
                (":scheme".into(), "http".into()),
                (":path".into(), "/".into()),
                (":authority".into(), "www.example.com".into()),
            ]
        );
    }

    #[test]
    fn c_4_1_request_with_huffman() {
        let mut d = HpackDecoder::default();
        let out = d.decode(&hex("8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff")).unwrap();
        assert_eq!(
            pairs(&out),
            vec![
                (":method".into(), "GET".into()),
                (":scheme".into(), "http".into()),
                (":path".into(), "/".into()),
                (":authority".into(), "www.example.com".into()),
            ]
        );
    }

    // --- Dynamic table ---

    #[test]
    fn indexes_an_inserted_entry_across_blocks() {
        let mut d = HpackDecoder::default();
        // Block 1: literal-incremental custom-key/custom-header -> becomes index 62.
        d.decode(&hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572")).unwrap();
        // Block 2: indexed 62 (0xbe) reproduces it.
        assert_eq!(pairs(&d.decode(&hex("be")).unwrap()), vec![("custom-key".into(), "custom-header".into())]);
    }

    #[test]
    fn evicts_to_honor_a_dynamic_table_size_update() {
        let mut d = HpackDecoder::default();
        d.decode(&hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572")).unwrap();
        d.decode(&hex("20")).unwrap(); // size update to 0 -> evict everything
        assert!(d.decode(&hex("be")).is_err()); // index 62 no longer valid
    }

    // --- Encoder <-> decoder round-trip ---

    #[test]
    fn round_trips_a_realistic_request_header_set() {
        let enc = HpackEncoder::new();
        let mut dec = HpackDecoder::default();
        let headers = vec![
            Header::new(":method", "GET"),
            Header::new(":scheme", "http"),
            Header::new(":path", "/hello/world?q=1"),
            Header::new(":authority", "127.0.0.1:8090"),
            Header::new("user-agent", "h2ts/0.1"),
            Header::new("accept", "*/*"),
            Header::new("x-custom-header", "some rather long value with spaces"),
            Header::never_indexed("authorization", "Bearer sekrit"),
        ];
        let decoded = dec.decode(&enc.encode(&headers)).unwrap();
        let expected: Vec<(String, String)> =
            headers.iter().map(|h| (h.name.to_ascii_lowercase(), h.value.clone())).collect();
        assert_eq!(pairs(&decoded), expected);
    }

    #[test]
    fn lowercases_header_names_on_encode() {
        let enc = HpackEncoder::new();
        let mut dec = HpackDecoder::default();
        let decoded = dec.decode(&enc.encode(&[Header::new("X-Mixed-Case", "v")])).unwrap();
        assert_eq!(pairs(&decoded), vec![("x-mixed-case".into(), "v".into())]);
    }
}
