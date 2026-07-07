//! Byte primitives — a growable big-endian writer and a cursor reader — the Rust
//! analogue of the TS `bytes.ts` (`ByteWriter` / `ByteReader`). No external deps.
//!
//! A couple of accessor methods round out the API for parity with the TS types
//! even where the current callers don't need them.
#![allow(dead_code)]

/// UTF-8 decode, lossy (never fails), matching the TS `TextDecoder({fatal:false})`.
pub fn decode_utf8(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Growable big-endian byte writer. `Vec<u8>` grows on demand, so building a
/// frame never allocates per field.
#[derive(Default)]
pub struct ByteWriter {
    buf: Vec<u8>,
}

impl ByteWriter {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u24(&mut self, v: u32) -> &mut Self {
        let b = v.to_be_bytes();
        self.buf.extend_from_slice(&b[1..4]);
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    /// The written bytes (no copy).
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Consume the writer, returning the owned bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// Cursor-based big-endian reader over a byte slice.
///
/// Fixed-width reads (`u8`/`u16`/`u24`/`u32`/`peek`) panic on out-of-bounds; the
/// caller MUST ensure enough bytes remain (frame/HPACK parsers guard with
/// [`remaining`](ByteReader::remaining) and length checks). [`bytes`](ByteReader::bytes)
/// is the fallible slice read used at variable-length boundaries.
pub struct ByteReader<'a> {
    buf: &'a [u8],
    pub cursor: usize,
}

impl<'a> ByteReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, cursor: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.cursor
    }

    pub fn u8(&mut self) -> u8 {
        let v = self.buf[self.cursor];
        self.cursor += 1;
        v
    }

    pub fn u16(&mut self) -> u16 {
        let v = u16::from_be_bytes([self.buf[self.cursor], self.buf[self.cursor + 1]]);
        self.cursor += 2;
        v
    }

    pub fn u24(&mut self) -> u32 {
        let a = self.buf[self.cursor] as u32;
        let b = self.buf[self.cursor + 1] as u32;
        let c = self.buf[self.cursor + 2] as u32;
        self.cursor += 3;
        (a << 16) | (b << 8) | c
    }

    pub fn u32(&mut self) -> u32 {
        let v = u32::from_be_bytes([
            self.buf[self.cursor],
            self.buf[self.cursor + 1],
            self.buf[self.cursor + 2],
            self.buf[self.cursor + 3],
        ]);
        self.cursor += 4;
        v
    }

    /// Read `n` bytes (no copy), or `None` if fewer than `n` remain.
    pub fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.remaining() < n {
            return None;
        }
        let out = &self.buf[self.cursor..self.cursor + n];
        self.cursor += n;
        Some(out)
    }

    /// The current byte without advancing.
    pub fn peek(&self) -> u8 {
        self.buf[self.cursor]
    }
}
