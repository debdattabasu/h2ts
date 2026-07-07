//! HPACK Huffman coding (RFC 7541 §5.2 / Appendix B) — port of `hpack/huffman.ts`.
//! The encoder is used for request headers; the decoder must be complete since a
//! server may Huffman-encode any response header.

use std::sync::OnceLock;

use super::huffman_table::{HUFFMAN_CODES, HUFFMAN_EOS};
use crate::errors::{ErrorCode, H2Error};

/// Number of bits the Huffman encoding of `input` would occupy.
fn encoded_bit_length(input: &[u8]) -> usize {
    input.iter().map(|&b| HUFFMAN_CODES[b as usize].1 as usize).sum()
}

/// Huffman-encode bytes, padding the final byte with the EOS prefix (all 1s).
pub fn huffman_encode(input: &[u8]) -> Vec<u8> {
    let total_bits = encoded_bit_length(input);
    let mut out = vec![0u8; total_bits.div_ceil(8)];
    let mut bit_pos = 0usize;

    for &byte in input {
        let (code, nbits) = HUFFMAN_CODES[byte as usize];
        for i in (0..nbits).rev() {
            if (code >> i) & 1 != 0 {
                out[bit_pos >> 3] |= 0x80 >> (bit_pos & 7);
            }
            bit_pos += 1;
        }
    }

    // Pad the remainder of the last byte with 1-bits (MSBs of the EOS code).
    while bit_pos & 7 != 0 {
        out[bit_pos >> 3] |= 0x80 >> (bit_pos & 7);
        bit_pos += 1;
    }
    out
}

/// True if Huffman-encoding `input` produces fewer bytes than the raw form.
pub fn huffman_shorter(input: &[u8]) -> bool {
    encoded_bit_length(input).div_ceil(8) < input.len()
}

// --- Decoder: a binary trie built once from the code table. ---

#[derive(Default)]
struct TrieNode {
    children: [Option<Box<TrieNode>>; 2],
    symbol: Option<usize>,
}

fn build_trie() -> TrieNode {
    let mut root = TrieNode::default();
    for (sym, &(code, nbits)) in HUFFMAN_CODES.iter().enumerate() {
        let mut node = &mut root;
        for i in (0..nbits).rev() {
            let bit = ((code >> i) & 1) as usize;
            node = node.children[bit].get_or_insert_with(|| Box::new(TrieNode::default()));
        }
        node.symbol = Some(sym); // sym == 256 marks EOS
    }
    root
}

fn root() -> &'static TrieNode {
    static ROOT: OnceLock<TrieNode> = OnceLock::new();
    ROOT.get_or_init(build_trie)
}

/// Huffman-decode bytes. Errors (`COMPRESSION_ERROR`) on malformed input.
pub fn huffman_decode(input: &[u8]) -> Result<Vec<u8>, H2Error> {
    let root = root();
    let mut out = Vec::new();
    let mut node = root;
    let mut partial_bits = 0u32;
    let mut partial_all_ones = true;

    for &byte in input {
        for bit in (0..8).rev() {
            let b = ((byte >> bit) & 1) as usize;
            if b == 0 {
                partial_all_ones = false;
            }
            node = match node.children[b].as_deref() {
                Some(n) => n,
                None => return Err(H2Error::new(ErrorCode::CompressionError, "invalid Huffman code")),
            };
            partial_bits += 1;
            if let Some(sym) = node.symbol {
                if sym == HUFFMAN_EOS {
                    return Err(H2Error::new(ErrorCode::CompressionError, "EOS symbol in Huffman stream"));
                }
                out.push(sym as u8);
                node = root;
                partial_bits = 0;
                partial_all_ones = true;
            }
        }
    }

    // Any leftover must be <=7 bits of all-1s (a prefix of the EOS code). §5.2.
    if !core::ptr::eq(node, root) && (partial_bits > 7 || !partial_all_ones) {
        return Err(H2Error::new(ErrorCode::CompressionError, "invalid Huffman padding"));
    }
    Ok(out)
}
