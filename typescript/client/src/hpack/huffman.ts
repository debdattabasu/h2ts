// HPACK Huffman coding — RFC 7541 §5.2 / Appendix B.
// Encoder is used for request headers; decoder must be complete because a
// server may Huffman-encode any response header.
import { H2Error } from "../errors.js";
import { HUFFMAN_CODES, HUFFMAN_EOS } from "./huffman-table.js";

/** Number of bits the Huffman encoding of `input` would occupy. */
function encodedBitLength(input: Uint8Array): number {
  let bits = 0;
  for (const byte of input) bits += HUFFMAN_CODES[byte]![1];
  return bits;
}

/** Huffman-encode bytes, padding the final byte with the EOS prefix (all 1s). */
export function huffmanEncode(input: Uint8Array): Uint8Array {
  const totalBits = encodedBitLength(input);
  const out = new Uint8Array((totalBits + 7) >>> 3);
  let bitPos = 0;

  for (const byte of input) {
    const entry = HUFFMAN_CODES[byte]!;
    const code = entry[0];
    const nbits = entry[1];
    for (let i = nbits - 1; i >= 0; i--) {
      if ((code >>> i) & 1) out[bitPos >>> 3]! |= 0x80 >>> (bitPos & 7);
      bitPos++;
    }
  }

  // Pad the remainder of the last byte with 1-bits (the MSBs of the EOS code).
  while (bitPos & 7) {
    out[bitPos >>> 3]! |= 0x80 >>> (bitPos & 7);
    bitPos++;
  }
  return out;
}

/** True if encoding `input` with Huffman produces fewer bytes than the raw form. */
export function huffmanShorter(input: Uint8Array): boolean {
  return (encodedBitLength(input) + 7) >>> 3 < input.length;
}

// --- Decoder: a binary trie built once from the code table. ---

interface TrieNode {
  children?: [TrieNode | undefined, TrieNode | undefined];
  symbol?: number;
}

const ROOT: TrieNode = buildTrie();

function buildTrie(): TrieNode {
  const root: TrieNode = {};
  for (let sym = 0; sym < HUFFMAN_CODES.length; sym++) {
    const entry = HUFFMAN_CODES[sym]!;
    const code = entry[0];
    const nbits = entry[1];
    let node = root;
    for (let i = nbits - 1; i >= 0; i--) {
      const bit = (code >>> i) & 1;
      const children = (node.children ??= [undefined, undefined]);
      node = children[bit] ??= {};
    }
    node.symbol = sym; // sym === 256 marks EOS
  }
  return root;
}

/** Huffman-decode bytes. Throws H2Error(COMPRESSION_ERROR) on malformed input. */
export function huffmanDecode(input: Uint8Array): Uint8Array {
  const out: number[] = [];
  let node = ROOT;
  let partialBits = 0;
  let partialAllOnes = true;

  for (const byte of input) {
    for (let bit = 7; bit >= 0; bit--) {
      const b = (byte >>> bit) & 1;
      if (b === 0) partialAllOnes = false;
      const next = node.children?.[b];
      if (next === undefined) {
        throw new H2Error("COMPRESSION_ERROR", "invalid Huffman code");
      }
      node = next;
      partialBits++;
      if (node.symbol !== undefined) {
        if (node.symbol === HUFFMAN_EOS) {
          throw new H2Error("COMPRESSION_ERROR", "EOS symbol in Huffman stream");
        }
        out.push(node.symbol);
        node = ROOT;
        partialBits = 0;
        partialAllOnes = true;
      }
    }
  }

  // Any leftover must be <=7 bits of all-1s (a prefix of the EOS code). RFC 7541 §5.2.
  if (node !== ROOT) {
    if (partialBits > 7 || !partialAllOnes) {
      throw new H2Error("COMPRESSION_ERROR", "invalid Huffman padding");
    }
  }
  return Uint8Array.from(out);
}
