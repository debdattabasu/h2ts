//! HPACK (RFC 7541) header compression: a full decoder (indexed, all literal
//! modes, dynamic table, Huffman) and a compact stateless encoder.
//!
//! TODO (port): from `typescript/client/src/hpack/` (`hpack.ts`, `huffman.ts`,
//! `static-table.ts`). Validate the decoder against the RFC 7541 Appendix C
//! vectors, as the TS client does.
