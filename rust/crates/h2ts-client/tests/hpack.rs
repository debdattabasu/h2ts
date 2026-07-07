//! HPACK tests (RFC 7541) — Huffman (App. B), the Appendix C decoder vectors,
//! dynamic-table behaviour, and an encoder↔decoder round-trip. Exercises the
//! public `hpack` API (Huffman primitives are `#[doc(hidden)] pub` for testing).

use h2ts_client::hpack::huffman::{huffman_decode, huffman_encode};
use h2ts_client::hpack::{Header, HpackDecoder, HpackEncoder};

/// Lossy UTF-8 decode (the client's `bytes::decode_utf8`, inlined for the test).
fn decode_utf8(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn hex(s: &str) -> Vec<u8> {
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    (0..clean.len() / 2)
        .map(|i| u8::from_str_radix(&clean[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

fn pairs(h: &[Header]) -> Vec<(String, String)> {
    h.iter()
        .map(|x| (x.name.clone(), x.value.clone()))
        .collect()
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
    assert_eq!(
        decode_utf8(&huffman_decode(&hex("a8eb10649cbf")).unwrap()),
        "no-cache"
    );
}

#[test]
fn huffman_round_trips_arbitrary_strings() {
    for s in ["", "a", "Hello, World!", "/some/path?x=1&y=2", "🎉 unicode"] {
        let raw = s.as_bytes();
        assert_eq!(
            decode_utf8(&huffman_decode(&huffman_encode(raw)).unwrap()),
            s
        );
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
    let out = d
        .decode(&hex(
            "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572",
        ))
        .unwrap();
    assert_eq!(
        pairs(&out),
        vec![("custom-key".into(), "custom-header".into())]
    );
}

#[test]
fn c_2_2_literal_without_indexing() {
    let mut d = HpackDecoder::default();
    let out = d
        .decode(&hex("040c 2f73 616d 706c 652f 7061 7468"))
        .unwrap();
    assert_eq!(pairs(&out), vec![(":path".into(), "/sample/path".into())]);
}

#[test]
fn c_2_3_literal_never_indexed() {
    let mut d = HpackDecoder::default();
    let out = d
        .decode(&hex("1008 7061 7373 776f 7264 0673 6563 7265 74"))
        .unwrap();
    assert_eq!(pairs(&out), vec![("password".into(), "secret".into())]);
}

#[test]
fn c_2_4_indexed() {
    let mut d = HpackDecoder::default();
    assert_eq!(
        pairs(&d.decode(&hex("82")).unwrap()),
        vec![(":method".into(), "GET".into())]
    );
}

#[test]
fn c_3_1_request_without_huffman() {
    let mut d = HpackDecoder::default();
    let out = d
        .decode(&hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d"))
        .unwrap();
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
    let out = d
        .decode(&hex("8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff"))
        .unwrap();
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
    d.decode(&hex(
        "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572",
    ))
    .unwrap();
    // Block 2: indexed 62 (0xbe) reproduces it.
    assert_eq!(
        pairs(&d.decode(&hex("be")).unwrap()),
        vec![("custom-key".into(), "custom-header".into())]
    );
}

#[test]
fn evicts_to_honor_a_dynamic_table_size_update() {
    let mut d = HpackDecoder::default();
    d.decode(&hex(
        "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572",
    ))
    .unwrap();
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
    let expected: Vec<(String, String)> = headers
        .iter()
        .map(|h| (h.name.to_ascii_lowercase(), h.value.clone()))
        .collect();
    assert_eq!(pairs(&decoded), expected);
}

#[test]
fn lowercases_header_names_on_encode() {
    let enc = HpackEncoder::new();
    let mut dec = HpackDecoder::default();
    let decoded = dec
        .decode(&enc.encode(&[Header::new("X-Mixed-Case", "v")]))
        .unwrap();
    assert_eq!(pairs(&decoded), vec![("x-mixed-case".into(), "v".into())]);
}
