import { describe, expect, it } from "vitest";
import { HpackDecoder, HpackEncoder, type Header } from "../src/hpack/hpack.js";
import { huffmanDecode, huffmanEncode } from "../src/hpack/huffman.js";
import { decodeUtf8, encodeUtf8 } from "../src/bytes.js";

function hex(s: string): Uint8Array {
  const clean = s.replace(/\s+/g, "");
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

const pairs = (h: Header[]) => h.map((x) => [x.name, x.value]);

describe("HPACK Huffman (RFC 7541 §5.2 / App. B)", () => {
  it("decodes www.example.com (C.4.1 vector)", () => {
    expect(decodeUtf8(huffmanDecode(hex("f1e3c2e5f23a6ba0ab90f4ff")))).toBe(
      "www.example.com",
    );
  });

  it("decodes no-cache (C.4.2 vector)", () => {
    expect(decodeUtf8(huffmanDecode(hex("a8eb10649cbf")))).toBe("no-cache");
  });

  it("round-trips arbitrary bytes", () => {
    for (const s of ["", "a", "Hello, World!", "/some/path?x=1&y=2", "🎉 unicode"]) {
      const raw = encodeUtf8(s);
      expect(decodeUtf8(huffmanDecode(huffmanEncode(raw)))).toBe(s);
    }
  });

  it("round-trips every byte value 0..255", () => {
    const all = new Uint8Array(256);
    for (let i = 0; i < 256; i++) all[i] = i;
    expect([...huffmanDecode(huffmanEncode(all))]).toEqual([...all]);
  });

  it("rejects an all-ones (EOS) padded overlong stream", () => {
    // 0xff... eventually decodes into EOS territory -> error
    expect(() => huffmanDecode(hex("ffffffff"))).toThrow();
  });
});

describe("HPACK decoder (RFC 7541 Appendix C)", () => {
  it("C.2.1 literal with indexing (custom-key: custom-header)", () => {
    const d = new HpackDecoder();
    const out = d.decode(
      hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572"),
    );
    expect(pairs(out)).toEqual([["custom-key", "custom-header"]]);
  });

  it("C.2.2 literal without indexing (:path /sample/path)", () => {
    const d = new HpackDecoder();
    expect(pairs(d.decode(hex("040c 2f73 616d 706c 652f 7061 7468")))).toEqual([
      [":path", "/sample/path"],
    ]);
  });

  it("C.2.3 literal never indexed (password: secret)", () => {
    const d = new HpackDecoder();
    expect(
      pairs(d.decode(hex("1008 7061 7373 776f 7264 0673 6563 7265 74"))),
    ).toEqual([["password", "secret"]]);
  });

  it("C.2.4 indexed (:method: GET)", () => {
    const d = new HpackDecoder();
    expect(pairs(d.decode(hex("82")))).toEqual([[":method", "GET"]]);
  });

  it("C.3.1 request without Huffman", () => {
    const d = new HpackDecoder();
    const out = d.decode(hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d"));
    expect(pairs(out)).toEqual([
      [":method", "GET"],
      [":scheme", "http"],
      [":path", "/"],
      [":authority", "www.example.com"],
    ]);
  });

  it("C.4.1 request with Huffman", () => {
    const d = new HpackDecoder();
    const out = d.decode(hex("8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff"));
    expect(pairs(out)).toEqual([
      [":method", "GET"],
      [":scheme", "http"],
      [":path", "/"],
      [":authority", "www.example.com"],
    ]);
  });
});

describe("HPACK dynamic table", () => {
  it("indexes an inserted entry across successive blocks", () => {
    const d = new HpackDecoder();
    // Block 1: literal-incremental custom-key/custom-header -> becomes index 62.
    d.decode(hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572"));
    // Block 2: indexed 62 (0xbe) should reproduce it.
    expect(pairs(d.decode(hex("be")))).toEqual([["custom-key", "custom-header"]]);
  });

  it("evicts to honor a dynamic table size update", () => {
    const d = new HpackDecoder();
    d.decode(hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572"));
    d.decode(hex("20")); // size update to 0 -> evict everything
    expect(() => d.decode(hex("be"))).toThrow(); // index 62 no longer valid
  });
});

describe("HPACK encoder <-> decoder round-trip", () => {
  it("round-trips a realistic request header set", () => {
    const enc = new HpackEncoder();
    const dec = new HpackDecoder();
    const headers: Header[] = [
      { name: ":method", value: "GET" },
      { name: ":scheme", value: "http" },
      { name: ":path", value: "/hello/world?q=1" },
      { name: ":authority", value: "127.0.0.1:8090" },
      { name: "user-agent", value: "h2ts/0.1" },
      { name: "accept", value: "*/*" },
      { name: "x-custom-header", value: "some rather long value with spaces" },
      { name: "authorization", value: "Bearer sekrit", neverIndex: true },
    ];
    const decoded = dec.decode(enc.encode(headers));
    expect(pairs(decoded)).toEqual(headers.map((h) => [h.name.toLowerCase(), h.value]));
  });

  it("lowercases header names on encode", () => {
    const enc = new HpackEncoder();
    const dec = new HpackDecoder();
    const decoded = dec.decode(enc.encode([{ name: "X-Mixed-Case", value: "v" }]));
    expect(pairs(decoded)).toEqual([["x-mixed-case", "v"]]);
  });
});
