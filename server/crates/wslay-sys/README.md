# wslay-sys

Low-level Rust FFI bindings to [**wslay**](https://github.com/tatsuhiro-t/wslay), Tatsuhiro Tsujikawa's WebSocket C library.

The wslay C sources are **vendored** in this crate and compiled at build time with [`cc`](https://crates.io/crates/cc); the bindings are generated with [`bindgen`](https://crates.io/crates/bindgen). No system wslay installation is required.

This crate exposes wslay's event-based API verbatim — it is **`unsafe` FFI only**. For a safe, async wrapper (and a full-duplex WebSocket⇄byte-stream bridge with true sub-frame streaming), see [`ws-tcp`](https://crates.io/crates/ws-tcp).

## Why wslay

wslay's event API, with `wslay_event_config_set_no_buffering`, delivers frame payloads **incrementally** via `on_frame_recv_chunk_callback` — it never buffers a whole frame, no matter how large. It performs no I/O itself (callback-driven), so it drops cleanly into async runtimes.

## Build requirements

- A C compiler (`cc` finds one automatically).
- `libclang` (required by `bindgen`). On macOS it ships with the Command Line Tools; on Debian/Ubuntu install `libclang-dev`.

## License

This crate is MIT-licensed. The vendored wslay sources are also MIT-licensed — see [`vendor/COPYING`](vendor/COPYING).

Part of [h2ts](https://github.com/debdattabasu/h2ts).
