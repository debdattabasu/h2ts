# wslay-sys portability — docs.rs build failure

_Date: 2026-07-30. Scope: `rust/crates/wslay-sys/build.rs` and the vendored wslay C.
Trigger: docs.rs reported **all builds failed** for `h2ts-server` 0.1.3._

**Method:** read the docs.rs build logs for both crates; read the vendored sources'
`HAVE_*` guards; reproduced the failure on `gcc 14.2` in a Linux container; confirmed the
docs.rs build image's package set; re-ran `cargo doc` in that container after the fix.

## TL;DR

The failing crate was **`wslay-sys`**, not `h2ts-server` — the latter only inherited it.
`build.rs` compiled the vendored C without `config.h` *and* without the individual macros
autotools would have written into it, so `htons`/`ntohs`/`ntohl` were only **implicitly
declared**. That is a warning on older toolchains and a hard error on GCC 14+ / Clang 16+.
Fixed by deriving the macros from Cargo's target env. A second, latent big-endian defect
was found in the same guard block and fixed alongside.

## 1. Implicit declaration of `htons`/`ntohs`/`ntohl` (the build failure)

`vendor/wslay_net.h` includes `<arpa/inet.h>` and `<netinet/in.h>` only under
`HAVE_ARPA_INET_H` / `HAVE_NETINET_IN_H`. `build.rs` deliberately ships no `config.h`
(the `HAVE_CONFIG_H` include is left unset — correct), but it also never set those two
macros, so the declarations never arrived. Call sites: `wslay_frame.c:75,235`,
`wslay_event.c:269,642,798,937`, `wslay_net.c:30,31`.

**Why it stayed hidden for two releases:**

- **macOS** declares them regardless — `wslay/wslay.h` → `<sys/types.h>` →
  `<sys/_endian.h>`. Verified directly: a TU including only `<sys/types.h>` compiles a
  call to `htons`. So local dev and any macOS CI are structurally blind to this.
- **Linux, GCC ≤ 13** — a warning, and `.warnings(false)` (i.e. `-w`) suppressed it. This
  is why `.github/workflows/ci.yml` is green: `ubuntu-latest` is still GCC 13.
- **docs.rs** builds on `ubuntu:resolute` (GCC 14+), where `-Wimplicit-function-declaration`
  is an **error by default**. `-w` does not downgrade a default-on error, so the C compile
  died before `bindgen` was ever reached.

On x86_64 the implicit declarations happened to behave (an `int` return truncated back to
`uint16_t`/`uint32_t` is the same value in `eax`), so this was a latent portability bug
rather than a live miscompile — but it was never *correct*.

**Fix.** `build.rs` sets `HAVE_ARPA_INET_H` + `HAVE_NETINET_IN_H`, or `HAVE_WINSOCK2_H`
when `CARGO_CFG_TARGET_OS == "windows"`. (`CARGO_CFG_TARGET_OS`, not `TARGET_FAMILY` —
the latter is comma-separated on some targets and would not compare equal.)

## 2. `WORDS_BIGENDIAN` never set (latent, same root cause)

Found while fixing item 1: the same guard block gates `ntoh64`/`hton64`. Unset,
`wslay_net.h` always takes the byteswap path, and on a big-endian target
`wslay_byteswap64` reduces to `(low32 << 32) | high32` — it swaps the two 32-bit halves
without swapping bytes within them, corrupting 64-bit extended-length frame headers.
The old comment ("little-endian-friendly by default") described the symptom as if it were
a property of wslay; it is really an unset macro. Now set from
`CARGO_CFG_TARGET_ENDIAN == "big"`. Not reachable on the x86_64/aarch64 targets h2ts is
built for, so no release is affected — recorded because it is exactly the kind of edge an
audit should pin rather than leave to chance.

## 3. Fallout check — `bindgen` on docs.rs

Because the C compile failed first, the logs never proved `bindgen` would succeed. The
docs.rs build image's `packages.txt` installs `clang`, `libclang-dev`, `llvm`, `llvm-dev`
and `lld`, and a full `cargo doc` in a Linux container with `clang` + `libclang-dev`
completed clean — so the C error was the only blocker.

## 4. Why both crates needed a publish

`h2ts-server` 0.1.3's published `.crate` ships a `Cargo.lock` pinning
`wslay-sys 0.1.1`, so a docs.rs *rebuild* of 0.1.3 would resolve straight back to the
broken version even after `wslay-sys` 0.1.2 exists. A new `h2ts-server` release is the
reliable route.

## Work log

- [x] **Root cause identified and fixed** — `build.rs` derives `HAVE_ARPA_INET_H` /
  `HAVE_NETINET_IN_H` / `HAVE_WINSOCK2_H` from `CARGO_CFG_TARGET_OS`. Failure reproduced
  on `gcc 14.2` before the change (identical error text, including GCC's
  `did you mean 'hton64'?` suggestion) and clean after.
- [x] **Big-endian `WORDS_BIGENDIAN` fixed** — derived from `CARGO_CFG_TARGET_ENDIAN`.
- [x] **Three rustdoc warnings closed** — the `h2ts-proxy` module doc's usage block was
  prose, so `[listen_addr]`, `[upstream_addr]` and `[keepalive_secs]` parsed as
  intra-doc links, and markdown collapsed the whitespace aligning `defaults:` under the
  usage line. Wrapped in a ```` ```text ```` fence, which fixes the rendering and the
  warnings together.
- [x] **Verified** — `cargo doc -p h2ts-server -p wslay-sys` on Linux + clang: zero
  warnings, zero errors. Rust 129 tests, `clippy --all-targets`, `make conformance`,
  Go `vet` + `test`, TS vitest + typecheck all pass.
- [x] **Released** — `wslay-sys` 0.1.2 (own code changed) and `h2ts-server` 0.1.4
  (doc comment changed, and needs the new dependency floor). `h2ts-client` links no
  wslay and its 0.1.3 docs built fine, so it stays put.
- [x] **CI gap closed** — _done 2026-07-30, after the release._ A `docs` job in
  `.github/workflows/ci.yml` runs `cargo doc --no-deps` over the three published crates
  with `CC=gcc-14` and `RUSTDOCFLAGS=-D warnings`; `make docs` is the local half (without
  the toolchain pin). Verified by reverting each fix in isolation against the job's exact
  command on GCC 14.2: reverting `build.rs` alone reproduces the `implicit declaration of
  function 'htons'` error, reverting the `h2ts-proxy` doc comment alone fails on the three
  `unresolved link` errors, and the current tree passes. Both regressions are therefore
  gated, not merely fixed.

  Note on why this needs a real toolchain bump rather than a flag: `-w` (from
  `.warnings(false)`) beats `-Werror=implicit-function-declaration` on GCC ≤ 13 in
  *either* order — measured on GCC 12.2 — so there is no CFLAGS-only assertion that would
  work on the default runner. GCC 14+ makes it a default-on **error**, which `-w` cannot
  downgrade, so the version bump is what does the work.
