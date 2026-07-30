use std::env;
use std::path::PathBuf;

fn main() {
    let vendor = PathBuf::from("vendor");
    let includes = vendor.join("includes");

    // Compile the vendored wslay C sources into a static library. wslay needs no
    // config.h (that include is guarded by HAVE_CONFIG_H, which we leave unset),
    // but it does read the individual HAVE_*/WORDS_BIGENDIAN macros that autotools
    // would have written into that config.h — so we set them ourselves, from what
    // Cargo already tells us about the target.
    let mut build = cc::Build::new();
    build
        .files([
            vendor.join("wslay_event.c"),
            vendor.join("wslay_frame.c"),
            vendor.join("wslay_net.c"),
            vendor.join("wslay_queue.c"),
        ])
        .include(&vendor)
        .include(&includes)
        .warnings(false);

    // wslay_frame.c and wslay_event.c call htons/ntohs, and wslay_net.c calls ntohl,
    // but wslay_net.h only includes the headers that declare them under these macros.
    // Leave them unset and the calls are merely *implicitly* declared: a warning on
    // older toolchains (and macOS declares them anyway, via sys/types.h -> sys/_endian.h,
    // so it builds there regardless) but a hard error on GCC 14+ / Clang 16+, which is
    // what breaks the docs.rs build.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        build.define("HAVE_WINSOCK2_H", None);
    } else {
        build
            .define("HAVE_ARPA_INET_H", None)
            .define("HAVE_NETINET_IN_H", None);
    }

    // ntoh64/hton64 are an identity on big-endian; without this wslay falls into the
    // byteswap path, which on a big-endian target swaps the two 32-bit halves and
    // corrupts extended-length WebSocket frame headers.
    if env::var("CARGO_CFG_TARGET_ENDIAN").as_deref() == Ok("big") {
        build.define("WORDS_BIGENDIAN", None);
    }

    build.compile("wslay");

    // Help bindgen/clang-sys find libclang on macOS Command Line Tools if the
    // environment hasn't already pointed at one.
    if env::var_os("LIBCLANG_PATH").is_none() {
        let clt = "/Library/Developer/CommandLineTools/usr/lib";
        if PathBuf::from(clt).join("libclang.dylib").exists() {
            env::set_var("LIBCLANG_PATH", clt);
        }
    }

    // Generate Rust FFI bindings from the public header.
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", includes.display()))
        .allowlist_function("wslay_.*")
        .allowlist_type("wslay_.*")
        .allowlist_var("WSLAY_.*")
        .generate()
        .expect("failed to generate wslay bindings");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out.join("bindings.rs"))
        .expect("failed to write wslay bindings");

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=vendor");
}
