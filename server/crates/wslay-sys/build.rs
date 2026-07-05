use std::env;
use std::path::PathBuf;

fn main() {
    let vendor = PathBuf::from("vendor");
    let includes = vendor.join("includes");

    // Compile the vendored wslay C sources into a static library. wslay needs no
    // config.h (that include is guarded by HAVE_CONFIG_H, which we leave unset)
    // and is little-endian-friendly by default.
    cc::Build::new()
        .files([
            vendor.join("wslay_event.c"),
            vendor.join("wslay_frame.c"),
            vendor.join("wslay_net.c"),
            vendor.join("wslay_queue.c"),
        ])
        .include(&vendor)
        .include(&includes)
        .warnings(false)
        .compile("wslay");

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
