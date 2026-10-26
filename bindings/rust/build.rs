// Select and validate the bundled library at build time; pass its crate-relative
// path to src/lib.rs for embedding. See docs/bindings.md for the selection policy.
use std::env;
use std::path::Path;

fn main() {
    // Declare the one custom cfg src/lib.rs uses to gate the bundled-bytes
    // path, so rustc's check-cfg validation accepts it.
    println!("cargo:rustc-check-cfg=cfg(bundled_lib)");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let bundling = env::var_os("CARGO_FEATURE_BUNDLE_LIBGENOMICSQLITE").is_some();
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    // The bundled-library candidate for this compilation target, if any.
    let candidate: Option<&str> = match (target_os.as_str(), target_arch.as_str()) {
        ("linux", "x86_64") => Some("x86_64-linux-gnu/libgenomicsqlite.so"),
        ("linux", "aarch64") => Some("aarch64-linux-gnu/libgenomicsqlite.so"),
        ("macos", "aarch64") => Some("aarch64-darwin/libgenomicsqlite.dylib"),
        _ => None,
    };

    let mut selected = "";
    if bundling {
        match candidate {
            Some(relpath) => {
                let path = Path::new(&manifest_dir).join(relpath);
                // Rerun this script if the bundled library appears or disappears.
                println!("cargo:rerun-if-changed={}", path.display());
                if path.is_file() {
                    selected = relpath;
                    println!("cargo:rustc-cfg=bundled_lib");
                } else {
                    panic!(
                        "genomicsqlite: the bundle_libgenomicsqlite feature is enabled \
                         but the bundled library {} is missing from the crate. \
                         Either package the prebuilt library at that path, or \
                         build with --no-default-features to load an external \
                         libgenomicsqlite at runtime instead.",
                        relpath
                    );
                }
            }
            None => {}
        }
    }
    println!("cargo:rustc-env=GENOMICSQLITE_BUNDLED_LIB={}", selected);
}
