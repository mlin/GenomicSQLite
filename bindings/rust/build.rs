// Build script for the genomicsqlite crate: when the bundle_libgenomicsqlite
// feature is enabled, select the ONE prebuilt libgenomicsqlite matching the
// compilation target (OS + CPU + libc ABI), validate that it's present in the
// package, and hand its file name to src/lib.rs via cargo:rustc-env, where a
// single include_bytes! embeds it.
//
// Supported bundled targets (the Linux libraries link glibc, so the target
// env must be gnu):
//     linux + x86_64 + gnu   -> libgenomicsqlite-linux-x86_64.so
//     linux + aarch64 + gnu  -> libgenomicsqlite-linux-aarch64.so
//     macos + aarch64        -> libgenomicsqlite-macos-aarch64.dylib
//
// Bundling is a build-time contract: a supported target whose bundled file is
// missing FAILS the build, as does a non-glibc Linux target whose CPU has a
// bundled (glibc) library in the package. Any other target, or building
// without the feature (--no-default-features), leaves
// GENOMICSQLITE_BUNDLED_LIB empty, and the crate loads an external
// libgenomicsqlite at runtime (LIBGENOMICSQLITE / system loader search).
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
    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();

    // The single bundled file for this compilation target, if supported.
    let bundled_file: Option<&str> = match (
        target_os.as_str(),
        target_arch.as_str(),
        target_env.as_str(),
    ) {
        ("linux", "x86_64", "gnu") => Some("libgenomicsqlite-linux-x86_64.so"),
        ("linux", "aarch64", "gnu") => Some("libgenomicsqlite-linux-aarch64.so"),
        ("macos", "aarch64", _) => Some("libgenomicsqlite-macos-aarch64.dylib"),
        _ => None,
    };

    let mut selected = "";
    if bundling {
        match bundled_file {
            Some(name) => {
                let path = Path::new(&manifest_dir).join(name);
                // Rerun this script if the bundled library appears or disappears.
                println!("cargo:rerun-if-changed={}", path.display());
                if path.is_file() {
                    selected = name;
                    println!("cargo:rustc-cfg=bundled_lib");
                } else {
                    panic!(
                        "genomicsqlite: the bundle_libgenomicsqlite feature is enabled \
                         but the bundled library {} for target {}-{}-{} is missing \
                         from the crate. Either package the prebuilt library under \
                         that name, or build with --no-default-features to load an \
                         external libgenomicsqlite at runtime instead.",
                        name, target_os, target_arch, target_env
                    );
                }
            }
            None => {
                // Incompatible-bundle guard: if this is a Linux target with a
                // non-glibc ABI and the package carries the glibc library for
                // the same CPU, reject the combination outright -- selecting
                // it would produce a broken binary, and silently ignoring it
                // would mask the ABI mismatch.
                if target_os == "linux" && target_env != "gnu" {
                    let glibc_twin = match target_arch.as_str() {
                        "x86_64" => Some("libgenomicsqlite-linux-x86_64.so"),
                        "aarch64" => Some("libgenomicsqlite-linux-aarch64.so"),
                        _ => None,
                    };
                    if let Some(twin) = glibc_twin {
                        let path = Path::new(&manifest_dir).join(twin);
                        println!("cargo:rerun-if-changed={}", path.display());
                        if path.is_file() {
                            panic!(
                                "genomicsqlite: the bundled {} is built for glibc \
                                 (linux-gnu), which is incompatible with this \
                                 linux-{} compilation target. Build with \
                                 --no-default-features and provide a \
                                 libgenomicsqlite built for linux-{} instead \
                                 (LIBGENOMICSQLITE env var / system loader).",
                                twin, target_env, target_env
                            );
                        }
                    }
                }
                // Any other unsupported target: no bundle; the crate loads an
                // external libgenomicsqlite at runtime.
            }
        }
    }
    println!("cargo:rustc-env=GENOMICSQLITE_BUNDLED_LIB={}", selected);
}
