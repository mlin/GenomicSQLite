// Build script for the genomicsqlite crate: inventory the prebuilt
// libgenomicsqlite libraries bundled next to Cargo.toml.
//
// A bundled library is embedded into the compilation unit only for the
// target it was built for, under a per-OS/architecture file name:
//     libgenomicsqlite-linux-x86_64.so    libgenomicsqlite-linux-aarch64.so
//     libgenomicsqlite-macos-x86_64.dylib libgenomicsqlite-macos-aarch64.dylib
// For each candidate file that actually exists in the package, this script
// sets a rustc cfg (bundled_linux_x86_64 etc.) gating the corresponding
// include_bytes! arm in src/lib.rs. A missing file therefore degrades to
// the runtime library search instead of breaking compilation with a
// missing-file error.
use std::env;
use std::path::Path;

// (target_os, target_arch, file name, cfg name) for each bundled candidate
const CANDIDATES: &[(&str, &str, &str, &str)] = &[
    (
        "linux",
        "x86_64",
        "libgenomicsqlite-linux-x86_64.so",
        "bundled_linux_x86_64",
    ),
    (
        "linux",
        "aarch64",
        "libgenomicsqlite-linux-aarch64.so",
        "bundled_linux_aarch64",
    ),
    (
        "macos",
        "x86_64",
        "libgenomicsqlite-macos-x86_64.dylib",
        "bundled_macos_x86_64",
    ),
    (
        "macos",
        "aarch64",
        "libgenomicsqlite-macos-aarch64.dylib",
        "bundled_macos_aarch64",
    ),
];

fn main() {
    // Declare the custom cfg names unconditionally, so rustc's check-cfg
    // validation accepts them wherever src/lib.rs mentions them.
    for cand in CANDIDATES {
        println!("cargo:rustc-check-cfg=cfg({})", cand.3);
    }
    println!("cargo:rustc-check-cfg=cfg(bundled_target)");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let bundling = env::var_os("CARGO_FEATURE_BUNDLE_LIBGENOMICSQLITE").is_some();
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    for cand in CANDIDATES {
        let path = Path::new(&manifest_dir).join(cand.2);
        // Rerun this script if a bundled library appears or disappears.
        println!("cargo:rerun-if-changed={}", path.display());
        if path.is_file() {
            println!("cargo:rustc-cfg={}", cand.3);
            if cand.0 == target_os && cand.1 == target_arch {
                // The compilation target has a bundled library, so one of
                // the include_bytes! arms in src/lib.rs will be compiled.
                println!("cargo:rustc-cfg=bundled_target");
            }
        } else if bundling && cand.0 == target_os && cand.1 == target_arch {
            println!(
                "cargo:warning=genomicsqlite: no bundled {} in the crate for {}-{}; \
                 the crate will fall back to searching for a system libgenomicsqlite at runtime",
                cand.2, cand.0, cand.1
            );
        }
    }
}
