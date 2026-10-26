# Distribution maintenance

## SQLite compatibility and deployment

The native extension rejects SQLite runtimes older than 3.31.0 because genomic range indexes use generated columns. This is a feature-compatibility floor, not a security support policy for SQLite. Passing the loader's version check does not establish that a runtime has current fixes or that all queries behave correctly. Use a maintained SQLite package and review the upstream [release history](https://www.sqlite.org/changes.html) and [guidance for untrusted inputs](https://www.sqlite.org/security.html).

The current CI tests Python 3.12 on Ubuntu 24.04 and macOS 15, using the selected interpreter's SQLite runtime. The portable Linux tests use the companion SQLite built by the Dockerfile, currently 3.54.1. These jobs do not cover every SQLite version above the feature floor or every supported Python version. The JDBC binding pins its own SQLite runtime separately in `bindings/jdbc/genomicsqlite-jdbc/pom.xml`. Report `SELECT sqlite_version()` from the host program when investigating compatibility; a shell's version need not match Python or JDBC.

Release binaries target Linux x86-64 (Ivy Bridge instruction set, glibc 2.28, GLIBCXX 3.4.25) and macOS Apple Silicon (macOS 15+). The Linux compiler target is `-march=ivybridge`, so the host must expose those CPU instructions, including AVX; an older CPU or a virtual machine with AVX disabled needs a source build with a suitable `CPU_ARCH` Docker build argument. Other platforms also require a source build. The Python package's `LIBGENOMICSQLITE` override can select that build; installing the wrapper alone does not provide native support for every platform.

Source builds require a compiler and C++ runtime supporting `-faligned-new` (GCC 7+ or a compatible Clang toolchain). The VFS embeds a cache-line-aligned queue; ordinary C++11 heap allocation does not guarantee its required alignment. The build enables aligned allocation while retaining C++11 and the selected `std::string` ABI. On Linux, the aligned allocation symbols require CXXABI 1.3.11, which is available within the release binary's GCC 8 runtime baseline. The `aligned_allocation` CTest checks heap alignment and queue operations.

## Download integrity

The Dockerfile verifies downloaded SQLite, Zstandard, and CMake artifacts before extracting or executing them. Version overrides require the matching digest override; changing only a version must fail verification. The committed SHA-256 values come from:

* SQLite 3.54.1: `sqlite-amalgamation-3540100.zip` fetched over HTTPS. Its published SHA3-256 on the [SQLite download page](https://www.sqlite.org/download.html) is `92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30`. After checking that digest, its SHA-256 was recorded in the Dockerfile.
* Zstandard 1.5.5: the upstream [`zstd-1.5.5.tar.gz.sha256`](https://github.com/facebook/zstd/releases/download/v1.5.5/zstd-1.5.5.tar.gz.sha256) release asset.
* CMake 3.17.3: the `cmake-3.17.3-Linux-x86_64.sh` entry in upstream [`cmake-3.17.3-SHA-256.txt`](https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3-SHA-256.txt).

For an update, obtain the checksum through the upstream HTTPS release metadata, verify the downloaded artifact, and update the version and digest together. Update SQLite's year in the download URL when needed. Rebuild all three Docker smoke-test targets and run the portable-library and installed-package tests in the workflow.

The CMake dependency graph pins `sqlite_zstd_vfs`, `sqlite_web_vfs`, `SQLiteCpp`, and `readerwriterqueue` to Git commits. The top-level `concurrentqueue` declaration pins the last of these before the transitive dependency declares its release tag. Review the transitive declarations when updating either VFS dependency.

These checks protect the selected source inputs; they do not make the entire build reproducible. Base-image tags, OS packages, Homebrew packages, and some CI tooling still change over time. Record the source revision and built-artifact digests when distributing or deploying binaries.
