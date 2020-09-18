# Genomics Extension for SQLite

### ("GenomicSQLite")

This [SQLite3 loadable extension](https://www.sqlite.org/loadext.html) adds features to the [ubiquitous](https://www.sqlite.org/mostdeployed.html) embedded RDBMS supporting applications in genome bioinformatics:

* genomic range indexing for overlap queries & joins
* streaming storage compression (also available [standalone](https://github.com/mlin/sqlite_zstd_vfs))
* pre-tuned settings for "big data"

Our **[Colab notebook](https://colab.research.google.com/drive/1OlHPOcRQBhDmEnS1wtOdtUGDkcD7LtKx?usp=sharing)** demonstrates key features with Python, one of several language bindings.

**USE AT YOUR OWN RISK:** The extension makes fundamental changes to the database storage layer. While designed to preserve ACID transaction safety, it's young and unlikely to have zero bugs. This project is not associated with the SQLite developers.

## [Installation & Programming Guide](https://mlin.github.io/GenomicSQLite/)

**Start Here 👉 [full documentation site](https://mlin.github.io/GenomicSQLite/)**

We supply the extension [prepackaged](https://github.com/mlin/GenomicSQLite/releases) for Linux x86-64 and macOS Catalina. An up-to-date version of SQLite itself is also required, as specified in the docs.

Programming language support:

* C/C++
* Python &ge;3.6

More to come. (Help wanted; see [Language Bindings Guide](https://mlin.github.io/GenomicSQLite/bindings/))

## Building from source

[![build](https://github.com/mlin/GenomicSQLite/workflows/build/badge.svg?branch=main)](https://github.com/mlin/GenomicSQLite/actions?query=workflow%3Abuild)

Most will prefer to install a pre-built shared library (see above). To build from source, see our [Actions yml (Ubuntu 20.04)](https://github.com/mlin/GenomicSQLite/blob/main/.github/workflows/build.yml) or [Dockerfile (Ubuntu 16.04)](https://github.com/mlin/GenomicSQLite/blob/main/Dockerfile) used to build the more-portable releases. Briefly, you'll need:

* C++11 build system
* CMake &ge; 3.14
* SQLite &ge; 3.31.0
* Zstandard &ge; 1.3.4

And incantations:

```
cmake -DCMAKE_BUILD_TYPE=Release -B build .
cmake --build build -j 4 --target genomicsqlite
```

...generating `build/libgenomicsqlite.so`. To run the test suite, you'll furthermore need:

* htslib &ge; 1.9, samtools, and tabix
* pigz
* Python &ge; 3.6 and packages: pytest pytest-xdist pre-commit black pylint flake8 
* clang-format & cppcheck

to:

```
pre-commit run --all-files  # formatters+linters
cmake -DCMAKE_BUILD_TYPE=Debug -B build .
cmake --build build -j 4
env -C build ctest -V
```
