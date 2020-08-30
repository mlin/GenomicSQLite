# Genomics Extension for SQLite

### ("GenomicSQLite")

[https://github.com/mlin/GenomicSQLite](https://github.com/mlin/GenomicSQLite)

This [SQLite3 loadable extension](https://www.sqlite.org/loadext.html) supports applications in genome bioinformatics by adding:

* genomic range indexing for overlap queries & joins
* streaming storage compression using multithreaded [Zstandard](https://facebook.github.io/zstd/)
* pre-tuned settings for "big data"

Our **[Colab notebook](https://colab.research.google.com/drive/1ZqRjs0IFnGvb7TARUybkkfzpFQ_xrrgm?usp=sharing)** demonstrates the Python interface.

**USE AT YOUR OWN RISK:** The extension makes fundamental changes to the database storage layer. While designed to preserve ACID transaction safety, it's young and unlikely to have zero bugs. This project is not associated with the SQLite developers.

## SQLite &ge; 3.31.0 required

**To use the Genomics Extension you might first need to upgrade SQLite itself.** The host program must link [SQLite version 3.31.0 (2020-01-22)](https://www.sqlite.org/releaselog/3_31_0.html) or newer. In your shell, `sqlite3 --version` displays the version installed with your OS, which is probably what your programs use; if in doubt, cause a program to report the result of `SELECT sqlite_version()`.

If this is too old, then upgrade the system/environment SQLite3 library if possible & applicable. Otherwise, modify your program's linking step or runtime environment to cause it to use an up-to-date version, for example by setting rpath or LD_LIBRARY_PATH/DYLD_LIBRARY_PATH to the location of an up-to-date shared library file. Resources:

* [How To Compile SQLite](https://www.sqlite.org/howtocompile.html)
* [DreamHost Knowledge Base - Installing a custom version of SQLite3](https://help.dreamhost.com/hc/en-us/articles/360028047592-Installing-a-custom-version-of-SQLite3)
* [Ubuntu PPA](https://launchpad.net/~dqlite/+archive/ubuntu/stable) with `sqlite3`
* [Rpmfind: libsqlite3](https://rpmfind.net/linux/rpm2html/search.php?query=libsqlite3&submit=Search+...&system=&arch=)
* [Sqlite :: Anaconda Cloud](https://anaconda.org/anaconda/sqlite)
* Homebrew [formula/sqlite](https://formulae.brew.sh/formula/sqlite), [formula-linux/sqlite](https://formulae.brew.sh/formula-linux/sqlite)

You can always `SELECT sqlite_version()` to verify the upgrade in your program.

## Installation

It's usually easiest to obtain the extension as a pre-compiled shared library (Linux .so or macOS .dylib), either installed with a package manager, or downloaded from [GitHub Releases](https://github.com/mlin/GenomicSQLite/releases).

=== "Python"
    ``` bash
    pip3 install [--user|--system] genomicsqlite
    # -or-
    conda install -c mlin genomicsqlite

    # The package loads a bundled shared library by default. To ignore the bundled
    # file and search the regular dynamic-linking paths instead, set environment
    # GENOMICSQLITE_SYSTEM_LIBRARY=1
    ```

=== "C/C++"
    ``` bash
    Download zip of shared library and genomicsqlite.h from GitHub Releases:
        https://github.com/mlin/GenomicSQLite/releases
    Build your program with the shared library, and also ensure the dynamic linker
    will find it at runtime, by either --
        1. installing it in a system or user lib directory (+ refresh cache)
        2. setting LD_LIBRARY_PATH or DYLD_LIBRARY_PATH environment variable
        3. building with -rpath

    Recommendation: -also- install the Python package, which includes a useful
                    command-line shell and smoke-test script.
    ```

See our [GitHub README](https://github.com/mlin/GenomicSQLite) for the source build procedure, if needed.

We welcome community contributions to the available language bindings; see the Language Bindings Guide if interested.

## Smoke test

We recommend trying our "smoke test" script to quickly verify your local system's compatibility. It requires the Python package installed per the instructions above.

```bash
wget -nv -O - https://raw.githubusercontent.com/mlin/GenomicSQLite/release/test/genomicsqlite_smoke_test.py \
    | python3 -
```

Even if you're not planning to use Python, this test's detailed logs may be useful to diagnose general problems loading the extension.

## Use cases

The extension makes SQLite an efficient foundation for:

1. Integrative genomics data warehouse
    * BED, GFF/GTF, FASTA, FASTQ, SAM, VCF, ...
    * One file, zero administration, portable between platforms and languages
2. Slicing & basic analysis with indexed SQL queries, joins, & aggregations
3. Transactional storage engine for API services, incremental reanalysis, real-time basecalling & metagenomics, ...
4. Experimental new data models, before dedicated storage format & tooling are warranted, if ever.

### Contraindications

1. Huge numerical arrays: see [HDF5](https://www.hdfgroup.org/solutions/hdf5/), [Zarr](https://zarr.readthedocs.io/en/stable/), [Parquet](https://parquet.apache.org/), [Arrow](https://arrow.apache.org/), [TileDB](https://github.com/TileDB-Inc/TileDB). <small>SQLite's [BLOB I/O](https://www.sqlite.org/c3ref/blob_open.html) leaves the door open for mash-ups!</small>
2. Parallel SQL analytics / OLAP: see [Spark](https://spark.apache.org/), [DuckDB](https://duckdb.org/), many commercial products. <small>(Some bases can be covered with a sharding harness for a pool of threads with their own SQLite connections...)</small>
3. Streaming: SQLite storage relies on randomly reading & writing throughout the database file.

**Proceed to the Programming Guide section to get started building applications.**
