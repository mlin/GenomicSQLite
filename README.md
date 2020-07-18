# Genomics Extension for SQLite

![build](https://github.com/mlin/GenomicSQLite/workflows/build/badge.svg?branch=main)

(**GenomicSQLite** for short!) Adds to the [ubiquitous](https://www.sqlite.org/mostdeployed.html) embedded RDBMS:

* genomic range indexing for overlap queries & joins
* streaming storage compression using [Zstandard](https://facebook.github.io/zstd/) (also available [standalone](https://github.com/mlin/sqlite_zstd_vfs))
* pre-tuned settings for "omics" scale datasets

Together, these make SQLite a [viable file format](https://www.sqlite.org/appfileformat.html) for storage, transport, and basic analysis of genomic data.

Notice: this project is *not* associated with the SQLite developers.
