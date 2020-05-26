# Genomics Extensions for SQLite
(**GenomicSQLite** for short!) Adds to the [#1 database engine](https://www.sqlite.org/mostdeployed.html):

* read/write storage compression using [Zstandard](https://facebook.github.io/zstd/) (also available [standalone](https://github.com/mlin/sqlite_zstd_vfs))
* genomic range indexing for efficient overlap queries & joins
* settings pre-tuned for processing >100GiB datasets

Together, these make SQLite a [suitable file format](https://www.sqlite.org/appfileformat.html) for storage, transport, and basic analysis of genomic data.
