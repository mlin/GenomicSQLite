# Genomics Extension for SQLite

### ("GenomicSQLite")

This [SQLite3 loadable extension](https://www.sqlite.org/loadext.html) adds features to the [ubiquitous](https://www.sqlite.org/mostdeployed.html) embedded RDBMS supporting applications in genome bioinformatics:

* genomic range indexing for overlap queries & joins
* streaming storage compression (also available [standalone](https://github.com/mlin/sqlite_zstd_vfs))
* pre-tuned settings for "big data"

Our **[Google Colaboratory Notebook](https://colab.research.google.com/drive/1ZqRjs0IFnGvb7TARUybkkfzpFQ_xrrgm?usp=sharing)** demonstrates the Python interface.

#### Use cases

The extension makes SQLite an efficient foundation for:

1. Integrative genomics data warehouse
    * BED, GFF/GTF, FASTA, FASTQ, SAM, VCF, ...
    * One file, zero administration, portable between platforms and languages
2. Slicing & basic analysis with indexed SQL queries, joins, & aggregations
3. Transactional storage engine for API services, incremental reanalysis, real-time basecalling & metagenomics, ...
4. Experimental new data models, before dedicated storage format & tooling are warranted, if ever.

#### Contraindications

1. Huge numerical arrays: see [HDF5](https://www.hdfgroup.org/solutions/hdf5/), [Zarr](https://zarr.readthedocs.io/en/stable/), [Parquet](https://parquet.apache.org/), [Arrow](https://arrow.apache.org/). SQLite's [BLOB I/O](https://www.sqlite.org/c3ref/blob_open.html) leaves the door open for mash-ups!
2. Parallel SQL analytics / OLAP: see [Spark](https://spark.apache.org/), [DuckDB](https://duckdb.org/), many commercial products. (Some bases can be covered with a sharding harness for a pool of threads with their own SQLite connections...)
3. Streaming: SQLite I/O, while often highly sequential in practice, relies on randomly seeking throughout the database file.

**USE AT YOUR OWN RISK:** The extension makes fundamental changes to the database storage layer. While designed with attention to preserving ACID transaction safety, it's young and unlikely to have zero bugs. This project is not associated with the SQLite developers.

## Under construction

The extension isn't quite ready for general use. The repo is public while we work on packaging and documentation.

## Building from source

![build](https://github.com/mlin/GenomicSQLite/workflows/build/badge.svg?branch=main)

Most will prefer to install a pre-built shared library, as documented. To build from source, see our [Actions yml (Ubuntu 20.04)](https://github.com/mlin/GenomicSQLite/blob/main/.github/workflows/build.yml) or [Dockerfile (Ubuntu 16.04)](https://github.com/mlin/GenomicSQLite/blob/main/Dockerfile) used to build the more-portable releases. Briefly, you'll need:

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
* Python &ge; 3.6, pytest, and pytest-xdist

to:

```
cmake -DCMAKE_BUILD_TYPE=Debug -B build .
cmake --build build -j 4
env -C build ctest -V
```
