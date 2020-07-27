# Genomics Extension for SQLite

## ("GenomicSQLite")

This [SQLite3 loadable extension](https://www.sqlite.org/loadext.html) adds features to the [ubiquitous](https://www.sqlite.org/mostdeployed.html) embedded RDBMS supporting applications in genome bioinformatics:

* genomic range indexing for overlap queries & joins
* streaming storage compression (also available [standalone](https://github.com/mlin/sqlite_zstd_vfs))
* pre-tuned settings for "big data"

Notice: this project is not associated with the SQLite developers.

### Use cases

The extension makes SQLite an efficient foundation for:

1. Integrative genomics data warehouse

    * BED, GFF/GTF, FASTA, FASTQ, SAM, VCF, ...

    * One file, zero administration, portable between platforms and languages

2. Slicing & basic analysis with indexed SQL queries, joins, & aggregations

3. Transactional storage engine for API services, incremental reanalysis, real-time basecalling & metagenomics, ...

4. Experimental new data models, before dedicated storage format & tooling are warranted, if ever.

### Contraindications

1. Huge numerical arrays: see [HDF5](https://www.hdfgroup.org/solutions/hdf5/), [Zarr](https://zarr.readthedocs.io/en/stable/), [Parquet](https://parquet.apache.org/), [Arrow](https://arrow.apache.org/). <small>SQLite's [BLOB I/O](https://www.sqlite.org/c3ref/blob_open.html) leaves the door open for mash-ups!</small>

2. Parallel SQL analytics / OLAP: see [Spark](https://spark.apache.org/), [DuckDB](https://duckdb.org/), many commercial products. <small>Some bases can be covered with a sharding harness for a pool of threads with their own SQLite connections...</small>

3. Streaming: SQLite I/O, while often highly sequential in practice, relies on randomly seeking throughout the database file.

## Under construction

![build](https://github.com/mlin/GenomicSQLite/workflows/build/badge.svg?branch=main)

The extension isn't quite ready for general use. The repo is public while we work on packaging and documentation.
