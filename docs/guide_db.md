# Programming Guide - Opening Compressed Databases

The Genomics Extension integrates with your programming language's existing SQLite3 bindings to provide a familiar experience wherever possible.

* Python: [sqlite3](https://docs.python.org/3/library/sqlite3.html)
* Java/JVM: [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc)
* Rust: [rusqlite](https://github.com/rusqlite/rusqlite)
* C++: [SQLiteCpp](https://github.com/SRombauts/SQLiteCpp) (optional, recommended) or directly using...
* C: [SQLite C/C++ API](https://www.sqlite.org/cintro.html)

First complete the [installation instructions](index.md).

## Loading the extension

=== "Python"
    ``` python3
    import sqlite3
    import genomicsqlite
    ```

=== "Java"
    ```java
    import java.sql.*;
    import net.mlin.genomicsqlite.GenomicSQLite;
    ```

=== "Rust"
    ```rust
    use genomicsqlite::ConnectionMethods;
    use rusqlite::{Connection, OpenFlags, params, NO_PARAMS};
    ```

    The `genomicsqlite::ConnectionMethods` trait makes available GenomicSQLite-specific methods for
    `rusqlite::Connection` (and `rusqlite::Transaction`). See [rustdoc](https://docs.rs/genomicsqlite)
    for some extra details.

=== "C++"
    ``` c++
    #include <sqlite3.h>
    #include "SQLiteCpp/SQLiteCpp.h" // optional
    #include "genomicsqlite.h"

    int main() {
      try {
        GENOMICSQLITE_CXX_INIT();
      } catch (std::runtime_error& exn) {
        // report exn.what()
      }
      ...
    }
    ```

    Link the program to `sqlite3` and `genomicsqlite` libraries. Optionally, include
    [SQLiteCpp](https://github.com/SRombauts/SQLiteCpp) headers *before* `genomicsqlite.h` to use
    its more-convenient API; but *don't* link it, as the `genomicsqlite` library has it built-in.

    GNU/Linux: to link the prebuilt `libgenomicsqlite.so` distributed from our GitHub Releases, you
    may have to compile your source with `CXXFLAGS=-D_GLIBCXX_USE_CXX11_ABI=0`. This is because the
    library is built against an old libstdc++ version to improve runtime compatibility. The
    function of this flag is explained in the libstdc++ docs on
    [Dual ABI](https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html). If you build
    `libgenomicsqlite.so` from source, then the flag will not be needed.

    General note: GenomicSQLite C++ routines are liable to throw exceptions.

=== "C"
    ``` c
    #include <sqlite3.h>
    #include "genomicsqlite.h"

    int main() {
      char *zErrMsg = 0;
      int rc = GENOMICSQLITE_C_INIT(&zErrMsg);
      if (rc != SQLITE_OK) {
        /* report zErrMsg */
        sqlite3_free(zErrMsg);
      }
      ...
    }
    ```

    Link the program to `sqlite3` and `genomicsqlite` libraries.

    All GenomicSQLite C routines returning a `char*` string use the following convention. If the
    operation succeeds, then it's a nonempty, null-terminated string. Otherwise, it points to a
    null byte followed immediately by a nonempty, null-terminated error message. *In either case,*
    the caller must free the string with `sqlite3_free()`. NULL is returned only if out of memory.

## Opening a compressed database

**↪ GenomicSQLite Open:** create or open a compressed database, returning a connection object with various settings pre-tuned for large datasets.

=== "Python"
    ``` python3
    dbconn = genomicsqlite.connect(
      db_filename,
      read_only=False,
      **kwargs  #  genomicsqlite + sqlite3.connect() arguments
    )
    assert isinstance(dbconn, sqlite3.Connection)
    ```

=== "Java"
    ```java
    java.util.Properties config = new java.util.Properties();
    config.setProperty("genomicsqlite.config_json", "{}");
    // Properties may originate from org.sqlite.SQLiteConfig.toProperties()
    // with genomicsqlite.config_json added in.

    Connection dbconn = DriverManager.getConnection(
      "jdbc:genomicsqlite:" + dbfileName,
      config
    );
    ```

=== "Rust"
    ```rust
    let dbconn: Connection = genomicsqlite::open(
      db_filename,
      OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
      &json::object::Object::new()  // tuning options
    )?;

    ```

=== "SQLiteCpp"
    ``` c++
    std::unique_ptr<SQLite::Database> GenomicSQLiteOpen(
      const std::string &db_filename,
      int flags = 0,
      const std::string &config_json = "{}"
    );
    ```

=== "C++"
    ``` c++
    int GenomicSQLiteOpen(
      const std::string &db_filename,
      sqlite3 **ppDb,
      std::string &errmsg_out,
      int flags = 0,  // as sqlite3_open_v2() e.g. SQLITE_OPEN_READONLY
      const std::string &config_json = "{}"
    ) noexcept; // returns sqlite3_open_v2() code
    ```

=== "C"
    ``` c
    int genomicsqlite_open(
      const char *db_filename,
      sqlite3 **ppDb,
      char **pzErrMsg, /* if nonnull and an error occurs, set to error message
                        * which caller should sqlite3_free() */
      int flags,              /* as sqlite3_open_v2() e.g. SQLITE_OPEN_READONLY */
      const char *config_json /* JSON text (may be null) */
    ); /* returns sqlite3_open_v2() code */
    ```

Afterwards, all the usual SQLite3 API operations are available through the returned connection object, which should finally be closed in the usual way. The [storage compression layer](https://github.com/mlin/sqlite_zstd_vfs) operates transparently underneath.

**❗ GenomicSQLite databases should *only* be opened using this routine.** If a program opens an existing GenomicSQLite database using a generic SQLite3 API, it will find a valid database whose schema is that of the compression layer instead of the intended application's. Writing into that schema might effectively corrupt the database!

### Tuning options

The aforementioned tuned settings can be further adjusted. Some bindings (e.g. C/C++) receive these options as the text of a JSON object with keys and values, while others admit individual arguments to the Open routine.

* **threads = -1**: thread budget for compression, sort, and prefetching/decompression operations; -1 to match up to 8 host processors. Set 1 to disable all background processing.
* **inner_page_KiB = 16**: [SQLite page size](https://www.sqlite.org/pragma.html#pragma_page_size) for new databases, any of {1, 2, 4, 8, 16, 32, 64}. Larger pages are more compressible, but increase random I/O cost.
* **outer_page_KiB = 32**: compression layer page size for new databases, any of {1, 2, 4, 8, 16, 32, 64}. <br/>
The default configuration (inner_page_KiB, outer_page_KiB) = (16,32) balances random access speed and compression. Try setting them to (8,16) to prioritize random access, or (64,2) to prioritize compression <small>(if compressed database will be <4TB)</small>.
* **zstd_level = 6**: Zstandard compression level for newly written data (-7 to 22)
* **unsafe_load = false**: set true to disable write transaction safety (see advice on bulk-loading below). <br/>
    **❗ A database written to unsafely is liable to be corrupted if the application crashes, or if there's a concurrent attempt to modify it.**
* **page_cache_MiB = 1024**: database cache size. Use a large cache to avoid repeated decompression in successive and complex queries. 
* **immutable = false**: set true to slightly reduce overhead reading from a database file that won't be modified by this or any concurrent program, guaranteed.
* **force_prefetch = false**: set true to enable background prefetching/decompression even if inner_page_KiB &lt; 16 (enabled by default only &ge; that, as it can be counterproductive below; YMMV)

The connection's potential memory usage can usually be budgeted as roughly the page cache size, plus the size of any uncommitted write transaction (unless unsafe_load), plus some safety factor. ❗However, this can *multiply by (threads+1)* during queries whose results are at least that large and must be re-sorted. That includes index creation, when the indexed columns total such size.

## genomicsqlite interactive shell

The Python package includes a `genomicsqlite` script that enters the [`sqlite3` interactive shell](https://sqlite.org/cli.html) on an existing compressed database. This is a convenient way to inspect and explore the data with *ad hoc* SQL queries, as one might use `grep` or `awk` on text files. With the Python package installed (`pip3 install genomicsqlite` or `conda install -c mlin genomicsqlite`):

```
$ genomicsqlite DB_FILENAME [--readonly]
```

to enter the SQL prompt with the database open. Or, add an SQL statement (in quotes) to perform and exit. If you've installed the Python package but the script isn't found, set your `PATH` to include the `bin` directory with Python console scripts.

**Database compaction.** The utility has a subcommand to compress and defragment an existing database file (compressed or uncompressed), which can increase its compression level and optimize access to it.

```
$ genomicsqlite DB_FILENAME --compact
```

generates `DB_FILENAME.compact`; see its `--help` for additional options, in particular `--level`, `--inner-page-KiB` and `--outer-page-KiB` affect the output file size as discussed above.

Due to decompression overhead, the compaction procedure may be impractically slow if the database has big tables that weren't initially written in their primary key order. To prevent this, see below *Optimizing storage layout*.

## Reading databases over the web

The **GenomicSQLite Open** routine and the `genomicsqlite` shell also accept http: and https: URLs instead of local filenames, creating a connection to read the compressed file over the web directly. The database connection must be opened read-only in the appropriate manner for your language bindings (such as the flag `SQLITE_OPEN_READONLY`). The URL server must support [HTTP GET range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) requests, and the content must not change for the lifetime of the connection.

Under the hood, the extension uses [libcurl](https://curl.se/libcurl/) to send web requests for necessary portions of the database file as queries proceed, with adaptive batching & prefetching to balance the number and size of these requests. This works well for point lookups and queries that scan largely-contiguous slices of tables and indexes (a modest number thereof). It's less suitable for big multi-way joins and other aggressively random access patterns; in such cases, it'd be better to download the database file upfront to open locally.

* The above-described `genomicsqlite DB_FILENAME --compact` tool can optimize a file's suitability for web access.
* Reading large databases over the web, budget an additional ~600MiB of memory for HTTP prefetch buffers.
* To disable TLS certificate and hostname verification, set web_insecure = true in the GenomicSQLite configuration, or SQLITE_WEB_INSECURE=1 in the environment.
* The HTTP driver writes log messages to standard error when requests fail or had to be retried, which can be disabled by setting configuration web_log = 0 or environment SQLITE_WEB_LOG=0; or increased up to 5 to log every request and other details.

## Advice for big data

### Writing large databases quickly

1. `sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0)` if available, to reduce overhead in SQLite3's allocation routines.
1. Open database with unsafe_load = true to reduce transaction processing overhead (at aforementioned risk) for the connection's lifetime.
1. Also open with the flag `SQLITE_OPEN_NOMUTEX`, if your application naturally serializes operations on the connection.
1. Perform all of the following steps within one big SQLite transaction, committed at the end.
1. Insert data rows reusing prepared, parameterized SQL statements.
    1. Process the rows in primary key order, if feasible (otherwise, see below *Optimizing storage layout*).
    1. Consider preparing data in producer thread(s), with a consumer thread executing insertion statements in a tight loop.
    1. Bind text/blob parameters using [`SQLITE_STATIC`](https://www.sqlite.org/c3ref/bind_blob.html) if suitable.
1. Create secondary indexes, including genomic range indexes, only after loading all row data. Use [partial indexes](https://www.sqlite.org/partialindex.html) when they suffice.

### Optimizing storage layout

For multiple reasons mentioned so far, large tables should have their rows initially inserted in primary key order (or whatever order will promote access locality), ensuring they'll be stored as such in the database file; and tables should be written one-at-a-time. If it's inconvenient to process the input data in this way, the following procedure can help:

1. Create [*temporary* table(s)](https://sqlite.org/lang_createtable.html) with the same schema as the destination table(s), but omitting any PRIMARY KEY specifiers, UNIQUE constraints, or other indexes.
2. Stream all the data into these temporary tables, which are fast to write and read, in whatever order is convenient.
3. `INSERT INTO permanent_table SELECT * FROM temp_table ORDER BY colA, colB, ...` using the primary key (or other desired sort order) for each table.

The Genomics Extension automatically enables SQLite's [parallel, external merge-sorter](https://sqlite.org/src/file/src/vdbesort.c) to execute the last step efficiently. Ensure it's [configured](https://www.sqlite.org/tempfiles.html) to use a suitable storage subsystem for big temporary files.

### Compression guidelines

The [Zstandard](https://facebook.github.io/zstd/)-based [compression layer](https://github.com/mlin/sqlite_zstd_vfs) is effective at capturing the high compressibility of bioinformatics data. But, one should expect a general-purpose database to use extra space to keep everything organized, compared to a file format dedicated to one read-only schema. To set a rough expectation, the maintainers feel fairly satisfied if the database file size isn't more than double that of a bespoke compression format — especially if it includes useful indexes (which if well-designed, should be relatively incompressible).

The aforementioned zstd_level, threads, and page_size options all affect the compression time-space tradeoff, while enlarging the page cache can reduce decompression overhead (workload-dependent).

If you plan to delete or overwrite a significant amount of data in an existing database, issue [`PRAGMA secure_delete=ON`](https://www.sqlite.org/pragma.html#pragma_secure_delete) beforehand to keep the compressed file as small as possible. This works by causing SQLite to overwrite unused database pages with all zeroes, which the compression layer can then reduce to a negligible size.

With SQLite's row-major table [storage format](https://www.sqlite.org/fileformat.html), the first read of a lone cell usually entails decompressing at least its whole row, and there aren't any special column encodings for deltas, run lengths, etc. The "last mile" of optimization may therefore involve certain schema compromises, such as storing infrequently-accessed columns in a separate table to join when needed, or using application-layer encodings with [BLOB I/O](https://www.sqlite.org/c3ref/blob_open.html).
