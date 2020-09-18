# Programming Guide

The Genomics Extension integrates with your programming language's existing SQLite3 bindings to provide a familiar experience wherever possible.

* Python: [sqlite3](https://docs.python.org/3/library/sqlite3.html)
* Java/JVM: [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc)
* C++: [SQLiteCpp](https://github.com/SRombauts/SQLiteCpp) (optional, recommended) or directly using...
* C: [SQLite C/C++ API](https://www.sqlite.org/cintro.html)

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

=== "C++"
    ``` c++
    // link program to sqlite3 and genomicsqlite libraries; optionally, SQLiteCpp:
    //     https://github.com/SRombauts/SQLiteCpp
    #include <sqlite3.h>
    #include "SQLiteCpp/SQLiteCpp.h" // optional 
    #include "genomicsqlite.h"

    // General note: most GenomicSQLite C++ routines are liable to throw.
    ```

=== "C"
    ``` c
    /* link program to sqlite3 and genomicsqlite libraries */
    #include <sqlite3.h>
    #include "genomicsqlite.h"

    /* General note: all GenomicSQLite C routines returning a char* string use
     * the following convention:
     * If the operation suceeds then it's a nonempty, null-terminated string.
     * Otherwise it points to a null byte followed immediately by a nonempty,
     * null-terminated error message.
     * IN EITHER CASE, the caller should free the string with sqlite3_free().
     * Null is returned only if malloc failed.
     */
    ```

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

### Advice for big data

**Tips for writing large databases quickly:**

1. `sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0)` if available, to reduce overhead in SQLite3's allocation routines.
1. Open database with unsafe_load = true to reduce transaction processing overhead (at aforementioned risk) for the connection's lifetime.
1. Also open with the flag `SQLITE_OPEN_NOMUTEX`, if your application naturally serializes operations on the connection.
1. Perform all of the following steps within one big SQLite transaction, committed at the end.
1. Insert data rows reusing prepared, parameterized SQL statements.
    1. For tables with explicit primary keys: insert their rows in primary key order, if feasible.
    1. Consider preparing data in producer thread(s), with a consumer thread executing insertion statements in a tight loop.
    1. Bind text/blob parameters using [`SQLITE_STATIC`](https://www.sqlite.org/c3ref/bind_blob.html) if suitable.
1. Create secondary indices, including genomic range indices, only after loading all row data. Use [partial indices](https://www.sqlite.org/partialindex.html) when they suffice.

**Compression guidelines**

The [Zstandard](https://facebook.github.io/zstd/)-based [compression layer](https://github.com/mlin/sqlite_zstd_vfs) is effective at capturing the typically high compressibility of bioinformatics data. But, one should expect a general-purpose database to need some extra space to keep everything organized, compared to a file format dedicated to one predetermined, read-only schema. To set a rough expectation, the maintainers feel fairly satisfied if the database file size isn't more than double that of a bespoke compression format — especially if it includes useful indices (which if well-designed, should be relatively incompressible).

With SQLite's row-major table [storage format](https://www.sqlite.org/fileformat.html), the first read of a lone cell usually entails decompressing at least its whole row, and there aren't any special column encodings for deltas, run lengths, etc. The "last mile" of optimization may therefore involve certain schema compromises, such as storing infrequently-accessed columns in a separate table to join when needed, or using application-layer encodings with [BLOB I/O](https://www.sqlite.org/c3ref/blob_open.html).

The aforementioned zstd_level, threads, and page_size options all affect the compression time-space tradeoff, while enlarging the page cache can reduce decompression overhead (workload-dependent).

## Genomic range indexing

### Overview

GenomicSQLite enables creation of a **Genomic Range Index (GRI)** for any database table in which each row represents a genomic feature with (chromosome, beginPosition, endPosition) coordinates. The coordinates may be sourced from table columns or by computing arithmetic expressions thereof. The index tracks any updates to the underlying table as usual, with one caveat explained below.

Once indexed, the table can be queried for all features overlapping a query range. A GRI query yields a [rowid](https://www.sqlite.org/rowidtable.html) set, which your SQL query can select from the indexed table for further filtering or analysis. Please review the brief SQLite documentation on [rowid](https://www.sqlite.org/rowidtable.html) and [Autoincrement](https://www.sqlite.org/autoinc.html).

### Conventions

Range positions are considered [**zero-based & half-open**](http://www.cs.utexas.edu/users/EWD/transcriptions/EWD08xx/EWD831.html), so the length of a feature is exactly endPosition-beginPosition nucleotides. The implementation doesn't strictly require this convention, but we strongly recommend observing it to minimize confusion. There is no practical limit on chromosome length, as position values may go up to 2<sup>60</sup>, but queries have a runtime factor logarithmic in the maximum feature length.

The extension provides routines to populate a small `_gri_refseq` table describing the genomic reference sequences, which other tables can reference by integer ID ("rid") instead of storing a column with textual sequence names like 'chr10'. This convention is not required, as the GRI can index either chromosome name or rid columns, but reasons to observe it include:

* Integers are more compact and faster to look up.
* Results sort properly with `ORDER BY rid` instead of considering e.g. `'chr10'` < `'chr2'` lexicographically.
* A table with chromosome names can be reconstructed easily by joining with `_gri_refseq`.

### Create GRI

**↪ Create Genomic Range Index SQL:** *Generate a string* containing a series of SQL statements which *when executed* create a GRI on an existing table. Executing them is left to the caller, perhaps after logging the contents. The statements should be executed within a transaction to succeed or fail atomically.

=== "Python"
    ``` python3
    create_gri_sql = genomicsqlite.create_genomic_range_index_sql(
      dbconn,
      'tableName',
      'chromosome',
      'beginPosition',
      'endPosition'
    )
    dbconn.executescript(create_gri_sql)
    ```

=== "Java"
    ```java
    String griSql = GenomicSQLite.createGenomicRangeIndexSQL(
      dbconn,
      "tableName",
      "chromosome",
      "beginPosition",
      "endPosition"
    );
    dbconn.createStatement().executeUpdate(griSql);
    ```

=== "SQLiteCpp"
    ``` c++
    std::string CreateGenomicRangeIndexSQL(
      const std::string &table,
      const std::string &rid,
      const std::string &beg,
      const std::string &end,
      int floor = -1
    );

    std::string create_gri_sql = CreateGenomicRangeIndexSQL(
      "tableName", "chromosome", "beginPosition", "endPosition"
    );
    // SQLite::Database* dbconn in a transaction
    dbconn->exec(create_gri_sql);
    ```

=== "C++"
    ``` c++
    std::string CreateGenomicRangeIndexSQL(
      const std::string &table,
      const std::string &rid,
      const std::string &beg,
      const std::string &end,
      int floor = -1
    );

    std::string create_gri_sql = CreateGenomicRangeIndexSQL(
      "tableName", "chromosome", "beginPosition", "endPosition"
    );
    // sqlite3* dbconn in a transaction
    char* errmsg = nullptr;
    int rc = sqlite3_exec(dbconn, create_gri_sql.c_str(), nullptr, nullptr, &errmsg);
    // check rc, free errmsg
    ```

=== "C"
    ``` c
    char* create_genomic_range_index_sql(
      const char* table,
      const char* rid,
      const char* beg,
      const char* end,
      int floor
    );
    char* create_gri_sql = create_genomic_range_index_sql(
      "tableName", "chromosome", "beginPosition", "endPosition", -1
    );
    if (*create_gri_sql) {
      char* errmsg = 0;
      /* sqlite3* dbconn in a transaction */
      int rc = sqlite3_exec(dbconn, create_gri_sql, 0, 0, &errmsg);
      /* check rc, free errmsg */
    } else {
     /* General note: all GenomicSQLite C routines returning a char* string use
      * the following convention:
      * If the operation suceeds then it's a nonempty, null-terminated string.
      * Otherwise it points to a null byte followed immediately by a nonempty,
      * null-terminated error message.
      * IN EITHER CASE, the caller should free the string with sqlite3_free().
      * Null is returned only if malloc failed.
      */
    }
    sqlite3_free(create_gri_sql);
    ```

The three arguments following the table name tell the indexing procedure how to read the feature coordinates from each table row.

1. The reference sequence may be sourced either by the name of a text column containing names like 'chr10', or of an integer reference ID (rid) column, as discussed above.
2. The begin and end positions are read from named integer columns, or by computing simple arithmetic expressions thereof.
3. For example, if the table happens to have `beginPosition` and `featureLength` columns, the end position may be formulated `'beginPosition+featureLength'`.

**❗ The table name and expressions are textually pasted into a template SQL script. Take care to prevent SQL injection, if they're in any way determined by external input.**

A last optional integer argument `floor` can be omitted or left at -1. <small>GRI performance may be improved slightly by setting `floor` to a positive integer *F* if the following is true: the lengths of the indexed features are almost all &gt;16<sup>*F*-1</sup>, with only very few outlier lengths &le;16<sup>*F*-1</sup>. For example, human exons are almost all &gt;16nt; one may therefore set `floor=2` as a modest optimization for such data. YMMV</small>

The indexing script will, among other steps, add a few [generated columns](https://sqlite.org/gencol.html) to the original table. So if you later `SELECT * FROM tableName`, you'll get these extra values back (column names starting with `_gri_`). The extra columns are "virtual" so they don't take up space in the table itself, but they do end up populating the stored index.

At present, GRI cannot be used on [WITHOUT ROWID](https://www.sqlite.org/withoutrowid.html) tables.

### Query GRI

The extension supplies a special SQL function to query a GRI-indexed table, generating the set of rowids identifying features that overlap a query range (queryChrom, queryBegin, queryEnd):

`genomic_range_rowids(tableName, queryChrom, queryBegin, queryEnd[, ceiling, floor])`

This is typically used to retrieve the result rows by selecting for `tableName._rowid_ IN genomic_range_rowids(...)`. For example,

```sql
SELECT col1, col2, ... FROM exons WHERE exons._rowid_ IN
  genomic_range_rowids('exons', 'chr12', 111803912, 111804012)
```

The queryChrom parameter might have SQL type TEXT or INTEGER, according to whether the GRI indexes name or rid.

The ordered rowid set identifies the features satisfying,

``` sql
queryChrom = featureChrom AND
  NOT (queryBegin > featureEnd OR queryEnd < featureBegin)
```

(*"query is not disjoint from feature"*)

By the half-open position convention, this includes features that *abut* as well as those that *overlap* the query range. If you don't want those, or if you want only "contained" features, simply add such constraints to your query's WHERE clause.

<small>The query will not match any rows with NULL feature coordinates. If needed, the GRI can inform this query for NULL chromosome/rid: `SELECT ... FROM tableName WHERE _gri_rid IS NULL`.</small>

#### Level bounds optimization

The optional, trailing `ceiling` & `floor` arguments to `genomic_range_rowids()` optimize GRI queries by skipping steps that'd be useless in view of the length distribution of the indexed features. (See Internals for full explanation.)

The extension supplies a SQL helper function `genomic_range_index_levels(tableName)` to detect the appropriate bounds for (the current snapshot of) the table. Example usage:

```sql
SELECT col1, col2, ... FROM exons, genomic_range_index_levels('exons')
  WHERE exons._rowid_ IN
    genomic_range_rowids('exons', 'chr12', 111803912, 111804012,
                         _gri_ceiling, _gri_floor)
```

Here `_gri_ceiling` and `_gri_floor` are columns of the single row computed by `genomic_range_index_levels('exons')`.

Alternatively, your program might first query `genomic_range_index_levels()` alone, then pass the bounds in to subsequent prepared queries, e.g. in Python:

```python3
(gri_ceiling, gri_floor) = next(
    con.execute("SELECT * FROM genomic_range_index_levels('exons')")
  )
for (queryChrom, queryBegin, queryEnd) in queryRanges:
  exons = list(
    con.execute(
      "SELECT * from exons WHERE exons._rowid_ IN \
        genomic_range_rowids('exons',?,?,?,?,?)",
      (queryChrom, queryBegin, queryEnd, gri_ceiling, gri_floor)
    )
  )
  ...
```

This bounds detection procedure has a small cost, which will be worthwhile if used to optimize many subsequent GRI queries (but possibly not if just for a few).

**❗ The bounds should be redetected if the min/max feature length may have been changed by inserts or updates to the table. GRI queries with incorrect bounds are liable to produce incomplete results.**

Omitting the bounds is always safe, albeit slower. <small>Instead of detecting current bounds, they can be figured manually as follows. Set the integer ceiling to *C*, 0 &lt; *C* &lt; 16, such that all (present & future) indexed features are guaranteed to have lengths &le;16<sup>*C*</sup>. For example, if you're querying features on the human genome, then you can set ceiling=7 because the lengthiest chromosome sequence is &lt;16<sup>7</sup>nt. Set the integer floor *F* to (i) the floor value supplied at GRI creation, if any; (ii) *F* &gt; 0 such that the minimum possible feature length &gt;16<sup>*F*-1</sup>, if any; or (iii) zero. The default, safe, albeit slower bounds are C=15, F=0.</small>

#### Joining tables on range overlap

Suppose we have two tables with genomic features to join on range overlap. Only the "right-hand" table must have a GRI; preferably the smaller of the two. For example, annotating a table of variants with the surrounding exon(s), if any:

``` sql
SELECT variants.*, exons._rowid_
FROM genomic_range_index_levels('exons'),
     variants LEFT JOIN exons ON exons._rowid_ IN
  genomic_range_rowids(
    'exons',
    variants.chrom,
    variants.beginPos,
    variants.endPos,
    _gri_ceiling,
    _gri_floor
  )
```

We fill out the GRI query range using the three coordinate columns of the variants table. The level bounds optimization is highly desirable for the "tight loop" of GRI queries during a join. See also "Advice for big data" below.

### Reference genome metadata

The following routines support the aforementioned, recommended convention for storing a `_gri_refseq` table with information about the genomic reference sequences, which other tables can cross-reference by integer ID (rid) instead of storing textual chromosome names. The columns of `_gri_refseq` include:

1. `_gri_rid INTEGER PRIMARY KEY`
2. `gri_refseq_name TEXT NOT NULL`
3. `gri_refseq_length INTEGER NOT NULL`
4. `gri_assembly TEXT` genome assembly name (optional)
5. `gri_refget_id TEXT` [refget](http://samtools.github.io/hts-specs/refget.html) sequence ID (optional)
6. `gri_refseq_meta_json TEXT DEFAULT '{}'` JSON object with arbitrary metadata

**↪ Put Reference Assembly SQL:** *Generate a string* containing a series of SQL statements which *when executed* creates `_gri_refseq` and populates it with information about a reference assembly whose details are bundled into the extension.

=== "Python"
    ``` python3
    refseq_sql = genomicsqlite.put_reference_assembly_sql(
      dbconn, 'GRCh38_no_alt_analysis_set'
    )
    dbconn.executescript(refseq_sql)
    ```

=== "Java"
    ```java
    String refSql = GenomicSQLite.putReferenceAssemblySQL(
      dbconn, "GRCh38_no_alt_analysis_set"
    );
    dbconn.createStatement().executeUpdate(refSql);
    ```

=== "SQLiteCpp"
    ``` c++
    std::string PutGenomicReferenceAssemblySQL(
      const std::string &assembly,
      const std::string &attached_schema = ""
    );

    // SQLite::Database* dbconn in a transaction
    dbconn->exec(PutGenomicReferenceAssemblySQL("GRCh38_no_alt_analysis_set"));
    ```

=== "C++"
    ``` c++
    std::string PutGenomicReferenceAssemblySQL(
      const std::string &assembly,
      const std::string &attached_schema = ""
    );

    std::string refseq_sql = PutGenomicReferenceAssemblySQL(
      "GRCh38_no_alt_analysis_set"
    );
    // sqlite3* dbconn in a transaction
    char* errmsg = nullptr;
    int rc = sqlite3_exec(dbconn, refseq_sql.c_str(), nullptr, nullptr, &errmsg);
    // check rc, free errmsg
    ```

=== "C"
    ``` c
    char* put_genomic_reference_assembly_sql(
      const char *assembly,
      const char *attached_schema
    );

    char* refseq_sql = put_genomic_reference_assembly_sql(
      "GRCh38_no_alt_analysis_set", nullptr
    );
    if (*refseq_sql) {
      char* errmsg = 0;
      /* sqlite3* dbconn in a transaction */
      int rc = sqlite3_exec(dbconn, refseq_sql, 0, 0, &errmsg);
      /* check rc, free errmsg */
    } else {
      /* see calling convention discussed in previous examples */
    }
    sqlite3_free(refseq_sql);
    ```

Available assemblies:

* `GRCh38_no_alt_analysis_set`

**↪ Put Reference Sequence SQL:** *Generate a string* containing a series of SQL statements which *when executed* creates `_gri_refseq` (if it doesn't exist) and adds *one* reference sequence with supplied attributes.

=== "Python"
    ``` python3
    refseq_sql = genomicsqlite.put_reference_sequence_sql(
      dbconn, 'chr17', 83257441
      # optional: assembly, refget_id, meta (dict), rid
    )
    dbconn.executescript(refseq_sql)
    ```

=== "Java"
    ```java
    String refSql = GenomicSQLite.putReferenceSequenceSQL(
      dbconn, "chr17", 83257441L
      // optional overloads:
      // String assembly, String refget_id, String meta_json, long rid
    );
    dbconn.createStatement().executeUpdate(refSql);
    ```

=== "SQLiteCpp"
    ``` c++
    std::string PutGenomicReferenceSequenceSQL(
      const std::string &name,
      sqlite3_int64 length,
      const std::string &assembly = "",
      const std::string &refget_id = "",
      const std::string &meta_json = "{}",
      sqlite3_int64 rid = -1,
      const std::string &attached_schema = ""
    );

    // SQLite::Database* dbconn in a transaction
    dbconn->exec(PutGenomicReferenceSequenceSQL("chr17", 83257441));
    ```

=== "C++"
    ``` c++
    std::string PutGenomicReferenceSequenceSQL(
      const std::string &name,
      sqlite3_int64 length,
      const std::string &assembly = "",
      const std::string &refget_id = "",
      const std::string &meta_json = "{}",
      sqlite3_int64 rid = -1,
      const std::string &attached_schema = ""
    );

    std::string refseq_sql = PutGenomicReferenceAssemblySQL(
      "chr17", 83257441
    );
    // sqlite3* dbconn in a transaction
    char* errmsg = nullptr;
    int rc = sqlite3_exec(dbconn, refseq_sql.c_str(), nullptr, nullptr, &errmsg);
    // check rc, free errmsg
    ```

=== "C"
    ``` c
    char* put_genomic_reference_sequence_sql(
      const char *name,
      sqlite3_int64 length,
      const char *assembly,
      const char *refget_id,
      const char *meta_json,
      sqlite3_int64 rid,
      const char *attached_schema
    );

    char* refseq_sql = put_genomic_reference_sequence_sql(
      "chr17", 83257441, 0, 0, 0, -1, 0
    );
    if (*refseq_sql) {
      char* errmsg = 0;
      /* sqlite3* dbconn in a transaction */
      int rc = sqlite3_exec(dbconn, refseq_sql, 0, 0, &errmsg);
      /* check rc, free errmsg */
    } else {
      /* see calling convention discussed in previous examples */
    }
    sqlite3_free(refseq_sql);
    ```

If the `rid` argument is omitted or -1 then it will be assigned automatically upon insertion.

**↪ Get Reference Sequences by Rid:** create an in-memory lookup table of the previously-stored reference information, keyed by rid integer. Assumes the stored information is read-only by this point. This table is for the application code's convenience to read tables that use the rid convention. Such uses can be also be served by SQL join on the `_gri_refseq` table (see Cookbook).

=== "Python"
    ``` python3
    class ReferenceSequence(NamedTuple):
      rid: int
      name: str
      length: int
      assembly: Optional[str]
      refget_id: Optional[str]
      meta: Dict[str, Any]

    refseq_by_rid = genomicsqlite.get_reference_sequences_by_rid(dbconn)
    # refseq_by_rid: Dict[int, ReferenceSequence]
    ```

=== "Java"
    ```java
    import java.util.HashMap;
    import net.mlin.genomicsqlite.ReferenceSequence;
    /*
    public class ReferenceSequence {
      public final long rid, length;
      public final String name, assembly, refgetId, metaJson;
    }
    */
    HashMap<Long, ReferenceSequence> refseqByRid
      = GenomicSQLite.getReferenceSequencesByRid(dbconn);
    ```

=== "SQLiteCpp"
    ``` c++
    struct gri_refseq_t {
      long long rid, length;
      std::string name, assembly, refget_id, meta_json;
    };
    std::map<long long, gri_refseq_t> GetGenomicReferenceSequencesByRid(
      sqlite3 *dbconn,
      const std::string &assembly = "",
      const std::string &attached_schema = ""
    );

    // SQLite::Database* dbconn
    auto refseq_by_rid = GetGenomicReferenceSequencesByRid(dbconn->getHandle());
    ```

=== "C++"
    ``` c++
    struct gri_refseq_t {
      long long rid, length;
      std::string name, assembly, refget_id, meta_json;
    };
    std::map<long long, gri_refseq_t> GetGenomicReferenceSequencesByRid(
      sqlite3 *dbconn,
      const std::string &assembly = "",
      const std::string &attached_schema = ""
    );

    // sqlite3* dbconn
    auto refseq_by_rid = GetGenomicReferenceSequencesByRid(dbconn);
    ```

=== "C"
    ``` c
    /* Omitted for want of idiomatic map type; pull requests welcome! */
    ```

The optional `assembly` argument restricts the retrieved sequences to those with matching `gri_assembly` value. However, mixing different assemblies in `_gri_refseq` is not recommended.

**↪ Get Reference Sequences by Name:** create an in-memory lookup table of the previously-stored reference information, keyed by sequence name. Assumes the stored information is read-only by this point. This table is for the application code's convenience to translate name to rid whilst formulating queries or inserting features from a text source.

=== "Python"
    ``` python3
    class ReferenceSequence(NamedTuple):
      rid: int
      name: str
      length: int
      assembly: Optional[str]
      refget_id: Optional[str]
      meta: Dict[str, Any]

    refseq_by_name = genomicsqlite.get_reference_sequences_by_name(dbconn)
    # refseq_by_name: Dict[str, ReferenceSequence]
    ```

=== "Java"
    ```java
    import java.util.HashMap;
    import net.mlin.genomicsqlite.ReferenceSequence;
    /*
    public class ReferenceSequence {
      public final long rid, length;
      public final String name, assembly, refgetId, metaJson;
    }
    */
    HashMap<String, ReferenceSequence> refseqByName
      = GenomicSQLite.getReferenceSequencesByName(dbconn);
    ```

=== "SQLiteCpp"
    ``` c++
    struct gri_refseq_t {
      long long rid, length;
      std::string name, assembly, refget_id, meta_json;
    };
    std::map<std::string, gri_refseq_t> GetGenomicReferenceSequencesByName(
      sqlite3 *dbconn,
      const std::string &assembly = "",
      const std::string &attached_schema = ""
    );

    // SQLite::Database* dbconn
    auto refseq_by_name = GetGenomicReferenceSequencesByName(dbconn->getHandle());
    ```

=== "C++"
    ``` c++
    struct gri_refseq_t {
      long long rid, length;
      std::string name, assembly, refget_id, meta_json;
    };
    std::map<std::string, gri_refseq_t> GetGenomicReferenceSequencesByName(
      sqlite3 *dbconn,
      const std::string &assembly = "",
      const std::string &attached_schema = ""
    );

    // sqlite3* dbconn
    auto refseq_by_name = GetGenomicReferenceSequencesByName(dbconn);
    ```

=== "C"
    ``` c
    /* Omitted for want of idiomatic map type; pull requests welcome! */
    ```

### Cookbook

#### rid to chromosome name

Table identifies each feature's chromosome by rid, and we want to see them with text chromosome names.

``` sql
SELECT gri_refseq_name, feature_table.*
  FROM feature_table NATURAL JOIN _gri_refseq
```

The join key here is `_gri_rid`, which is one of the generated columns added by GRI creation.

Alternatively, the application code can read rid from the row and translate it using the lookup table generated by the **Get Reference Sequences by Rid** routine.

#### Query rid using chromosome name

We're making a GRI query on a table that stores rid integers, but our query range has a chromosome name.

``` sql
SELECT feature_table.* FROM
  (SELECT _gri_rid AS rid FROM _gri_refseq
    WHERE gri_refseq_name='chr12') AS query, feature_table
  WHERE feature_table._rowid_ IN
    genomic_range_rowids('feature_table',query.rid,111803912,111804012)
```

We use a subquery to look up the rid corresponding to the known chromosome name. Alternatively, the application code can first convert the query name to rid using the lookup table generated by the **Get Reference Sequences by Name** routine.

#### Circular chromosome query

On circular chromosomes, range queries should include features that wrap around the origin to end inside the desired range. If we've stored them naively with featureEnd = featureBegin + featureLength, then we can build a unified query with reference to the stored chromosome lengths:

``` sql
SELECT col1, col2, ... FROM
  (SELECT gri_refseq_length FROM _gri_refseq WHERE _gri_rid = queryRid),
  featureTable WHERE featureTable._rowid_ IN
  (genomic_range_rowids('featureTable', queryRid, queryBegin, queryEnd)
   UNION
   genomic_range_rowids(
     'featureTable',
     queryRid,
     gri_refseq_length+queryBegin,
     gri_refseq_length+queryEnd))
```

We query a second range beyond the chromosome length, which will match features that wrap around into the query. `UNION` deduplicates the result rowids.

As a convention, set `"circular": true` in the `_gri_refseq.gri_refseq_meta_json` for circular chromosomes.

### Advice for big data

A SQLite table's rowid order indicates its physical storage layout. It's therefore preferable for a mainly-GRI-queried table to have had its rows originally inserted in genomic range order, so that the features' (chromosome, beginPosition) monotonically increase with rowid, and range queries enjoy storage/cache locality. While not required in theory, this may be needed in practice for GRI queries that will match a material fraction of a big table's rows.

You can sort the rows of an existing table into a new table with the same schema, with something like `INSERT INTO sorted SELECT * FROM original NOT INDEXED ORDER BY chromosome, beginPosition`. The Genomics Extension enables SQLite's [parallel, external merge-sorter](https://sqlite.org/src/file/src/vdbesort.c) to execute this efficiently; still, if it's feasible to load sorted data upfront, so much the better.

<small>
Note 1. Make sure [SQLite is configured](https://www.sqlite.org/tempfiles.html) to use a suitable storage subsystem for big temporary files generated whilst sorting. `NOT INDEXED` forces SQLite to use the external sorter instead of some index that'd mislead it into reading the entire table in a shuffled order.

Note 2. The "original" table should come from a separate [attached database](https://www.sqlite.org/lang_attach.html) to avoid `DROP TABLE original` from the final database, which is costly due to the need to defragment afterwards.
</small>

A series of many GRI queries (including in the course of a join) should also proceed in genomic range order. If this isn't possible, then ideally the database page cache should be enlarged to fit the entire indexed table in memory.

If you expect a GRI query to yield a very large, contiguous rowid result set (e.g. all features on a chromosome, in a table *known* to be range-sorted), then the following specialized query plan may be advantageous:

1. Ask GRI for *first* relevant rowid, `SELECT MIN(_rowid_) AS firstRowid FROM genomic_range_rowids(...)`
2. Open a cursor on `SELECT ... FROM tableName WHERE _rowid_ >= firstRowid`
3. Loop through rows for as long as they're relevant.

But this plan strongly depends on the contiguity assumption.

## Other routines

#### Attach GenomicSQLite database

**↪ GenomicSQLite Attach:** *Generate a string* containing a series of SQL statements to execute on an existing database connection in order to [ATTACH](https://www.sqlite.org/lang_attach.html) a GenomicSQLite database under a given schema name. The main connection may be a plain, uncompressed SQLite3 database, as long as (i) it was opened with the `SQLITE_OPEN_URI` flag or language equivalent and (ii) the Genomics Extension is loaded in the executing program.

=== "Python"
    ``` python3
    dbconn = sqlite3.connect('any.db', uri=True)
    attach_sql = genomicsqlite.attach_sql(dbconn, 'compressed.db', 'db2')
    # attach_sql() also takes configuration keyword arguments like
    # genomicsqlite.connect()
    dbconn.executescript(attach_sql)
    # compressed.db now attached as db2
    ```

=== "Java"
    ```java
    // If needed, no-op to trigger initial load of Genomics Extension:
    DriverManager.getConnection("jdbc:genomicsqlite::memory:");

    Connection dbconn = DriverManager.getConnection("jdbc:sqlite:any.db");
    String attachSql = GenomicSQLite.attachSQL(dbconn, "compressed.db", "db2", "{}");
    //                                                             config_json ^^^^
    dbconn.createStatement().executeUpdate(attachSql);
    // compressed.db now attached as db2
    ```

=== "SQLiteCpp"
    ``` c++
    std::string GenomicSQLiteAttachSQL(
      const std::string &dbfile,
      const std::string &schema_name,
      const std::string &config_json = "{}"
    );

    std::string attach_sql = GenomicSQLiteAttachSQL("compressed.db", "db2");
    SQLite::Database dbconn("any.db", SQLITE_OPEN_URI);
    dbconn.exec(attach_sql);
    // compressed.db now attached as db2
    ```

=== "C++"
    ``` c++
    std::string GenomicSQLiteAttachSQL(
      const std::string &dbfile,
      const std::string &schema_name,
      const std::string &config_json = "{}"
    );

    std::string attach_sql = GenomicSQLiteAttachSQL("compressed.db", "db2");
    // sqlite3* dbconn opened using sqlite3_open_v2() on some db
    //   with SQLITE_OPEN_URI
    char* errmsg = nullptr;
    int rc = sqlite3_exec(dbconn, attach_sql.c_str(), nullptr, nullptr, &errmsg);
    // check rc, free errmsg

    // compressed.db now attached as db2
    ```

=== "C"
    ``` c
    char* genomicsqlite_attach_sql(
      const char *dbfile,
      const char *schema_name,
      const char *config_json
    );

    char* attach_sql = genomicsqlite_attach_sql("compressed.db", "db2", "{}");
    if (*attach_sql) {
      char* errmsg = 0;
      /* sqlite3* dbconn opened using sqlite3_open_v2() on some db
       * with SQLITE_OPEN_URI */
      int rc = sqlite3_exec(dbconn, attach_sql, 0, 0, &errmsg);
      /* check rc, free errmsg */
    } else {
     /* see calling convention discussed in previous examples */
    }
    sqlite3_free(attach_sql);

    /* compressed.db now attached as db2 */
    ```

❗ The file and schema names are textually pasted into a template SQL script. Take care to prevent SQL injection, if they're in any way determined by external input.

#### Compress existing SQLite3 database

**↪ GenomicSQLite Vacuum Into:** *Generate a string* containing a series of SQL statements to execute on an existing database in order to copy it into a new compressed & [defragmented](https://www.sqlite.org/lang_vacuum.html) file. The source database may be a plain, uncompressed SQLite3 database, as long as (i) it was opened with the `SQLITE_OPEN_URI` flag or language equivalent and (ii) the Genomics Extension is loaded in the executing program.

=== "Python"
    ``` python3
    dbconn = sqlite3.connect('existing.db', uri=True)
    vacuum_sql = genomicsqlite.vacuum_into_sql(dbconn, 'compressed.db')
    # vacuum_into_sql() also takes configuration keyword arguments like
    # genomicsqlite.connect() to control compression level & page sizes

    dbconn.executescript(vacuum_sql)
    dbconn2 = genomicsqlite.connect('compressed.db')
    ```

=== "Java"
    ```java
    // If needed, no-op to trigger initial load of Genomics Extension:
    DriverManager.getConnection("jdbc:genomicsqlite::memory:");

    Connection dbconn = DriverManager.getConnection("jdbc:sqlite:existing.db");
    String vacuumSql = GenomicSQLite.vacuumIntoSQL(dbconn, "compressed.db", "{}");
    //                                                          config_json ^^^^
    dbconn.createStatement().executeUpdate(vaccumSql);
    Connection dbconn2 = DriverManager.getConnection(
      "jdbc:genomicsqlite:compressed.db"
    );
    ```

=== "SQLiteCpp"
    ``` c++
    std::string GenomicSQLiteVacuumIntoSQL(
      const std::string &dest_filename,
      const std::string &config_json = "{}"
    );

    std::string vacuum_sql = GenomicSQLiteVacuumIntoSQL("compressed.db");
    SQLite::Database dbconn("existing.db", SQLITE_OPEN_READONLY | SQLITE_OPEN_URI);
    dbconn.exec(vacuum_sql);
    auto dbconn2 = GenomicSQLiteOpen("compressed.db");
    ```

=== "C++"
    ``` c++
    std::string GenomicSQLiteVacuumIntoSQL(
      const std::string &dest_filename,
      const std::string &config_json = "{}"
    );

    std::string vacuum_sql = GenomicSQLiteVacuumIntoSQL("compressed.db");
    // sqlite3* dbconn opened using sqlite3_open_v2() on some existing.db
    //   with SQLITE_OPEN_URI
    char* errmsg = nullptr;
    int rc = sqlite3_exec(dbconn, vacuum_sql.c_str(), nullptr, nullptr, &errmsg);
    // check rc, free errmsg

    // rc = GenomicSQLiteOpen("compressed.db", ...);
    ```

=== "C"
    ``` c
    char* genomicsqlite_vacuum_into_sql(
      const char *dest_filename,
      const char *config_json
    );

    char* vacuum_sql = genomicsqlite_vacuum_into_sql("compressed.db", "{}");
    if (*vacuum_sql) {
      char* errmsg = 0;
      /* sqlite3* dbconn opened using sqlite3_open_v2() on some existing.db
       * with SQLITE_OPEN_URI */
      int rc = sqlite3_exec(dbconn, vacuum_sql, 0, 0, &errmsg);
      /* check rc, free errmsg */
    } else {
     /* see calling convention discussed in previous examples */
    }
    sqlite3_free(vacuum_sql);

    /* genomicsqlite_open("compressed.db", ...); */
    ```

#### Genomics Extension version

**↪ GenomicSQLite Version**

=== "SQL"
    ``` sql
    SELECT genomicsqlite_version()
    ```

=== "Python"
    ``` python3
    genomicsqlite.__version__
    ```

=== "Java"
    ```java
    String genomicsqliteVersion = GenomicSQLite.version(dbconn);
    ```

=== "C++"
    ``` c++
    std::string GenomicSQLiteVersion();
    ```

=== "C"
    ``` c
    char* genomicsqlite_version();
    /* result to be sqlite3_free() */
    ```

#### JSON functions

The Genomics Extension bundles the SQLite developers' [JSON1 extension](https://www.sqlite.org/json1.html) and enables it automatically. The following conventions are recommended,

1. JSON object columns should be named *_json with type `TEXT DEFAULT '{}'`.
2. JSON array columns should be named *_jsarray with type `TEXT DEFAULT '[]'`.

The JSON1 functions can be used with [generated columns](https://sqlite.org/gencol.html) to effectively enable indices on JSON-embedded fields.

## genomicsqlite interactive shell

The Python package includes a `genomicsqlite` script that starts the [`sqlite3` interactive shell](https://sqlite.org/cli.html) with the Genomics Extension enabled. Simply invoke,

```
$ genomicsqlite /path/to/compressed.db [-readonly]
```

to enter the SQL prompt with the database open. Or, add an SQL statement (in quotes) to perform and exit. If you've installed the Python package but the script isn't found, you probably need to augment your `PATH` with the directory for Python console scripts.
