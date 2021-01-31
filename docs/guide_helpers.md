# Programming Guide - Useful Routines

#### DNA reverse complement

Reverse-complements a DNA text value (containing only characters from the set `AGCTagct`), preserving original case.

=== "SQL"
    ``` sql
    SELECT dna_revcomp('AGCTagct')  -- 'agctAGCT'
    ```

Given NULL, returns NULL. Any other input is an error.

#### Parse genomic range text

These SQL functions process a text value like `'chr1:2,345-6,789'` into its three parts (sequence/chromosome name, begin position, and end position).

=== "SQL"
    ``` sql
    SELECT parse_genomic_range_sequence('chr1:2,345-6,789')  -- 'chr1'
    SELECT parse_genomic_range_begin('chr1:2,345-6,789')     -- 2344 (!)
    SELECT parse_genomic_range_end('chr1:2,345-6,789')       -- 6789
    ```

❗ Since such text ranges are conventionally one-based and closed, `parse_genomic_range_begin()` effectively converts them to zero-based and half-open by [returning one less than the text begin position](https://genome.ucsc.edu/FAQ/FAQtracks#tracks1).

Given NULL, each function returns NULL. An error is raised if the text value can't be parsed, or for any other input type.

#### Two-bit encoding for nucleotide sequences

The extension supplies SQL functions to pack a DNA/RNA sequence TEXT value into a smaller BLOB value, using two bits per nucleotide. (Review [SQLite Datatypes](https://www.sqlite.org/datatype3.html) on the important differences between TEXT and BLOB values & columns.) Storing a large database of sequences using such BLOBs instead of TEXT can improve application I/O efficiency, with up to 4X more nucleotides cached in the same memory space. It is not, however, expected to greatly shrink the database file on disk, owing to the automatic storage compression.

The encoding is case-insensitive and considers `T` and `U` equivalent.

*Encoding:*

=== "SQL"
    ``` sql
    SELECT nucleotides_twobit('TCAG')
    ```

Given a TEXT value consisting of characters from the set `ACGTUacgtu`, compute a two-bit-encoded BLOB value that can later be decoded using `twobit_dna()` or `twobit_rna()`. Given any other ASCII TEXT value (including empty), pass it through unchanged as TEXT. Given NULL, return NULL. Any other input is an error.

Typically used to populate a BLOB column `C` with e.g.

```sql
INSERT INTO some_table(...,C) VALUES(...,nucleotides_twobit(?))
```

This works even if some of the sequences contain `N`s or other characters, in which case those sequences are stored as the original TEXT values. Make sure the column has schema type `BLOB` to avoid spurious coercions, and by convention, the column should be named *_twobit.

*Decoding:*

=== "SQL"
    ``` sql
    SELECT twobit_dna(nucleotides_twobit('TCAG'))
    SELECT twobit_rna(nucleotides_twobit('UCAG'))
    SELECT twobit_dna(nucleotides_twobit('TCAG'),Y,Z)
    SELECT twobit_rna(nucleotides_twobit('UCAG'),Y,Z)
    ```

Given a two-bit-encoded BLOB value, decode the nucleotide sequence as uppercased TEXT, with `T`'s for `twobit_dna()` and `U`'s for `twobit_rna()`. Given a TEXT value, pass it through unchanged. Given NULL, return NULL. Any other first input is an error.

The optional `Y` and `Z` arguments can be used to compute [`substr(twobit_dna(X),Y,Z)`](https://sqlite.org/lang_corefunc.html#substr) more efficiently, without decoding the whole sequence. <small>Unfortunately however, [SQLite internals](https://sqlite.org/forum/forumpost/756c1a1e48?t=h) make this operation still liable to use time & memory proportional to the full length of X, not Z. If frequent random access into long sequences is needed, then consider splitting them across multiple rows.</small>

Take care to only use BLOBs originally produced by `nucleotides_twobit()`, as other BLOBs may decode to spurious nucleotide sequences. If you `SELECT twobit_dna(C) FROM some_table` on a column with mixed BLOB and TEXT values as suggested above, note that the results actually stored as TEXT preserve their case and T/U letters, unlike decoded BLOBs.

*Length:*

=== "SQL"
    ``` sql
    SELECT twobit_length(dna_twobit('TCAG'))
    ```

Given a two-bit-encoded BLOB value, return the length of the *decoded* sequence (without actually decoding it). This is *not* equal to `4*length(BLOB)` due to padding.

Given a TEXT value, return its byte length. Given NULL, return NULL. Any other input is an error.

#### JSON1 and UINT extensions

The Genomics Extension bundles the SQLite developers' [JSON1 extension](https://www.sqlite.org/json1.html) and enables it automatically. By convention, JSON object columns should be named \*_json and JSON array columns should be named \*_jsarray. The JSON1 functions can be used with [generated columns](https://sqlite.org/gencol.html) to effectively allow indexing of JSON-embedded fields.

The [UINT collating sequence](https://www.sqlite.org/uintcseq.html) is also bundled. This can be useful to make e.g. `ORDER BY chromosome COLLATE UINT` put 'chr2' before 'chr10'.

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

=== "Rust"
    ```rust
    let genomicsqlite_options = json::object::Object::new();
    // If needed, trigger initial load of Genomics Extension prior to
    // opening other connections
    let nop = genomicsqlite::open(
      ":memory:",
      OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
      &genomicsqlite_options
    )?;

    // dbconn: rusqlite::Connection with SQLITE_OPEN_URI
    let attach_sql = dbconn
      .genomicsqlite_attach_sql("compressed.db", "db2",
                                &genomicsqlite_options)?;
    dbconn.execute_batch(&attach_sql)?;
    // compressed.db is now attached as db2
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


=== "Rust"
    ```rust
    let genomicsqlite_options = json::object::Object::new();
    // If needed, trigger initial load of Genomics Extension prior to
    // opening other connections
    let nop = genomicsqlite::open(
      ":memory:",
      OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
      &genomicsqlite_options
    )?;

    // dbconn: rusqlite::Connection with SQLITE_OPEN_URI
    let vacuum_sql = dbconn
      .genomicsqlite_vacuum_into_sql("compressed.db",
                                     &genomicsqlite_options)?;
    dbconn.execute_batch(&vacuum_sql)?;
    let dbconn2 = genomicsqlite::open(
      "compressed.db",
      OpenFlags::SQLITE_OPEN_READ_WRITE,
      &genomicsqlite_options
    )?;
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

=== "Rust"
    ```rust
    let v: String = dbconn.genomicsqlite_version();
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
