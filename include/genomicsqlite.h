#include <stddef.h>
#ifndef SQLITE3EXT_H
#include <sqlite3.h>
#endif

/*
 * C bindings
 *
 * Convention for all functions returning char*:
 * - Return value is always a non-null buffer which must be freed using sqlite3_free()
 * - On success, it's a nonempty, null-terminated string
 * - On failure, it's a null byte, followed immediately by a null-terminated error message
 */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Return the GenomicSQLite version.
 */
char *genomicsqlite_version();

/*
 * Get configuration defaults.
 */
char *genomicsqlite_default_config_json();

/* Prior to using genomicsqlite_open(), initialize the library as follows:
 *   char *zErrMsg = 0;
 *   int rc = GENOMICSQLITE_C_INIT(&zErrMsg);
 *   if (rc != SQLITE_OK) {
 *     ...
 *     sqlite3_free(zErrMsg);
 *   }
 *   ...
 */
int genomicsqlite_init(int (*)(const char *, sqlite3 **, int, const char *),
                       int (*)(sqlite3 *, int),
                       int (*)(sqlite3 *, const char *, const char *, char **), char **pzErrMsg);
#define GENOMICSQLITE_C_INIT(pzErrMsg)                                                             \
    genomicsqlite_init(sqlite3_open_v2, sqlite3_enable_load_extension, sqlite3_load_extension,     \
                       pzErrMsg);

/*
 * Wrap sqlite3_open() and initialize the "connection" for use with GenomicSQLite. config_json if
 * supplied, will be merged into defaults (i.e. it's not necessary to include defaults)
 */
int genomicsqlite_open(const char *dbfile, sqlite3 **ppDb, char **pzErrMsg, int flags,
                       const char *config_json);

/*
 * Generate SQL script to run on existing SQLite connection (not necessarily GenomicSQLite) to
 * attach a GenomicSQLite database file under the given schema name, with given configuration. The
 * connection must have been opened with the SQLITE_OPEN_URI flag or equivalent.
 */
char *genomicsqlite_attach_sql(const char *dbfile, const char *schema_name,
                               const char *config_json);

/*
 * Generate SQL script to run on existing SQLite database (not necessarily GenomicSQLite) to cause
 * creation of a defragmented & GenomicSQLite-compressed copy. The connection must have been opened
 * with the SQLITE_OPEN_URI flag or equivalent.
 */
char *genomicsqlite_vacuum_into_sql(const char *destfile, const char *config_json);

/*
 * Generate SQL script to create a genomic range index on the specified table.
 * rid: column name for the reference sequence (chromosome) ID of each row
 * beg: column name or simple SQL expression for the interval begin position
 * end: column name or simple SQL expression for the interval end position
 */
char *create_genomic_range_index_sql(const char *table, const char *rid, const char *beg,
                                     const char *end, int floor);

/*
 * Generate parenthesized SELECT statement to query the existing genomic range index of the
 * specified table. The query finds table rows which overlap the query range, producing one column
 * of _rowid_'s.
 * dbconn: If provided, the procedure first inspects the index to collect information that may
 *         allow it to optimize the generated query SQL. Recommended if the generated SQL will be
 *         used repeatedly, which is the typical case.
 *         A query optimized in this way must not be used after rows are added or updated, as it's
 *         then liable to produce incomplete results. The SQL should be regenerated after such
 *         changes.
 *         Alternatively, if dbconn is NULL then a less-efficient version of the query is generated
 *         which is safe to use across updates (also saving the small upfront cost of inspection).
 * qrid: query reference sequence (chromosome) ID; defaults to "?1" i.e. the first parameter of the
 *       compiled statement. One could substitute a different parameter, a constant value, or a SQL
 *       expression referring to columns of a joined table.
 * qbeg: query range begin position; defaults to "?2"
 * qend: query range end position; defaults to "?3"
 */
char *genomic_range_rowids_sql(sqlite3 *dbconn, const char *indexed_table, const char *qrid,
                               const char *qbeg, const char *qend, int ceiling, int floor);

/*
 * Optional storage of refrence sequence metadata
 */
char *put_genomic_reference_assembly_sql(const char *assembly, const char *attached_schema);
char *put_genomic_reference_sequence_sql(const char *name, sqlite3_int64 length,
                                         const char *assembly, const char *refget_id,
                                         const char *meta_json, sqlite3_int64 rid,
                                         const char *attached_schema);

/*
 * Low-level routines for two-bit nucleotide encoding (normally used via SQL functions, but
 * available to C FFI callers here)
 */

/*
 * Two-bit encode the nucleotide character sequence of specified length. The output byte count is
 * a function of len as follows: len==0 => 0, len==1 => 1, else => (len+7)/4.
 * Returns:
 *  n >= 0: success; wrote n bytes
 *      -1: encountered non-nucleotide ASCII character
 *      -2: encountered non-ASCII character (e.g. UTF-8) or NUL
 */
int nucleotides_twobit(const char *seq, size_t len, void *out);

/*
 * Given two-bit-encoded blob pointer & size, compute original nucleotide sequence length
 */
size_t twobit_length(const void *data, size_t sz);

/*
 * Given two-bit-encoded blob, decode the nucleotide subsequence [ofs, ofs+len). To get the whole
 * sequence, set ofs=0 & len=twobit_length(data, datasize). Caller must ensure that:
 * 1. ofs+len <= twobit_length(data, datasize)
 * 2. out is preallocated len+1 bytes (a null terminator will be affixed)
 */
void twobit_dna(const void *data, size_t sz, size_t ofs, size_t len, char *out);
void twobit_rna(const void *data, size_t sz, size_t ofs, size_t len, char *out);

/* Reverse-complement DNA sequence. The out buffer must be preallocated len+1 bytes (a null
 * terminator will be affixed).
 * Returns:
 *       0: success
 *      -1: encountered non-DNA character
 */
int dna_revcomp(const char *dna, size_t len, char *out);

/*
 * C++ bindings: are liable to throw exceptions except where marked noexcept
 */
#ifdef __cplusplus
}

#include <map>
#include <string>

std::string GenomicSQLiteVersion();
std::string GenomicSQLiteDefaultConfigJSON();

/* Prior to using GenomicSQLiteOpen(), initialize the library as follows:
 *   try {
 *     GENOMICSQLITE_CXX_INIT();
 *   } catch (std::runtime_error &exn) {
 *     ...
 *   }
 */
void GenomicSQLiteInit(int (*open_v2)(const char *, sqlite3 **, int, const char *),
                       int (*enable_load_extension)(sqlite3 *, int),
                       int (*load_extension)(sqlite3 *, const char *, const char *, char **));
#define GENOMICSQLITE_CXX_INIT()                                                                   \
    GenomicSQLiteInit(sqlite3_open_v2, sqlite3_enable_load_extension, sqlite3_load_extension);

int GenomicSQLiteOpen(const std::string &dbfile, sqlite3 **ppDb, std::string &errmsg_out,
                      int flags = 0, const std::string &config_json = "{}") noexcept;
#ifdef SQLITECPP_VERSION
/*
 * For use with SQLiteCpp -- https://github.com/SRombauts/SQLiteCpp
 * (include SQLiteCpp/SQLiteCpp.h first)
 */
#include <memory>
std::unique_ptr<SQLite::Database> GenomicSQLiteOpen(const std::string &dbfile, int flags = 0,
                                                    const std::string &config_json = "{}");
#endif

std::string GenomicSQLiteAttachSQL(const std::string &dbfile, const std::string &schema_name,
                                   const std::string &config_json = "{}");

std::string GenomicSQLiteVacuumIntoSQL(const std::string &dbfile,
                                       const std::string &config_json = "{}");

std::string CreateGenomicRangeIndexSQL(const std::string &table, const std::string &rid,
                                       const std::string &beg, const std::string &end,
                                       int floor = 0);
std::string GenomicRangeRowidsSQL(sqlite3 *dbconn, const std::string &indexed_table,
                                  const std::string &qrid = "?1", const std::string &qbeg = "?2",
                                  const std::string &qend = "?3", int ceiling = -1, int floor = -1);

std::string PutGenomicReferenceAssemblySQL(const std::string &assembly,
                                           const std::string &attached_schema = "");
std::string PutGenomicReferenceSequenceSQL(const std::string &name, sqlite3_int64 length,
                                           const std::string &assembly = "",
                                           const std::string &refget_id = "",
                                           const std::string &meta_json = "{}",
                                           sqlite3_int64 rid = -1,
                                           const std::string &attached_schema = "");

/* Lookup helpers for stored reference sequence metadata (assumes it's finalized) */
struct gri_refseq_t {
    long long rid, length;
    std::string name, assembly, refget_id, meta_json = "{}";
};
std::map<long long, gri_refseq_t>
GetGenomicReferenceSequencesByRid(sqlite3 *dbconn, const std::string &assembly = "",
                                  const std::string &attached_schema = "");
std::map<std::string, gri_refseq_t>
GetGenomicReferenceSequencesByName(sqlite3 *dbconn, const std::string &assembly = "",
                                   const std::string &attached_schema = "");

/* implementation underlying parse_genomic_range_{sequence,begin,end} */
std::tuple<std::string, uint64_t, uint64_t> parse_genomic_range(const std::string &txt);
#endif
