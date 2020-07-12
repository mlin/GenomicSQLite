#include <sqlite3.h>

#define GRI_MAX_POS (68719476735LL)
#define GRI_LEVELS (9)
#define GRI_MAX_LEVEL (GRI_LEVELS - 1)

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
 * Return the GenomicSQLite version. Also checks the SQLite version, failing if it's incompatible.
 */
char *genomicsqlite_version_check();

/*
 * Get configuration defaults.
 */
char *genomicsqlite_default_config_json();

/*
 * Wrap sqlite3_open() and initialize the "connection" for use with GenomicSQLite. config_json if
 * supplied, will be merged into defaults (i.e. it's not necessary to include defaults)
 */
int genomicsqlite_open(const char *dbfile, sqlite3 **ppDb, int flags, const char *config_json);

/*
 * Genomic range indexing
 */
char *create_genomic_range_index_sql(const char *table, const char *rid, const char *beg,
                                     const char *end, int max_depth);
char *genomic_range_rowids_sql(const char *indexed_table, sqlite3 *dbconn, const char *qrid,
                               const char *qbeg, const char *qend);

/*
 * Optional storage of refrence sequence metadata
 */
char *put_genomic_reference_assembly_sql(const char *assembly, const char *attached_schema);
char *put_genomic_reference_sequence_sql(const char *name, sqlite3_int64 length,
                                         const char *assembly, const char *refget_id,
                                         sqlite3_int64 rid, const char *attached_schema);

/*
 * C++ bindings: are liable to throw exceptions except where marked noexcept
 */
#ifdef __cplusplus
}

#include <map>
#include <string>

std::string GenomicSQLiteVersionCheck();
std::string GenomicSQLiteDefaultConfigJSON();

int GenomicSQLiteOpen(const std::string &dbfile, sqlite3 **ppDb, int flags,
                      const std::string &config_json = "{}") noexcept;
#ifdef SQLITECPP_VERSION
/*
 * For use with SQLiteCpp -- https://github.com/SRombauts/SQLiteCpp
 * (include SQLiteCpp/SQLiteCpp.h first)
 */
#include <memory>
std::unique_ptr<SQLite::Database> GenomicSQLiteOpen(const std::string &dbfile, int flags,
                                                    const std::string &config_json = "{}");
#endif

std::string CreateGenomicRangeIndexSQL(const std::string &table, const std::string &rid,
                                       const std::string &beg, const std::string &end,
                                       int max_depth = -1);
std::string GenomicRangeRowidsSQL(const std::string &indexed_table, sqlite3 *dbconn = nullptr,
                                  const std::string &qrid = "?1", const std::string &qbeg = "?2",
                                  const std::string &qend = "?3");

std::string PutGenomicReferenceAssemblySQL(const std::string &assembly,
                                           const std::string &attached_schema = "");
std::string PutGenomicReferenceSequenceSQL(const std::string &name, sqlite3_int64 length,
                                           const std::string &assembly = "",
                                           const std::string &refget_id = "",
                                           sqlite3_int64 rid = -1,
                                           const std::string &attached_schema = "");

struct gri_refseq_t {
    unsigned long long rid, length;
    std::string name, assembly, refget_id;
};
std::map<unsigned long long, gri_refseq_t>
GetGenomicReferenceSequencesByRid(sqlite3 *dbconn, const std::string &assembly = "",
                                  const std::string &attached_schema = "");
std::map<std::string, gri_refseq_t>
GetGenomicReferenceSequencesByName(sqlite3 *dbconn, const std::string &assembly = "",
                                   const std::string &attached_schema = "");
#endif
