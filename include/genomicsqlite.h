#include <sqlite3.h>

#define GRI_MAX_POS (68719476735LL)
#define GRI_LEVELS (9)
#define GRI_MAX_LEVEL (GRI_LEVELS - 1)

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Wrap sqlite3_open() and initialize the "connection" for use with GenomicSQLite.
 */
int genomicsqlite_open(const char *dbfile, sqlite3 **ppDb, int flags, int unsafe_load,
                       int page_cache_size, int threads, int zstd_level, int inner_page_size,
                       int outer_page_size);

/*
 * Subroutines of genomicsqlite_open() exposed to support equivalent idiomatic bindings for other
 * programming languages.
 */
const char *genomicsqlite_version_check();
char *genomicsqlite_uri(const char *dbfile, int unsafe_load, int threads, int zstd_level,
                        int outer_page_size);
char *genomicsqlite_tuning_sql(const char *attached_schema, int unsafe_load, int page_cache_size,
                               int threads, int inner_page_size);

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
 * C++ bindings: are liable to throw exceptions except where marked
 */
#ifdef __cplusplus
}

#include <map>
#include <string>

int GenomicSQLiteOpen(const std::string &dbfile, sqlite3 **ppDb, int flags,
                      bool unsafe_load = false, int page_cache_size = -1048576, int threads = -1,
                      int zstd_level = 6, int inner_page_size = 16384,
                      int outer_page_size = 32768) noexcept;
#ifdef SQLITECPP_VERSION
/*
 * For use with SQLiteCpp -- https://github.com/SRombauts/SQLiteCpp
 * (include SQLiteCpp/SQLiteCpp.h first)
 */
#include <memory>
std::unique_ptr<SQLite::Database>
GenomicSQLiteOpen(const std::string &dbfile, int flags, bool unsafe_load = false,
                  int page_cache_size = -1048576, int threads = -1, int zstd_level = 6,
                  int inner_page_size = 16384, int outer_page_size = 32768);
#endif

std::string GenomicSQLiteURI(const std::string &dbfile, bool unsafe_load = false, int threads = -1,
                             int zstd_level = 6, int outer_page_size = 32768);
std::string GenomicSQLiteTuningSQL(const std::string &attached_schema = "",
                                   bool unsafe_load = false, int page_cache_size = 0,
                                   int threads = -1, int inner_page_size = 16384);

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
