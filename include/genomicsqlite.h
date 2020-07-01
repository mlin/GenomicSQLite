#include <sqlite3.h>

#define GRI_MAX_POS (68719476735LL)

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Wrap sqlite3_open() and initialize the "connection" for use with GenomicSQLite.
 */
int GenomicSQLiteOpen(const char *dbfile, sqlite3 **ppDb, int flags, int unsafe_load,
                      int page_cache_size, int threads, int zstd_level, int inner_page_size,
                      int outer_page_size);

/*
 * Subroutines of GenomicSQLiteOpen() exposed to support equivalent idiomatic bindings for other
 * programming languages.
 */
const char *GenomicSQLiteVersionCheck();
char *GenomicSQLiteURI(const char *dbfile, int unsafe_load, int threads, int zstd_level,
                       int outer_page_size);
char *GenomicSQLiteTuning(const char *attached_schema, int unsafe_load, int page_cache_size,
                          int threads, int inner_page_size);

/*
 * Genomic range indexing routines
 */
char *CreateGenomicRangeIndex(const char *table, const char *assembly, int max_level,
                              const char *rid_col, const char *beg_expr, const char *end_expr);

char *OverlappingGenomicRanges(sqlite3 *dbconn, const char *indexed_table, const char *qrid,
                               const char *qbeg, const char *qend);
char *OnOverlappingGenomicRanges(sqlite3 *dbconn, const char *indexed_right_table,
                                 const char *left_rid, const char *left_beg, const char *left_end);

/*
 * Optional storage of refrence sequence metadata
 */
char *PutReferenceAssembly(const char *assembly, const char *attached_schema);
char *PutReferenceSequence(const char *name, const char *assembly, const char *refget_id,
                           sqlite3_int64 length, int first, sqlite3_int64 rid,
                           const char *attached_schema);

/*
 * C++ bindings: are liable to throw exceptions except where marked
 */
#ifdef __cplusplus
}

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
                             int outer_page_size = 32768, int zstd_level = 6);
std::string GenomicSQLiteTuning(const std::string &attached_schema = "", bool unsafe_load = false,
                                int page_cache_size = 0, int threads = -1,
                                int inner_page_size = 16384);

std::string CreateGenomicRangeIndex(const std::string &table, const char *assembly, int max_level,
                                    const char *rid_col, const char *beg_expr,
                                    const char *end_expr);

std::string OverlappingGenomicRanges(sqlite3 *dbconn, const std::string &indexed_table,
                                     const char *qrid, const char *qbeg, const char *qend);
std::string OnOverlappingGenomicRanges(sqlite3 *dbconn, const std::string &indexed_right_table,
                                       const char *left_rid, const char *left_beg,
                                       const char *left_end);

std::string PutReferenceAssembly(const std::string &assembly,
                                 const char *attached_schema = nullptr);
std::string PutReferenceSequence(const std::string &name, const std::string &assembly,
                                 const char *refget_id, sqlite3_int64 length, bool first = true,
                                 sqlite3_int64 rid = -1, const char *attached_schema = nullptr);
#endif
