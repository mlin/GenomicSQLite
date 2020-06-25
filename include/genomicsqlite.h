#include <sqlite3.h>

#define GRI_MAX_POS (68719476735LL)

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Wrap sqlite3_open() and initialize the "connection" for use with GenomicSQLite.
 */
int GenomicSQLiteOpen(const char *dbfile, sqlite3 **ppDb, int flags, int zstd_level,
                      sqlite3_int64 page_cache_size, int threads, int unsafe_load);

/*
 * Subroutines of GenomicSQLiteOpen() exposed to support equivalent idiomatic bindings for other
 * programming languages.
 */
const char *GenomicSQLiteVersionCheck();
char *GenomicSQLiteURI(const char *dbfile, int zstd_level, int threads, int unsafe_load);
char *GenomicSQLiteTuning(sqlite3_int64 page_cache_size, int threads, int unsafe_load,
                          const char *attached_schema);

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

#ifdef __cplusplus
}

#include <string>

int GenomicSQLiteOpen(const std::string &dbfile, sqlite3 **ppDb, int flags, int zstd_level = 8,
                      sqlite3_int64 page_cache_size = 0, int threads = -1,
                      bool unsafe_load = false);
std::string GenomicSQLiteURI(const std::string &dbfile, int zstd_level = 8, int threads = -1,
                             bool unsafe_load = false);
std::string GenomicSQLiteTuning(sqlite3_int64 page_cache_size = 0, int threads = -1,
                                bool unsafe_load = false, const char *attached_schema = nullptr);

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
