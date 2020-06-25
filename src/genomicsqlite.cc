#include <map>
#include <regex>
#include <sqlite3ext.h>
#include <sstream>
#include <thread>
SQLITE_EXTENSION_INIT1
#include "genomicsqlite.h"
#include "zstd_vfs.h"

using namespace std;

#ifndef NDEBUG
#define _DBG cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && cerr
#endif

/**************************************************************************************************
 * genomic_range_bin() SQL function to calculate bin number from beg+end (optional: max_level)
 **************************************************************************************************/

#define GRI_LEVELS (9)
const sqlite3_int64 GRI_BIN_OFFSETS[] = {
    0,
    1,
    1 + 16,
    1 + 16 + 256,
    1 + 16 + 256 + 4096,
    1 + 16 + 256 + 4096 + 65536,
    1 + 16 + 256 + 4096 + 65536 + 1048576,
    1 + 16 + 256 + 4096 + 65536 + 1048576 + 16777216,
    1 + 16 + 256 + 4096 + 65336 + 1048576 + 16777216 + 268435456,
};
const sqlite_int64 GRI_POS_OFFSETS[] = {
    0, 134217728, 8388608, 524288, 32768, 2048, 128, 8, 0,
};
const sqlite3_int64 GRI_BIN_COUNT = GRI_BIN_OFFSETS[GRI_LEVELS - 1] + 4294967296LL;

static void genomic_range_bin(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2 && !(argc == 3 && sqlite3_value_type(argv[2]) == SQLITE_INTEGER)) {
        sqlite3_result_error(context, "genomic_range_bin() expects two or three integer arguments",
                             -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    if (sqlite3_value_type(argv[0]) != SQLITE_INTEGER ||
        sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "genomic_range_bin() expects integer arguments", -1);
        return;
    }
    sqlite3_int64 beg = sqlite3_value_int64(argv[0]), end = sqlite3_value_int64(argv[1]),
                  max_level = argc >= 3 ? sqlite3_value_int64(argv[2]) : GRI_LEVELS - 1;
    if (beg < 0 || end < beg || end > GRI_MAX_POS || max_level < 0 || max_level >= GRI_LEVELS) {
        sqlite3_result_error(context, "genomic_range_bin() domain error", -1);
        return;
    }
    int lv = max_level;
    sqlite3_int64 divisor = 1LL << (4 * (GRI_LEVELS - lv));
    for (; ((beg - GRI_POS_OFFSETS[lv]) / divisor) != ((end - GRI_POS_OFFSETS[lv]) / divisor);
         --lv, divisor *= 16)
        ;
    assert(lv >= 0);
    sqlite3_int64 ans = (beg - GRI_POS_OFFSETS[lv]) / divisor;
    assert(ans >= 0);
    sqlite3_result_int64(context, ans + GRI_BIN_OFFSETS[lv]);
}

/**************************************************************************************************
 * SQLite loadable extension initialization
 **************************************************************************************************/

static int register_gri_functions(sqlite3 *db, const char **pzErrMsg,
                                  const sqlite3_api_routines *pApi) {
    int rc =
        sqlite3_create_function_v2(db, "genomic_range_bin", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                   nullptr, genomic_range_bin, nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK)
        return rc;
    return sqlite3_create_function_v2(db, "genomic_range_bin", 3,
                                      SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr,
                                      genomic_range_bin, nullptr, nullptr, nullptr);
}

/*
** This routine is called when the extension is loaded.
*/
extern "C" int sqlite3_genomicsqlite_init(sqlite3 *db, char **pzErrMsg,
                                          const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    int rc = (new ZstdVFS())->Register("zstd");
    if (rc != SQLITE_OK)
        return rc;
    rc = register_gri_functions(db, nullptr, pApi);
    if (rc != SQLITE_OK)
        return rc;
    rc = sqlite3_auto_extension((void (*)(void))register_gri_functions);
    if (rc != SQLITE_OK)
        return rc;
    return SQLITE_OK_LOAD_PERMANENTLY;
}

/**************************************************************************************************
 * connection & tuning helpers
 **************************************************************************************************/

string _version_msg;
extern "C" const char *GenomicSQLiteVersionCheck() {
    // The newest SQLite feature currently required is "Generated Columns"
    const int MIN_SQLITE_VERSION_NUMBER = 3031000;
    const string MIN_SQLITE_VERSION = "3.31.0";
    if (sqlite3_libversion_number() < MIN_SQLITE_VERSION_NUMBER) {
        if (_version_msg.empty()) {
            _version_msg = "SQLite library version (" + string(sqlite3_libversion()) +
                           ") is older than required by the GenomicSQLite extension (" +
                           MIN_SQLITE_VERSION + ")";
        }
        return _version_msg.c_str();
    }
    return nullptr;
}

string GenomicSQLiteURI(const string &dbfile, int zstd_level, int threads, bool unsafe_load) {
    ostringstream out;
    out << "file:" << dbfile << "?vfs=zstd&outer_page_size=32768&level=" << zstd_level;
    if (unsafe_load) {
        out << "&outer_unsafe";
    }
    return out.str();
}

#define C_WRAPPER(call)                                                                            \
    string ans;                                                                                    \
    try {                                                                                          \
        ans = call;                                                                                \
    } catch (exception & exn) {                                                                    \
        ans = string(1, 0) + exn.what();                                                           \
    }                                                                                              \
    char *sql = (char *)sqlite3_malloc(ans.size() + 1);                                            \
    if (sql) {                                                                                     \
        memcpy(sql, ans.c_str(), ans.size());                                                      \
        sql[ans.size()] = 0;                                                                       \
    }                                                                                              \
    return sql;

extern "C" char *GenomicSQLiteURI(const char *dbfile, int zstd_level, int threads,
                                  int unsafe_load) {
    C_WRAPPER(GenomicSQLiteURI(string(dbfile), zstd_level, threads, unsafe_load != 0));
}

string GenomicSQLiteTuning(sqlite3_int64 page_cache_size, int threads, bool unsafe_load,
                           const char *schema) {
    string schema_prefix;
    if (schema) {
        schema_prefix = schema;
        schema_prefix += ".";
    }
    map<string, string> pragmas;
    pragmas[schema_prefix + "cache_size"] =
        to_string(page_cache_size != 0 ? page_cache_size : -1048576);
    pragmas["threads"] = to_string(threads >= 0 ? threads : thread::hardware_concurrency());
    if (unsafe_load) {
        pragmas[schema_prefix + "journal_mode"] = "OFF";
        pragmas[schema_prefix + "synchronous"] = "OFF";
        pragmas[schema_prefix + "auto_vacuum"] = "FULL";
    } else {
        pragmas[schema_prefix + "journal_mode"] = "MEMORY";
    }
    ostringstream out;
    out << "PRAGMA " << schema_prefix << "page_size=16384"; // must go first
    for (const auto &p : pragmas) {
        out << ";\nPRAGMA " << p.first << "=" << p.second;
    }
    return out.str();
}

extern "C" char *GenomicSQLiteTuning(sqlite3_int64 page_cache_size, int threads, int unsafe_load,
                                     const char *schema) {
    C_WRAPPER(GenomicSQLiteTuning(page_cache_size, threads, unsafe_load != 0, schema));
}

int GenomicSQLiteOpen(const string &dbfile, sqlite3 **ppDb, int flags, int zstd_level,
                      sqlite3_int64 page_cache_size, int threads, bool unsafe_load) {
    // ensure extension is registered
    int ret;
    static bool loaded = false;
    *ppDb = nullptr;
    if (!loaded) {
        ret =
            sqlite3_open_v2(":memory:", ppDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
        if (ret != SQLITE_OK) {
            return ret;
        }
        ret = sqlite3_load_extension(*ppDb, "libgenomicsqlite", nullptr, nullptr);
        if (ret != SQLITE_OK) {
            return ret;
        }
        ret = sqlite3_close_v2(*ppDb);
        *ppDb = nullptr;
        if (ret != SQLITE_OK) {
            return ret;
        }
        if (GenomicSQLiteVersionCheck()) {
            return SQLITE_RANGE;
        }
        loaded = true;
    }

    // open as requested
    ret = sqlite3_open_v2(GenomicSQLiteURI(dbfile, zstd_level, threads, unsafe_load).c_str(), ppDb,
                          SQLITE_OPEN_URI | flags, nullptr);
    if (ret != SQLITE_OK) {
        return ret;
    }
    return sqlite3_exec(*ppDb, GenomicSQLiteTuning(page_cache_size, threads, unsafe_load).c_str(),
                        nullptr, nullptr, nullptr);
}

extern "C" int GenomicSQLiteOpen(const char *filename, sqlite3 **ppDb, int flags, int zstd_level,
                                 sqlite3_int64 page_cache_size, int threads, int unsafe_load) {
    return GenomicSQLiteOpen(string(filename), ppDb, flags, zstd_level, page_cache_size, threads,
                             unsafe_load != 0);
}

/**************************************************************************************************
 * GRI helpers
 **************************************************************************************************/

static string sqlquote(const std::string &v) {
    // TODO: reject any unusual characters
    return "'" + regex_replace(v, std::regex("'", regex::ECMAScript), "''") + "'";
}

static string gri_refseq_ddl(const char *schema) {
    string schema_prefix;
    if (schema && schema[0]) {
        schema_prefix = schema;
        schema_prefix += ".";
    }
    ostringstream out;
    out << "CREATE TABLE IF NOT EXISTS " << schema_prefix << "_gri_refseq_meta"
        << "(rid INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, assembly TEXT,"
        << " refget_id TEXT, length INTEGER NOT NULL, UNIQUE(assembly,name))"
        << ";\nCREATE INDEX IF NOT EXISTS " << schema_prefix << "_gri_refseq_meta_name ON "
        << schema_prefix << "_gri_refseq_meta(name)";
    return out.str();
}

static pair<string, string> split_schema_table(const string &qtable) {
    auto p = qtable.find('.');
    if (p == string::npos) {
        return make_pair(string(), qtable);
    }
    return make_pair(qtable.substr(0, p + 1), qtable.substr(p + 1));
}

static string remove_table_prefixes(const string &expr, const string &table) {
    // TODO: escape table as needed
    std::regex table_prefix(table + "\\.", regex::ECMAScript);
    return regex_replace(expr, table_prefix, "");
}

string CreateGenomicRangeIndex(const string &schema_table, const char *assembly, int max_level,
                               const char *rid_col, const char *beg_expr, const char *end_expr) {
    auto split = split_schema_table(schema_table);
    string schema = split.first, table = split.second;
    string rid(rid_col && rid_col[0] ? rid_col : (table + ".rid")),
        beg(beg_expr && beg_expr[0] ? beg_expr : (table + ".beg")),
        end(end_expr && end_expr[0] ? end_expr : (table + ".end"));
    if (max_level < 0 || max_level > 8) {
        max_level = 8;
    }
    size_t p;
#define COL(s)                                                                                     \
    ((p = s.find(table + ".")) != string::npos                                                     \
         ? remove_table_prefixes(s, table)                                                         \
         : (throw invalid_argument(                                                                \
               "CreateGenomicRangeIndex: beg/end column names must include table. prefix")))
    ostringstream out;
    out << gri_refseq_ddl(nullptr) << ";\nALTER TABLE " << schema << table
        << " ADD COLUMN _gri_bin INTEGER AS (genomic_range_bin((" << COL(beg) << "),(" << COL(end)
        << ")," << max_level << ")) VIRTUAL"
        << ";\nCREATE INDEX " << schema << table << "_gri ON " << table << "(" << COL(rid)
        << ", _gri_bin)"
        << ";\nCREATE TABLE IF NOT EXISTS " << schema
        << "_gri_meta(indexed_table TEXT NOT NULL PRIMARY KEY, "
           "rid_col "
           "TEXT NOT NULL, beg_expr TEXT NOT NULL, end_expr TEXT NOT NULL, assembly TEXT"
           ", max_level INTEGER NOT NULL) WITHOUT ROWID"
        << ";\nINSERT INTO " << schema
        << "_gri_meta(indexed_table,rid_col,beg_expr,end_expr,assembly,max_level) "
           "VALUES("
        << sqlquote(table) << ',';
    out << sqlquote(rid) << ',' << sqlquote(beg) << ',' << sqlquote(end) << ','
        << (assembly ? sqlquote(string(assembly)) : "NULL") << ',' << max_level << ");";
    return out.str();
}

extern "C" char *CreateGenomicRangeIndex(const char *table, const char *assembly, int max_level,
                                         const char *rid_col, const char *beg_expr,
                                         const char *end_expr) {
    C_WRAPPER(
        CreateGenomicRangeIndex(string(table), assembly, max_level, rid_col, beg_expr, end_expr));
}

struct gri_properties {
    string rid_col, beg_expr, end_expr;
    int min_level, max_level;
};

static gri_properties InspectGRI(sqlite3 *dbconn, const string &schema, const string &table) {
    // read _gri_meta to detect the indexed columns/expressions
    string query = "SELECT rid_col,beg_expr,end_expr,assembly,max_level FROM " + schema +
                   "_gri_meta WHERE "
                   "indexed_table = " +
                   sqlquote(table);
    shared_ptr<sqlite3_stmt> stmt;
    {
        sqlite3_stmt *pStmt = nullptr;
        if (sqlite3_prepare_v3(dbconn, query.c_str(), -1, 0, &pStmt, nullptr) != SQLITE_OK) {
            throw runtime_error("error inspecting genomic range index schema");
        }
        stmt = shared_ptr<sqlite3_stmt>(pStmt, sqlite3_finalize);
    }

    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        throw runtime_error("table has no genomic range index");
    }
    gri_properties ans;
    ans.rid_col = (const char *)sqlite3_column_text(stmt.get(), 0);
    ans.beg_expr = (const char *)sqlite3_column_text(stmt.get(), 1);
    ans.end_expr = (const char *)sqlite3_column_text(stmt.get(), 2);
    ans.min_level = 0;
    ans.max_level = sqlite3_column_int64(stmt.get(), 4);
    assert(ans.min_level >= 0 && ans.min_level <= ans.max_level && ans.max_level < GRI_LEVELS);

    // if assembly is specified, find the max ref sequence length to improve min_level
    if (sqlite3_column_type(stmt.get(), 3) == SQLITE_TEXT) {
        string assembly = (const char *)sqlite3_column_text(stmt.get(), 3);
        query = "SELECT MAX(length) FROM " + schema + "_gri_refseq_meta WHERE assembly = ?";
        {
            sqlite3_stmt *pStmt = nullptr;
            if (sqlite3_prepare_v3(dbconn, query.c_str(), -1, 0, &pStmt, nullptr) != SQLITE_OK) {
                throw runtime_error("database lacks any reference assembly details");
            }
            stmt = shared_ptr<sqlite3_stmt>(pStmt, sqlite3_finalize);
        }

        if (sqlite3_bind_text(stmt.get(), 1, assembly.c_str(), -1, nullptr) != SQLITE_OK) {
            throw runtime_error("error querying reference assembly details");
        }

        if (sqlite3_step(stmt.get()) != SQLITE_ROW ||
            sqlite3_column_type(stmt.get(), 0) != SQLITE_INTEGER) {
            throw runtime_error("database lacks valid details for specified reference assembly");
        }
        sqlite3_int64 max_refseq_length = sqlite3_column_int64(stmt.get(), 0);
        if (max_refseq_length > 0) {
            // set min_level to the highest level with bin size at least max_refseq_length
            sqlite3_int64 bin_size = GRI_MAX_POS;
            for (int lv = 0; lv < ans.max_level && bin_size >= max_refseq_length;
                 lv++, bin_size /= 16) {
                ans.min_level = lv;
            }
        }
        stmt.reset();
    }
    return ans;
}

static string BetweenTerm(const string &qrid, const string &q, int lv) {
    assert(lv >= 0 && lv < GRI_LEVELS);
    sqlite3_int64 divisor = 1LL << (4 * (GRI_LEVELS - lv));
    string ans;
    if (GRI_POS_OFFSETS[lv]) {
        ans = "(" + q + ") - " + to_string(GRI_POS_OFFSETS[lv]);
    } else {
        ans = q;
    }
    ans = "(" + ans + ") / " + to_string(divisor);
    if (GRI_BIN_OFFSETS[lv]) {
        ans = "(" + ans + ") + " + to_string(GRI_BIN_OFFSETS[lv]);
    }
    ans = "((" + qrid + "),(" + ans + "))";
    return ans;
}

static string OverlappingGenomicRanges(sqlite3 *dbconn, const string &indexed_table,
                                       const char *qrid, const char *qbeg, const char *qend,
                                       int join) {
    auto split = split_schema_table(indexed_table);
    string schema = split.first, table = split.second;
    auto table_gri = InspectGRI(dbconn, schema, table);

    const string qrids = (qrid && qrid[0]) ? qrid : "?1", qbegs = (qbeg && qbeg[0]) ? qbeg : "?2",
                 qends = (qend && qend[0]) ? qend : "?3";
    ostringstream betweens;
    betweens << "(";
    int lv = table_gri.max_level;
    assert(lv < GRI_LEVELS);
    for (sqlite3_int64 divisor = 1LL << (4 * (GRI_LEVELS - lv)); lv >= table_gri.min_level;
         --lv, divisor *= 16) {
        if (lv < table_gri.max_level) {
            betweens << " OR\n  ";
        }
        betweens << "((" << table_gri.rid_col << "," << indexed_table << "._gri_bin) ";
        if (lv > table_gri.min_level) {
            betweens << "BETWEEN " << BetweenTerm(qrids, qbegs, lv) << " AND "
                     << BetweenTerm(qrids, qends, lv);
        } else {
            betweens << "= ((" << qrids << ")," << GRI_BIN_OFFSETS[lv] << ")";
        }
        betweens << ")";
    }
    betweens << ")";
    ostringstream out;
    out << " " << indexed_table << " INDEXED BY " << table << "_gri";
    if (join) {
        out << " ON\n " << betweens.str();
    } else {
        out << " WHERE\n " << betweens.str();
    }
    // TODO: if indexed_table is attached, how to prepend schema to column names?
    // maybe need to regex s/table./schema.table./g ewww
    out << "\n AND NOT ((" << qbegs << ") > (" << table_gri.end_expr << ") OR (" << qends << ") < ("
        << table_gri.beg_expr << "))\n";
    return out.str();
}

string OverlappingGenomicRanges(sqlite3 *dbconn, const string &indexed_table, const char *qrid,
                                const char *qbeg, const char *qend) {
    return OverlappingGenomicRanges(dbconn, indexed_table, qrid, qbeg, qend, 0);
}

extern "C" char *OverlappingGenomicRanges(sqlite3 *dbconn, const char *table, const char *qrid,
                                          const char *qbeg, const char *qend) {
    C_WRAPPER(OverlappingGenomicRanges(dbconn, string(table), qrid, qbeg, qend));
}

string OnOverlappingGenomicRanges(sqlite3 *dbconn, const string &indexed_table, const char *qrid,
                                  const char *qbeg, const char *qend) {
    return OverlappingGenomicRanges(dbconn, indexed_table, qrid, qbeg, qend, 1);
}

extern "C" char *OnOverlappingGenomicRanges(sqlite3 *dbconn, const char *table, const char *qrid,
                                            const char *qbeg, const char *qend) {
    C_WRAPPER(OnOverlappingGenomicRanges(dbconn, string(table), qrid, qbeg, qend));
}

/**************************************************************************************************
 * reference sequence metadata (_gri_refseq_meta) helpers
 **************************************************************************************************/

string PutReferenceSequence(const string &name, const string &assembly, const char *refget_id,
                            sqlite3_int64 length, bool first, sqlite3_int64 rid,
                            const char *schema) {
    string schema_prefix;
    if (schema && schema[0]) {
        schema_prefix = schema;
        schema_prefix += ".";
    }
    ostringstream out;
    if (first) {
        out << gri_refseq_ddl(schema) << ";\n";
    }
    out << "INSERT INTO " << schema_prefix
        << "_gri_refseq_meta(rid,name,assembly,refget_id,length) VALUES("
        << (rid >= 0 ? std::to_string(rid) : "NULL") << "," << sqlquote(name) << ","
        << sqlquote(assembly) << "," << (refget_id && refget_id[0] ? sqlquote(refget_id) : "NULL")
        << "," << std::to_string(length) << ")";
    return out.str();
}

extern "C" char *PutReferenceSequence(const char *name, const char *assembly, const char *refget_id,
                                      sqlite3_int64 length, int first, sqlite3_int64 rid,
                                      const char *schema) {
    C_WRAPPER(PutReferenceSequence(string(name), string(assembly), refget_id, length, first != 0,
                                   rid, schema));
}

string PutReferenceAssembly(const string &assembly, const char *schema) {
    map<string, vector<tuple<const char *, sqlite3_int64, const char *>>> assemblies;
    /*
    wget -nv -O -
    ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz
    \ | pigz -dc > GRCh38_no_alt_analysis_set.fa samtools faidx GRCh38_no_alt_analysis_set.fa for
    rnm in $(cut -f1 GRCh38_no_alt_analysis_set.fa.fai); do sqm5=$(samtools faidx
    GRCh38_no_alt_analysis_set.fa "$rnm" | grep -v '^>' | tr -d '\n' | tr a-z A-Z | md5sum | cut -f1
    -d ' ') sqlen=$(samtools faidx GRCh38_no_alt_analysis_set.fa "$rnm" | grep -v '^>' | tr -d '\n'
    | wc -c) echo "{\"${rnm}\",${sqlen},\"${sqm5}\"}," done
    */
    assemblies["GRCh38_no_alt_analysis_set"] = {
        {"chr1", 248956422, "6aef897c3d6ff0c78aff06ac189178dd"},
        {"chr2", 242193529, "f98db672eb0993dcfdabafe2a882905c"},
        {"chr3", 198295559, "76635a41ea913a405ded820447d067b0"},
        {"chr4", 190214555, "3210fecf1eb92d5489da4346b3fddc6e"},
        {"chr5", 181538259, "a811b3dc9fe66af729dc0dddf7fa4f13"},
        {"chr6", 170805979, "5691468a67c7e7a7b5f2a3a683792c29"},
        {"chr7", 159345973, "cc044cc2256a1141212660fb07b6171e"},
        {"chr8", 145138636, "c67955b5f7815a9a1edfaa15893d3616"},
        {"chr9", 138394717, "6c198acf68b5af7b9d676dfdd531b5de"},
        {"chr10", 133797422, "c0eeee7acfdaf31b770a509bdaa6e51a"},
        {"chr11", 135086622, "1511375dc2dd1b633af8cf439ae90cec"},
        {"chr12", 133275309, "96e414eace405d8c27a6d35ba19df56f"},
        {"chr13", 114364328, "a5437debe2ef9c9ef8f3ea2874ae1d82"},
        {"chr14", 107043718, "e0f0eecc3bcab6178c62b6211565c807"},
        {"chr15", 101991189, "f036bd11158407596ca6bf3581454706"},
        {"chr16", 90338345, "db2d37c8b7d019caaf2dd64ba3a6f33a"},
        {"chr17", 83257441, "f9a0fb01553adb183568e3eb9d8626db"},
        {"chr18", 80373285, "11eeaa801f6b0e2e36a1138616b8ee9a"},
        {"chr19", 58617616, "85f9f4fc152c58cb7913c06d6b98573a"},
        {"chr20", 64444167, "b18e6c531b0bd70e949a7fc20859cb01"},
        {"chr21", 46709983, "974dc7aec0b755b19f031418fdedf293"},
        {"chr22", 50818468, "ac37ec46683600f808cdd41eac1d55cd"},
        {"chrX", 156040895, "2b3a55ff7f58eb308420c8a9b11cac50"},
        {"chrY", 57227415, "ce3e31103314a704255f3cd90369ecce"},
        {"chrM", 16569, "c68f52674c9fb33aef52dcf399755519"},
        {"chr1_KI270706v1_random", 175055, "62def1a794b3e18192863d187af956e6"},
        {"chr1_KI270707v1_random", 32032, "78135804eb15220565483b7cdd02f3be"},
        {"chr1_KI270708v1_random", 127682, "1e95e047b98ed92148dd84d6c037158c"},
        {"chr1_KI270709v1_random", 66860, "4e2db2933ea96aee8dab54af60ecb37d"},
        {"chr1_KI270710v1_random", 40176, "9949f776680c6214512ee738ac5da289"},
        {"chr1_KI270711v1_random", 42210, "af383f98cf4492c1f1c4e750c26cbb40"},
        {"chr1_KI270712v1_random", 176043, "c38a0fecae6a1838a405406f724d6838"},
        {"chr1_KI270713v1_random", 40745, "cb78d48cc0adbc58822a1c6fe89e3569"},
        {"chr1_KI270714v1_random", 41717, "42f7a452b8b769d051ad738ee9f00631"},
        {"chr2_KI270715v1_random", 161471, "b65a8af1d7bbb7f3c77eea85423452bb"},
        {"chr2_KI270716v1_random", 153799, "2828e63b8edc5e845bf48e75fbad2926"},
        {"chr3_GL000221v1_random", 155397, "3238fb74ea87ae857f9c7508d315babb"},
        {"chr4_GL000008v2_random", 209709, "a999388c587908f80406444cebe80ba3"},
        {"chr5_GL000208v1_random", 92689, "aa81be49bf3fe63a79bdc6a6f279abf6"},
        {"chr9_KI270717v1_random", 40062, "796773a1ee67c988b4de887addbed9e7"},
        {"chr9_KI270718v1_random", 38054, "b0c463c8efa8d64442b48e936368dad5"},
        {"chr9_KI270719v1_random", 176845, "cd5e932cfc4c74d05bb64e2126873a3a"},
        {"chr9_KI270720v1_random", 39050, "8c2683400a4aeeb40abff96652b9b127"},
        {"chr11_KI270721v1_random", 100316, "9654b5d3f36845bb9d19a6dbd15d2f22"},
        {"chr14_GL000009v2_random", 201709, "862f555045546733591ff7ab15bcecbe"},
        {"chr14_GL000225v1_random", 211173, "63945c3e6962f28ffd469719a747e73c"},
        {"chr14_KI270722v1_random", 194050, "51f46c9093929e6edc3b4dfd50d803fc"},
        {"chr14_GL000194v1_random", 191469, "6ac8f815bf8e845bb3031b73f812c012"},
        {"chr14_KI270723v1_random", 38115, "74a4b480675592095fb0c577c515b5df"},
        {"chr14_KI270724v1_random", 39555, "c3fcb15dddf45f91ef7d94e2623ce13b"},
        {"chr14_KI270725v1_random", 172810, "edc6402e58396b90b8738a5e37bf773d"},
        {"chr14_KI270726v1_random", 43739, "fbe54a3197e2b469ccb2f4b161cfbe86"},
        {"chr15_KI270727v1_random", 448248, "84fe18a7bf03f3b7fc76cbac8eb583f1"},
        {"chr16_KI270728v1_random", 1872759, "369ff74cf36683b3066a2ca929d9c40d"},
        {"chr17_GL000205v2_random", 185591, "458e71cd53dd1df4083dc7983a6c82c4"},
        {"chr17_KI270729v1_random", 280839, "2756f6ee4f5780acce31e995443508b6"},
        {"chr17_KI270730v1_random", 112551, "48f98ede8e28a06d241ab2e946c15e07"},
        {"chr22_KI270731v1_random", 150754, "8176d9a20401e8d9f01b7ca8b51d9c08"},
        {"chr22_KI270732v1_random", 41543, "d837bab5e416450df6e1038ae6cd0817"},
        {"chr22_KI270733v1_random", 179772, "f1fa05d48bb0c1f87237a28b66f0be0b"},
        {"chr22_KI270734v1_random", 165050, "1d17410ae2569c758e6dd51616412d32"},
        {"chr22_KI270735v1_random", 42811, "eb6b07b73dd9a47252098ed3d9fb78b8"},
        {"chr22_KI270736v1_random", 181920, "2ff189f33cfa52f321accddf648c5616"},
        {"chr22_KI270737v1_random", 103838, "2ea8bc113a8193d1d700b584b2c5f42a"},
        {"chr22_KI270738v1_random", 99375, "854ec525c7b6a79e7268f515b6a9877c"},
        {"chr22_KI270739v1_random", 73985, "760fbd73515fedcc9f37737c4a722d6a"},
        {"chrY_KI270740v1_random", 37240, "69e42252aead509bf56f1ea6fda91405"},
        {"chrUn_KI270302v1", 2274, "ee6dff38036f7d03478c70717643196e"},
        {"chrUn_KI270304v1", 2165, "9423c1b46a48aa6331a77ab5c702ac9d"},
        {"chrUn_KI270303v1", 1942, "2cb746c78e0faa11e628603a4bc9bd58"},
        {"chrUn_KI270305v1", 1472, "f9acea3395b6992cf3418b6689b98cf9"},
        {"chrUn_KI270322v1", 21476, "7d459255d1c54369e3b64e719061a5a5"},
        {"chrUn_KI270320v1", 4416, "d898b9c5a0118e76481bf5695272959e"},
        {"chrUn_KI270310v1", 1201, "af6cb123af7007793bac06485a2a20e9"},
        {"chrUn_KI270316v1", 1444, "6adde7a9fe7bd6918f12d0f0924aa8ba"},
        {"chrUn_KI270315v1", 2276, "ecc43e822dc011fae1fcfd9981c46e9c"},
        {"chrUn_KI270312v1", 998, "26499f2fe4c65621fd8f4ecafbad31d7"},
        {"chrUn_KI270311v1", 12399, "59594f9012d8ce21ed5d1119c051a2ba"},
        {"chrUn_KI270317v1", 37690, "cd4b1fda800f6ec9ea8001994dbf6499"},
        {"chrUn_KI270412v1", 1179, "7bb9612f733fb7f098be79499d46350c"},
        {"chrUn_KI270411v1", 2646, "fc240322d91d43c04f349cc58fda3eca"},
        {"chrUn_KI270414v1", 2489, "753e02ef3b1c590e0e3376ad2ebb5836"},
        {"chrUn_KI270419v1", 1029, "58455e7a788f0dc82034d1fb109f6f5c"},
        {"chrUn_KI270418v1", 2145, "1537ec12b9c58d137a2d4cb9db896bbc"},
        {"chrUn_KI270420v1", 2321, "bac954a897539c91982a7e3985a49910"},
        {"chrUn_KI270424v1", 2140, "747c8f94f34d5de4ad289bc604708210"},
        {"chrUn_KI270417v1", 2043, "cd26758fda713f9c96e51d541f50c2d0"},
        {"chrUn_KI270422v1", 1445, "3fce80eb4c0554376b591699031feb56"},
        {"chrUn_KI270423v1", 981, "bdf5a85c001731dccfb150db2bfe58ac"},
        {"chrUn_KI270425v1", 1884, "665a46879bbb48294b0cfa87b61e71f6"},
        {"chrUn_KI270429v1", 1361, "ee8962dbef9396884f649e566b78bf06"},
        {"chrUn_KI270442v1", 392061, "796289c4cda40e358991f9e672490015"},
        {"chrUn_KI270466v1", 1233, "530b7033716a5d72dd544213c513fd12"},
        {"chrUn_KI270465v1", 1774, "bb1b2323414425c46531b3c3d22ae00d"},
        {"chrUn_KI270467v1", 3920, "db34e0dc109a4afd499b5ec6aaae9754"},
        {"chrUn_KI270435v1", 92983, "1655c75415b9c29e143a815f44286d05"},
        {"chrUn_KI270438v1", 112505, "e765271939b854dd6826aa764e930c87"},
        {"chrUn_KI270468v1", 4055, "0a603090f06108ed7aff75df0767b822"},
        {"chrUn_KI270510v1", 2415, "cd7348b3b5d9d0dfef6aed2af75ce920"},
        {"chrUn_KI270509v1", 2318, "1cdeb8c823d839e1d1735b5bc9a14856"},
        {"chrUn_KI270518v1", 2186, "3fd898b62ca859f50fb8b83e7706352b"},
        {"chrUn_KI270508v1", 1951, "7d42a358d472b9cbdfdf30c8742473d0"},
        {"chrUn_KI270516v1", 1300, "1cbaaafbbf016906a5bf5886f5a0ecb7"},
        {"chrUn_KI270512v1", 22689, "ba1021c82d1230af856f59079e2f71b4"},
        {"chrUn_KI270519v1", 138126, "8d754e9c9afd904fba0a2cd577fcc9a1"},
        {"chrUn_KI270522v1", 5674, "070b4678e22501029c2e3297115216bc"},
        {"chrUn_KI270511v1", 8127, "907ca34a4a2a6673632ebaf513a4c1a4"},
        {"chrUn_KI270515v1", 6361, "dd7527ee8e0bdb0a43ca0b2a5456c8c3"},
        {"chrUn_KI270507v1", 5353, "311894d0a815eb07c5cac49da851cb4a"},
        {"chrUn_KI270517v1", 3253, "913440c38d95c618617ca69bb9296170"},
        {"chrUn_KI270529v1", 1899, "4caf890f2586daab8e4b2e2db904f05f"},
        {"chrUn_KI270528v1", 2983, "d75c9235f0b8c449fc4352997c56b086"},
        {"chrUn_KI270530v1", 2168, "04549369e1197c626669a10164613635"},
        {"chrUn_KI270539v1", 993, "19e3a982e67eafef39c5a3e4163f1e17"},
        {"chrUn_KI270538v1", 91309, "d60b72221cc7af871f2c757577e4c92a"},
        {"chrUn_KI270544v1", 1202, "e62a14b14467cdf48b5a236c66918f0f"},
        {"chrUn_KI270548v1", 1599, "866b0db8e9cf66208c2c064bd09ce0a2"},
        {"chrUn_KI270583v1", 1400, "b127e2e6dbe358ff192b271b8c6ee690"},
        {"chrUn_KI270587v1", 2969, "36be47659719f47b95caa51744aa8f70"},
        {"chrUn_KI270580v1", 1553, "1df30dae0f605811d927dcea58e729fc"},
        {"chrUn_KI270581v1", 7046, "9f26945f9f9b3f865c9ebe953cbbc1a9"},
        {"chrUn_KI270579v1", 31033, "fe62fb1964002717cc1b034630e89b1f"},
        {"chrUn_KI270589v1", 44474, "211c215414693fe0a2399cf82e707e03"},
        {"chrUn_KI270590v1", 4685, "e8a57f147561b361091791b9010cd28b"},
        {"chrUn_KI270584v1", 4513, "d93636c9d54abd013cfc0d4c01334032"},
        {"chrUn_KI270582v1", 6504, "6fd9804a7478d2e28160fe9f017689cb"},
        {"chrUn_KI270588v1", 6158, "37ffa850e69b342a8f8979bd3ffc77d4"},
        {"chrUn_KI270593v1", 3041, "f4a5bfa203e9e81acb640b18fb11e78e"},
        {"chrUn_KI270591v1", 5796, "d6af509d69835c9ac25a30086e5a4051"},
        {"chrUn_KI270330v1", 1652, "c2c590706a339007b00c59e0b8937e78"},
        {"chrUn_KI270329v1", 1040, "f023f927ae84c5cc48dc4dce11ba90f2"},
        {"chrUn_KI270334v1", 1368, "53afe12d1371f250a3d1de655345d374"},
        {"chrUn_KI270333v1", 2699, "57baf650c47bba9b3a8b7c6d0fb55ad6"},
        {"chrUn_KI270335v1", 1048, "eb27188639503b524d2659a23b8262ea"},
        {"chrUn_KI270338v1", 1428, "301ef75a6b2996d745eb3464bd352b57"},
        {"chrUn_KI270340v1", 1428, "56b462bac20d385cdfcde0155fe4c3a1"},
        {"chrUn_KI270336v1", 1026, "69ad2d85d870c8b0269434581e86e30e"},
        {"chrUn_KI270337v1", 1121, "16fc8d71a2662a6cfec7bdeec3d810c6"},
        {"chrUn_KI270363v1", 1803, "6edd17a912f391022edbc192d49f2489"},
        {"chrUn_KI270364v1", 2855, "6ff66a8e589ca27d93b5bac0e5b13a87"},
        {"chrUn_KI270362v1", 3530, "bc82401ffd9a5ae711fa0ea34da8d2f0"},
        {"chrUn_KI270366v1", 8320, "44a0b65b7ba6bcff37eca202e7d966ea"},
        {"chrUn_KI270378v1", 1048, "fc13bda7dbd914c92fb7e49489d1350f"},
        {"chrUn_KI270379v1", 1045, "3218bef25946cd95de585dfc7750f63b"},
        {"chrUn_KI270389v1", 1298, "2c9b08c57c27e714d4d5259fd91b6983"},
        {"chrUn_KI270390v1", 2387, "7a64d89ea14990c16d20f4d6e7283e10"},
        {"chrUn_KI270387v1", 1537, "22a12462264340c25e912b8485cdfa91"},
        {"chrUn_KI270395v1", 1143, "7c03ca4756c1620f318fb189214388d8"},
        {"chrUn_KI270396v1", 1880, "9069bed3c2efe7cc87227d619ad5816f"},
        {"chrUn_KI270388v1", 1216, "76f9f3315fa4b831e93c36cd88196480"},
        {"chrUn_KI270394v1", 970, "d5171e863a3d8f832f0559235987b1e5"},
        {"chrUn_KI270386v1", 1788, "b9b1baaa7abf206f6b70cf31654172db"},
        {"chrUn_KI270391v1", 1484, "1fa5cf03b3eac0f1b4a64948fd09de53"},
        {"chrUn_KI270383v1", 1750, "694d75683e4a9554bcc1291edbcaee43"},
        {"chrUn_KI270393v1", 1308, "3724e1d70677d6b5c4bcf17fd40da111"},
        {"chrUn_KI270384v1", 1658, "b06e44ea15d0a57618d6ca7d2e6ac5d2"},
        {"chrUn_KI270392v1", 971, "59b3ca8de65fb171683f8a06d3b4bf0d"},
        {"chrUn_KI270381v1", 1930, "2a9297cfd3b3807195ab9ad07e775d99"},
        {"chrUn_KI270385v1", 990, "112a8b1df94ef0498a0bfe2d2ea5cc23"},
        {"chrUn_KI270382v1", 4215, "e7085cdcee6ad62f359744e13d3209fc"},
        {"chrUn_KI270376v1", 1136, "59e8fc80b78d62325082334b43dffdba"},
        {"chrUn_KI270374v1", 2656, "dbc92c9a92e558946e58b4909ec95dd5"},
        {"chrUn_KI270372v1", 1650, "53a9d5e8fd28bce5da5efcfd9114dbf2"},
        {"chrUn_KI270373v1", 1451, "b174fe53be245a840cd6324e39b88ced"},
        {"chrUn_KI270375v1", 2378, "d678250c97e9b94aa390fa46e70a6d83"},
        {"chrUn_KI270371v1", 2805, "a0af3d778dfeb7963e8e6d84c0c54fba"},
        {"chrUn_KI270448v1", 7992, "0f40827c265cb813b6e723da6c9b926b"},
        {"chrUn_KI270521v1", 7642, "af5bef7cefec7bd7efa729ac6c5be088"},
        {"chrUn_GL000195v1", 182896, "5d9ec007868d517e73543b005ba48535"},
        {"chrUn_GL000219v1", 179198, "f977edd13bac459cb2ed4a5457dba1b3"},
        {"chrUn_GL000220v1", 161802, "fc35de963c57bf7648429e6454f1c9db"},
        {"chrUn_GL000224v1", 179693, "d5b2fc04f6b41b212a4198a07f450e20"},
        {"chrUn_KI270741v1", 157432, "86eaea8a15a3950e37442eaaa5c9dc92"},
        {"chrUn_GL000226v1", 15008, "1c1b2cd1fccbc0a99b6a447fa24d1504"},
        {"chrUn_GL000213v1", 164239, "9d424fdcc98866650b58f004080a992a"},
        {"chrUn_KI270743v1", 210658, "3b62d9d3100f530d509e4efebd98502c"},
        {"chrUn_KI270744v1", 168472, "e90aee46b947ff8c32291a6843fde3f9"},
        {"chrUn_KI270745v1", 41891, "1386fe3de6f82956f2124e19353ff9c1"},
        {"chrUn_KI270746v1", 66486, "c470486a0a858e14aa21d7866f83cc17"},
        {"chrUn_KI270747v1", 198735, "62375d812ece679c9fd2f3d08d4e22a4"},
        {"chrUn_KI270748v1", 93321, "4f6c6ab005c852a4352aa33e7cc88ded"},
        {"chrUn_KI270749v1", 158759, "c899a7b4e911d371283f3f4058ca08b7"},
        {"chrUn_KI270750v1", 148850, "c022ba92f244b7dc54ea90c4eef4d554"},
        {"chrUn_KI270751v1", 150742, "1b758bbdee0e9ca882058d916cba9d29"},
        {"chrUn_KI270752v1", 27745, "e0880631848337bd58559d9b1519da63"},
        {"chrUn_KI270753v1", 62944, "25075fb2a1ecada67c0eb2f1fe0c7ec9"},
        {"chrUn_KI270754v1", 40191, "fe9e16233cecbc244f06f3acff3d03b8"},
        {"chrUn_KI270755v1", 36723, "4a7da6a658955bd787af8add3ccb5751"},
        {"chrUn_KI270756v1", 79590, "2996b120a5a5e15dab6555f0bf92e374"},
        {"chrUn_KI270757v1", 71251, "174c73b60b41d8a1ef0fbaa4b3bdf0d3"},
        {"chrUn_GL000214v1", 137718, "46c2032c37f2ed899eb41c0473319a69"},
        {"chrUn_KI270742v1", 186739, "2f31c013a4a8301deb8ab7ed1ca1cd99"},
        {"chrUn_GL000216v2", 176608, "725009a7e3f5b78752b68afa922c090c"},
        {"chrUn_GL000218v1", 161147, "1d708b54644c26c7e01c2dad5426d38c"},
        {"chrEBV", 171823, "6743bd63b3ff2b5b8985d8933c53290a"}};

    auto p = assemblies.find(assembly);
    if (p == assemblies.end()) {
        return "";
    }
    ostringstream out;
    bool first = true;
    for (const auto &q : p->second) {
        if (!first) {
            out << ";\n";
        }
        out << PutReferenceSequence(get<0>(q), assembly, get<2>(q), get<1>(q), first, -1, schema);
        first = false;
    }
    return out.str();
}

extern "C" char *PutReferenceAssembly(const char *assembly, const char *schema) {
    C_WRAPPER(PutReferenceAssembly(string(assembly), schema));
}
