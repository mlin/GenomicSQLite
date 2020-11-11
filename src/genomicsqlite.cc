#include <assert.h>
#include <sqlite3ext.h>
#include <sstream>
#include <thread>
#include <vector>
extern "C" {
SQLITE_EXTENSION_INIT1
}
#include "SQLiteCpp/SQLiteCpp.h"
#include "SQLiteVirtualTable.hpp"
#include "genomicsqlite.h"
#include "hardcoded_refseq.hpp"
#include "zstd_vfs.h"

using namespace std;

#ifndef NDEBUG
#define _DBG cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && cerr
#endif
#define _DBGV(x) _DBG << #x << " = " << (x) << endl;

/**************************************************************************************************
 * connection & tuning helpers
 **************************************************************************************************/

std::string GenomicSQLiteVersion() { return string(GIT_REVISION); }

// boilerplate for C bindings to C++ functions
#define C_WRAPPER(call)                                                                            \
    string ans;                                                                                    \
    try {                                                                                          \
        ans = call;                                                                                \
    } catch (exception & exn) {                                                                    \
        ans = string(1, 0) + exn.what();                                                           \
    }                                                                                              \
    char *copy = (char *)sqlite3_malloc(ans.size() + 1);                                           \
    if (copy) {                                                                                    \
        memcpy(copy, ans.c_str(), ans.size());                                                     \
        copy[ans.size()] = 0;                                                                      \
    }                                                                                              \
    return copy;

extern "C" char *genomicsqlite_version() { C_WRAPPER(GenomicSQLiteVersion()) }

#define SQL_WRAPPER(invocation)                                                                    \
    try {                                                                                          \
        string ans = invocation;                                                                   \
        sqlite3_result_text(ctx, ans.c_str(), -1, SQLITE_TRANSIENT);                               \
    } catch (std::bad_alloc &) {                                                                   \
        sqlite3_result_error_nomem(ctx);                                                           \
    } catch (std::exception & exn) {                                                               \
        sqlite3_result_error(ctx, exn.what(), -1);                                                 \
    }

static void sqlfn_genomicsqlite_version(sqlite3_context *ctx, int argc,
                                        sqlite3_value **argv){SQL_WRAPPER(GenomicSQLiteVersion())}

std::string GenomicSQLiteDefaultConfigJSON() {
    return R"({
    "unsafe_load": false,
    "immutable": false,
    "page_cache_MiB": 1024,
    "threads": -1,
    "force_prefetch": false,
    "zstd_level": 6,
    "inner_page_KiB": 16,
    "outer_page_KiB": 32
})";
}

static unique_ptr<SQLite::Statement> ConfigExtractor(SQLite::Database &tmpdb,
                                                     const string &config_json) {
    string merged_json = GenomicSQLiteDefaultConfigJSON();
    if (config_json.size() > 2) { // "{}"
        SQLite::Statement patch(tmpdb, "SELECT json_patch(?,?)");
        patch.bind(1, merged_json);
        patch.bind(2, config_json);
        if (!patch.executeStep() || patch.getColumnCount() != 1 || !patch.getColumn(0).isText())
            throw std::runtime_error("error processing config JSON");
        merged_json = patch.getColumn(0).getText();
    }

    unique_ptr<SQLite::Statement> ans(new SQLite::Statement(tmpdb, "SELECT json_extract(?,?)"));
    ans->bind(1, merged_json);
    return ans;
}

extern "C" char *genomicsqlite_default_config_json() { C_WRAPPER(GenomicSQLiteDefaultConfigJSON()) }

static void sqlfn_genomicsqlite_default_config_json(sqlite3_context *ctx, int argc,
                                                    sqlite3_value **argv){
    SQL_WRAPPER(GenomicSQLiteDefaultConfigJSON())}

string GenomicSQLiteURI(const string &dbfile, const string &config_json = "") {
    SQLite::Database tmpdb(":memory:", SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
    auto extract = ConfigExtractor(tmpdb, config_json);

    extract->bind(2, "$.mode");
    if (!extract->executeStep() || extract->getColumnCount() != 1)
        throw std::runtime_error("error processing config JSON $.mode");
    string mode;
    if (extract->getColumn(0).isText()) {
        mode = extract->getColumn(0).getText();
    }
    extract->reset();

    extract->bind(2, "$.immutable");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.immutable");
    bool immutable = extract->getColumn(0).getInt() != 0;
    extract->reset();

    extract->bind(2, "$.unsafe_load");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.unsafe_load");
    bool unsafe_load = extract->getColumn(0).getInt() != 0;
    extract->reset();

    extract->bind(2, "$.threads");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.threads");
    int threads = extract->getColumn(0).getInt();
    extract->reset();

    extract->bind(2, "$.force_prefetch");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.force_prefetch");
    bool force_prefetch = extract->getColumn(0).getInt() != 0;
    extract->reset();

    extract->bind(2, "$.inner_page_KiB");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.inner_page_KiB");
    int inner_page_KiB = extract->getColumn(0).getInt();
    extract->reset();

    extract->bind(2, "$.outer_page_KiB");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.outer_page_KiB");
    int outer_page_KiB = extract->getColumn(0).getInt();
    extract->reset();

    extract->bind(2, "$.zstd_level");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.zstd_level");
    int zstd_level = extract->getColumn(0).getInt();
    extract->reset();

    ostringstream uri;
    uri << "file:" << dbfile << "?vfs=zstd"; // TODO: URI-encode dbfile
    if (!mode.empty()) {
        uri << "&mode=" << mode;
    }
    uri << "&threads=" << to_string(threads);
    uri << "&outer_page_size=" << to_string(outer_page_KiB * 1024);
    uri << "&outer_cache_size=-65536"; // enlarge to hold index b-tree pages for large db's
    uri << "&level=" << to_string(zstd_level);
    if (threads > 1 && inner_page_KiB < 16 && !force_prefetch) {
        // prefetch is usually counterproductive if inner_page_KiB < 16
        uri << "&noprefetch=1";
    }
    if (immutable) {
        uri << "&immutable=1";
    }
    if (unsafe_load) {
        uri << "&nolock=1&outer_unsafe";
    }
    return uri.str();
}

extern "C" char *genomicsqlite_uri(const char *dbfile, const char *config_json) {
    C_WRAPPER(GenomicSQLiteURI(string(dbfile), config_json ? config_json : ""));
}

// boilerplate for SQLite custom function bindings
#define ARG_TYPE(idx, expected)                                                                    \
    assert(argc > idx);                                                                            \
    if (sqlite3_value_type(argv[idx]) != (expected)) {                                             \
        string errmsg =                                                                            \
            string(__func__) + "() argument #" + to_string(idx + 1) + " type mismatch";            \
        sqlite3_result_error(ctx, errmsg.c_str(), -1);                                             \
        return;                                                                                    \
    }

#define OPTIONAL_ARG_TYPE(idx, expected, dest)                                                     \
    assert(argc > idx);                                                                            \
    dest = sqlite3_value_type(argv[idx]);                                                          \
    if (dest != (expected) && dest != SQLITE_NULL) {                                               \
        string errmsg =                                                                            \
            string(__func__) + "() argument #" + to_string(idx + 1) + " type mismatch";            \
        sqlite3_result_error(ctx, errmsg.c_str(), -1);                                             \
        return;                                                                                    \
    }

#define ARG(dest, idx, expected, suffix)                                                           \
    ARG_TYPE(idx, expected)                                                                        \
    dest = sqlite3_value_##suffix(argv[idx]);

#define ARG_OPTIONAL(dest, idx, expected, suffix)                                                  \
    if (argc > idx) {                                                                              \
        int __ty;                                                                                  \
        OPTIONAL_ARG_TYPE(idx, expected, __ty);                                                    \
        if (__ty != SQLITE_NULL)                                                                   \
            dest = sqlite3_value_##suffix(argv[idx]);                                              \
    }

#define ARG_TEXT(dest, idx)                                                                        \
    ARG_TYPE(idx, SQLITE_TEXT)                                                                     \
    dest = (const char *)sqlite3_value_text(argv[idx]);

#define ARG_TEXT_OPTIONAL(dest, idx)                                                               \
    if (argc > idx) {                                                                              \
        int __ty;                                                                                  \
        OPTIONAL_ARG_TYPE(idx, SQLITE_TEXT, __ty)                                                  \
        if (__ty != SQLITE_NULL)                                                                   \
            dest = (const char *)sqlite3_value_text(argv[idx]);                                    \
    }

static void sqlfn_genomicsqlite_uri(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    string dbfile, config_json;
    assert(argc == 1 || argc == 2);
    ARG_TEXT(dbfile, 0)
    ARG_TEXT_OPTIONAL(config_json, 1);
    SQL_WRAPPER(GenomicSQLiteURI(dbfile, config_json))
}

string GenomicSQLiteTuningSQL(const string &config_json, const string &schema = "") {
    SQLite::Database tmpdb(":memory:", SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
    auto extract = ConfigExtractor(tmpdb, config_json);

    extract->bind(2, "$.unsafe_load");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.unsafe_load");
    bool unsafe_load = extract->getColumn(0).getInt() != 0;
    extract->reset();

    extract->bind(2, "$.page_cache_MiB");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.page_cache_MiB");
    auto page_cache_MiB = extract->getColumn(0).getInt64();
    extract->reset();

    extract->bind(2, "$.threads");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.threads");
    int threads = extract->getColumn(0).getInt();
    extract->reset();

    extract->bind(2, "$.inner_page_KiB");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.inner_page_KiB");
    int inner_page_KiB = extract->getColumn(0).getInt();
    extract->reset();

    string schema_prefix;
    if (!schema.empty()) {
        schema_prefix = schema + ".";
    }
    map<string, string> pragmas;
    pragmas[schema_prefix + "cache_size"] = to_string(-1024 * page_cache_MiB);
    pragmas[schema_prefix + "max_page_count"] = "2147483646";
    if (schema_prefix.empty()) {
        pragmas["threads"] =
            to_string(threads >= 0 ? threads : std::min(8, (int)thread::hardware_concurrency()));
    }
    if (unsafe_load) {
        pragmas[schema_prefix + "journal_mode"] = "OFF";
        pragmas[schema_prefix + "synchronous"] = "OFF";
        pragmas[schema_prefix + "auto_vacuum"] = "FULL";
        pragmas[schema_prefix + "locking_mode"] = "EXCLUSIVE";
    } else {
        pragmas[schema_prefix + "journal_mode"] = "MEMORY";
    }
    ostringstream out;
    // must go first:
    out << "PRAGMA " << schema_prefix << "page_size=" << to_string(inner_page_KiB * 1024);
    for (const auto &p : pragmas) {
        out << "; PRAGMA " << p.first << "=" << p.second;
    }
    return out.str();
}

extern "C" char *genomicsqlite_tuning_sql(const char *config_json, const char *schema) {
    C_WRAPPER(GenomicSQLiteTuningSQL(string(config_json ? config_json : ""),
                                     string(schema ? schema : "")))
}

static void sqlfn_genomicsqlite_tuning_sql(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    string config_json, schema;
    ARG_TEXT_OPTIONAL(config_json, 0)
    ARG_TEXT_OPTIONAL(schema, 1)
    SQL_WRAPPER(GenomicSQLiteTuningSQL(config_json, schema))
}

static void ensure_ext_loaded() {
    // for C/C++ GenomicSQLiteOpen
    static bool ext_loaded = false;
    if (!ext_loaded) {
        SQLite::Database db(":memory:", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE);
        db.loadExtension("libgenomicsqlite", nullptr);
        ext_loaded = true;
    }
}

int GenomicSQLiteOpen(const string &dbfile, sqlite3 **ppDb, string &errmsg_out, int flags,
                      const string &config_json) noexcept {
    try {
        ensure_ext_loaded();
    } catch (SQLite::Exception &exn) {
        errmsg_out = "failed loading libgenomicsqlite shared library: " + string(exn.what());
        return exn.getErrorCode();
    } catch (std::exception &exn) {
        errmsg_out = "failed loading libgenomicsqlite shared library: " + string(exn.what());
        return SQLITE_ERROR;
    }

    // open as requested
    try {
        int ret = sqlite3_open_v2(GenomicSQLiteURI(dbfile, config_json).c_str(), ppDb,
                                  SQLITE_OPEN_URI | flags, nullptr);
        if (ret != SQLITE_OK) {
            errmsg_out = sqlite3_errstr(ret);
            return ret;
        }
        char *zErrmsg = nullptr;
        ret = sqlite3_exec(*ppDb, GenomicSQLiteTuningSQL(config_json).c_str(), nullptr, nullptr,
                           &zErrmsg);
        if (ret != SQLITE_OK) {
            if (zErrmsg) {
                errmsg_out = zErrmsg;
                sqlite3_free(zErrmsg);
            } else {
                errmsg_out = sqlite3_errmsg(*ppDb);
            }
            sqlite3_close_v2(*ppDb);
            *ppDb = nullptr;
            return ret;
        }
        return SQLITE_OK;
    } catch (SQLite::Exception &exn) {
        errmsg_out = exn.what();
        return exn.getErrorCode();
    } catch (std::exception &exn) {
        errmsg_out = exn.what();
        return SQLITE_ERROR;
    }
}

extern "C" int genomicsqlite_open(const char *filename, sqlite3 **ppDb, char **pzErrMsg, int flags,
                                  const char *config_json) {
    string errmsg;
    int ret =
        GenomicSQLiteOpen(string(filename), ppDb, errmsg, flags, config_json ? config_json : "");
    if (ret && !errmsg.empty() && pzErrMsg) {
        *pzErrMsg = (char *)sqlite3_malloc(errmsg.size());
        strcpy(*pzErrMsg, errmsg.c_str());
    }
    return ret;
}

unique_ptr<SQLite::Database> GenomicSQLiteOpen(const string &dbfile, int flags,
                                               const string &config_json) {
    ensure_ext_loaded();
    unique_ptr<SQLite::Database> db(
        new SQLite::Database(GenomicSQLiteURI(dbfile, config_json), SQLITE_OPEN_URI | flags));
    db->exec(GenomicSQLiteTuningSQL(config_json));
    return db;
}

static string sqlquote(const std::string &v) {
    ostringstream ans;
    ans << "'";
    for (char c : v) {
        if (c < 32 || c > 126)
            throw std::invalid_argument("non-printable character in: " + v);
        if (c == '\'')
            ans << "''";
        else
            ans << c;
    }
    ans << "'";
    return ans.str();
}

string GenomicSQLiteAttachSQL(const string &dbfile, const string &schema_name,
                              const string &config_json) {
    ostringstream ans;
    ans << "ATTACH " << sqlquote(GenomicSQLiteURI(dbfile, config_json)) << " AS " << schema_name
        << ";" << GenomicSQLiteTuningSQL(config_json, schema_name);
    return ans.str();
}

extern "C" char *genomicsqlite_attach_sql(const char *dbfile, const char *schema_name,
                                          const char *config_json) {
    C_WRAPPER(GenomicSQLiteAttachSQL(string(dbfile), string(schema_name),
                                     config_json ? config_json : ""));
}

static void sqlfn_genomicsqlite_attach_sql(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    string dbfile, schema_name, config_json;
    ARG_TEXT(dbfile, 0);
    ARG_TEXT(schema_name, 1);
    ARG_TEXT_OPTIONAL(config_json, 2);
    SQL_WRAPPER(GenomicSQLiteAttachSQL(dbfile, schema_name, config_json));
}

string GenomicSQLiteVacuumIntoSQL(const string &destfile, const string &config_json) {
    SQLite::Database tmpdb(":memory:", SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
    auto extract = ConfigExtractor(tmpdb, config_json);
    extract->bind(2, "$.inner_page_KiB");
    if (!extract->executeStep() || extract->getColumnCount() != 1 ||
        !extract->getColumn(0).isInteger())
        throw std::runtime_error("error processing config JSON $.inner_page_KiB");
    int inner_page_KiB = extract->getColumn(0).getInt();

    string desturi = GenomicSQLiteURI(destfile, config_json) + "&outer_unsafe=true";

    ostringstream ans;
    ans << "PRAGMA page_size = " << (inner_page_KiB * 1024) << ";\nPRAGMA auto_vacuum = FULL"
        << ";\nVACUUM INTO " << sqlquote(desturi);
    return ans.str();
}

extern "C" char *genomicsqlite_vacuum_into_sql(const char *destfile, const char *config_json) {
    C_WRAPPER(GenomicSQLiteVacuumIntoSQL(string(destfile), config_json ? config_json : ""));
}

static void sqlfn_genomicsqlite_vacuum_into_sql(sqlite3_context *ctx, int argc,
                                                sqlite3_value **argv) {
    string destfile, config_json;
    ARG_TEXT(destfile, 0)
    ARG_TEXT_OPTIONAL(config_json, 1)
    SQL_WRAPPER(GenomicSQLiteVacuumIntoSQL(destfile, config_json))
}

/**************************************************************************************************
 * GRI implementation
 **************************************************************************************************/

static pair<string, string> split_schema_table(const string &qtable) {
    auto p = qtable.find('.');
    if (p == string::npos) {
        return make_pair(string(), qtable);
    }
    return make_pair(qtable.substr(0, p + 1), qtable.substr(p + 1));
}

string CreateGenomicRangeIndexSQL(const string &schema_table, const string &rid, const string &beg,
                                  const string &end, int floor) {
    auto split = split_schema_table(schema_table);
    string schema = split.first, table = split.second;
    if (floor == -1) {
        floor = 0;
    }
    if (!(floor >= 0 && floor < 16)) {
        throw std::invalid_argument("GenomicSQLite: must have 0 <= floor < 16");
    }
    size_t p;
    ostringstream out;
    out << "ALTER TABLE " << schema_table << " ADD COLUMN _gri_rid INTEGER AS (" << rid
        << ") VIRTUAL";
    out << ";\nALTER TABLE " << schema_table << " ADD COLUMN _gri_beg INTEGER AS (" << beg
        << ") VIRTUAL";
    out << ";\nALTER TABLE " << schema_table << " ADD COLUMN _gri_len INTEGER AS ((" << end << ")-("
        << beg << ")) VIRTUAL";
    out << ";\nALTER TABLE " << schema_table
        << " ADD COLUMN _gri_lvl INTEGER AS (CASE WHEN _gri_len IS NULL OR _gri_len < 0 THEN NULL";
    for (int lv = floor; lv < 16; ++lv) {
        // note: negate _gri_lvl so that most index b-tree insertions (small features on levels
        //       closest to 0) will be rightmost
        out << " WHEN _gri_len <= 0x1" << string(lv, '0') << " THEN -" << lv;
    }
    out << " ELSE NULL END) VIRTUAL";
    out << ";\nCREATE INDEX " << schema_table << "__gri ON " << table
        << "(_gri_rid, _gri_lvl, _gri_beg, _gri_len)";
    return out.str();
}

extern "C" char *create_genomic_range_index_sql(const char *table, const char *rid, const char *beg,
                                                const char *end, int floor) {
    assert(table && table[0]);
    assert(rid && rid[0]);
    assert(beg && beg[0]);
    assert(end && end[0]);
    C_WRAPPER(CreateGenomicRangeIndexSQL(string(table), rid, beg, end, floor));
}

static void sqlfn_create_genomic_range_index_sql(sqlite3_context *ctx, int argc,
                                                 sqlite3_value **argv) {
    string schema_table, rid, beg, end;
    sqlite3_int64 floor = -1;
    assert(argc == 4 || argc == 5);
    ARG_TEXT(schema_table, 0)
    ARG_TEXT(rid, 1)
    ARG_TEXT(beg, 2)
    ARG_TEXT(end, 3)
    ARG_OPTIONAL(floor, 4, SQLITE_INTEGER, int64)
    SQL_WRAPPER(CreateGenomicRangeIndexSQL(schema_table, rid, beg, end, floor))
}

static pair<int, int> DetectLevelRange(sqlite3 *dbconn, const string &schema_table) {
    string table = split_schema_table(schema_table).second;

    // Detect min & max level actually occupied in the table's GRI.
    //
    // We'd like to write simply SELECT MIN(_gri_lvl), MAX(_gri_lvl) ... and trust SQLite to plan
    // an efficient skip-scan of the GRI on (_gri_rid, _gri_lvl, ...). Unfortunately it doesn't do
    // that, so instead we have to write convoluted SQL explicating the efficient plan.
    //
    // This consists of --
    // (i) recursive CTE to find the set of relevant _gri_rid (because even
    //       SELECT DISTINCT _gri_rid ... triggers a full scan of the index)
    // (ii) for each _gri_rid: pick out the min/max level with ORDER BY _gri_lvl [DESC] LIMIT 1
    // (iii) min() and  max() over the per-rid answers
    // We do the (iii) aggregation externally to ensure SQLite only does one pass through the index

    string tbl_gri = schema_table + " INDEXED BY " + table + "__gri";
    string query =
        "WITH RECURSIVE __distinct(__rid) AS\n"
        " (SELECT (SELECT _gri_rid FROM " +
        tbl_gri +
        " ORDER BY _gri_rid NULLS LAST LIMIT 1) AS __rid_0 WHERE __rid_0 IS NOT NULL\n"
        "  UNION ALL\n"
        "  SELECT (SELECT _gri_rid FROM " +
        tbl_gri +
        " WHERE _gri_rid > __rid ORDER BY _gri_rid LIMIT 1) AS __rid_i FROM __distinct WHERE __rid_i IS NOT NULL)\n"
        "SELECT\n"
        " (SELECT _gri_lvl FROM " +
        tbl_gri +
        " WHERE _gri_rid = __rid AND _gri_lvl <= 0 ORDER BY _gri_rid, _gri_lvl LIMIT 1),\n"
        " (SELECT _gri_lvl FROM " +
        tbl_gri +
        " WHERE _gri_rid = __rid AND _gri_lvl <= 0 ORDER BY _gri_rid DESC, _gri_lvl DESC LIMIT 1)\n"
        "FROM __distinct";
    _DBG << endl << query << endl;
    shared_ptr<sqlite3_stmt> stmt;
    {
        sqlite3_stmt *pStmt = nullptr;
        if (sqlite3_prepare_v3(dbconn, query.c_str(), -1, 0, &pStmt, nullptr) != SQLITE_OK) {
            throw runtime_error("GenomicSQLite: table is probably missing genomic range index; " +
                                string(sqlite3_errmsg(dbconn)));
        }
        stmt = shared_ptr<sqlite3_stmt>(pStmt, sqlite3_finalize);
    }

    sqlite3_int64 min_lvl = 15, max_lvl = 0;
    int rc;
    while ((rc = sqlite3_step(stmt.get())) == SQLITE_ROW) {
        // un-negating as we go
        if (sqlite3_column_type(stmt.get(), 0) == SQLITE_INTEGER) {
            max_lvl = max(max_lvl, 0 - sqlite3_column_int64(stmt.get(), 0));
        }
        if (sqlite3_column_type(stmt.get(), 1) == SQLITE_INTEGER) {
            min_lvl = min(min_lvl, 0 - sqlite3_column_int64(stmt.get(), 1));
        }
    }
    if (rc != SQLITE_DONE) {
        throw runtime_error("GenomicSQLite: error inspecting GRI; " +
                            string(sqlite3_errmsg(dbconn)));
    }
    if (min_lvl == 15 && max_lvl == 0) {
        // empty
        swap(min_lvl, max_lvl);
    }
    if (!(0 <= min_lvl && min_lvl <= max_lvl && max_lvl < 16)) {
        throw runtime_error("GenomicSQLite: GRI corrupted");
    }
    return std::pair<int, int>(min_lvl, max_lvl);
}

string GenomicRangeRowidsSQL(sqlite3 *dbconn, const string &indexed_table, const string &qrid,
                             const string &qbeg, const string &qend, int ceiling, int floor) {
    if (ceiling < 0) {
        auto p = DetectLevelRange(dbconn, indexed_table);
        floor = floor >= 0 ? floor : p.first;
        ceiling = p.second;
    } else if (floor == -1) {
        floor = 0;
    }
    if (!(0 <= floor && floor <= ceiling && ceiling < 16)) {
        throw invalid_argument("GenomicSQLite: invalid floor/ceiling");
    }
    string table = split_schema_table(indexed_table).second;

    ostringstream lvq; // per-level queries
    lvq << " (";
    for (int lv = ceiling; lv >= floor; --lv) {
        if (lv < ceiling) {
            lvq << "\n  UNION ALL\n  ";
        }
        const string &it = indexed_table;
        lvq << "SELECT _rowid_ FROM " << it << " INDEXED BY " << table << "__gri WHERE"
            << "\n   (" << it << "._gri_rid," << it << "._gri_lvl," << it << "._gri_beg) BETWEEN (("
            << qrid << "),-" << lv << ",(" << qbeg << ")-0x1" << string(lv, '0') << ") AND (("
            << qrid << "),-" << lv << ",(" << qend << ")-0)" // (*)
            << "\n   AND (" << it << "._gri_beg+" << it << "._gri_len) >= (" << qbeg << ")";
        /*
         * (*) For some reason, we have to obfuscate qend a little (such as with unary *+*, or by
         * adding or subtracting zero) or else SQLite generates an inefficient query plan for joins
         * (where qbeg & qend name columns of another table). Regular queries, where qbeg & qend
         * name bound parameters, don't seem to mind one way or the other.
         * We preferred subtracting zero over unary *+* to avoid any possible pitfalls from the
         * latter's type affinity stripping (Sec 8.1 in https://www.sqlite.org/optoverview.html)
         */
    }
    lvq << ")";
    string ans = "(SELECT _rowid_ FROM\n" + lvq.str() + "\n ORDER BY _rowid_)";
    _DBG << ans << endl;
    return ans;
}

extern "C" char *genomic_range_rowids_sql(sqlite3 *dbconn, const char *table, const char *qrid,
                                          const char *qbeg, const char *qend, int ceiling,
                                          int floor) {
    C_WRAPPER(GenomicRangeRowidsSQL(dbconn, string(table), (qrid && qrid[0]) ? qrid : "?1",
                                    (qbeg && qbeg[0]) ? qbeg : "?2",
                                    (qend && qend[0]) ? qend : "?3", ceiling, floor));
}

static void sqlfn_genomic_range_rowids_sql(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    string indexed_table, qrid = "?1", qbeg = "?2", qend = "?3";
    sqlite3_int64 ceiling = -1, floor = -1;
    assert(argc >= 1 && argc <= 6);
    ARG_TEXT(indexed_table, 0)
    ARG_TEXT_OPTIONAL(qrid, 1)
    ARG_TEXT_OPTIONAL(qbeg, 2)
    ARG_TEXT_OPTIONAL(qend, 3)
    ARG_OPTIONAL(ceiling, 4, SQLITE_INTEGER, int64)
    ARG_OPTIONAL(floor, 5, SQLITE_INTEGER, int64)
    SQL_WRAPPER(GenomicRangeRowidsSQL(sqlite3_context_db_handle(ctx), indexed_table, qrid, qbeg,
                                      qend, (int)ceiling, (int)floor))
}

/**************************************************************************************************
 * genomic_range_rowids() and genomic_range_index_levels() table-valued functions
 **************************************************************************************************/

// genomic_range_rowids(tableName, qrid, qbeg, qend[, ceiling [, floor]]): runs the
// GenomicRangeRowidsSQL query with passed-through arguments, caching the SQLite prepared
// statements between queries. Cached statements can be reused only for the same ceiling and floor
// values, which if omitted default to the maximum and minimum possible values (safe but
// less efficient)
class GenomicRangeRowidsCursor : public SQLiteVirtualTableCursor {
  public:
    struct table_stmt_cache {
        int ceiling = 15, floor = 0;
        vector<shared_ptr<sqlite3_stmt>> pool;
    };
    using stmt_cache = map<string, GenomicRangeRowidsCursor::table_stmt_cache>;

    GenomicRangeRowidsCursor(sqlite3 *db, stmt_cache &stmt_cache)
        : db_(db), stmt_cache_(stmt_cache) {}
    virtual ~GenomicRangeRowidsCursor() { ReturnStmtToCache(); }

    int Filter(int idxNum, const char *idxStr, int argc, sqlite3_value **argv) override {
        ReturnStmtToCache();
        table_name_.clear();
        ceiling_ = 15;
        floor_ = 0;
        if (argc < 4 || argc > 6) {
            Error("genomic_range_rowids() expects 4-6 arguments");
        } else if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
            Error("genomic_range_rowids() argument 1 should be the GRI-indexed table name");
        } else {
            try {
                table_name_ = (const char *)sqlite3_value_text(argv[0]);
                // TODO: sanitize table_name

                if (argc >= 5) {
                    if (sqlite3_value_type(argv[4]) == SQLITE_INTEGER) {
                        ceiling_ = sqlite3_value_int(argv[4]);
                    } else if (sqlite3_value_type(argv[4]) != SQLITE_NULL) {
                        throw runtime_error("genomic_range_rowids() expected integer ceiling");
                    }
                    if (argc >= 6) {
                        if (sqlite3_value_type(argv[5]) == SQLITE_INTEGER) {
                            floor_ = sqlite3_value_int(argv[5]);
                        } else if (sqlite3_value_type(argv[5]) != SQLITE_NULL) {
                            throw runtime_error("genomic_range_rowids() expected integer floor");
                        }
                    }
                }
                if (floor_ < 0 || ceiling_ > 15 || floor_ > ceiling_) {
                    throw runtime_error("genomic_range_rowids() ceiling/floor domain error");
                }

                // find or create the table_stmt_cache for this table
                auto cache_it = stmt_cache_.find(table_name_);
                if (cache_it == stmt_cache_.end()) {
                    stmt_cache_[table_name_] = table_stmt_cache();
                    cache_it = stmt_cache_.find(table_name_);
                    assert(cache_it != stmt_cache_.end());
                }
                auto &cache = cache_it->second;
                // if we've been given new level bounds then wipe the cache
                if (cache.ceiling != ceiling_ || cache.floor != floor_) {
                    cache.pool.clear();
                    cache.ceiling = ceiling_;
                    cache.floor = floor_;
                }

                if (cache.pool.empty()) {
                    // prepare new sqlite3_stmt for GRI query
                    string sql =
                        GenomicRangeRowidsSQL(db_, table_name_, "?1", "?2", "?3", ceiling_, floor_);
                    sql = sql.substr(1, sql.size() - 2); // trim parentheses
                    sqlite3_stmt *pStmt = nullptr;
                    if (sqlite3_prepare_v3(db_, sql.c_str(), -1, 0, &pStmt, nullptr) != SQLITE_OK) {
                        throw runtime_error(
                            "genomic_range_rowids(): table doesn't exsit or lacks GRI; " +
                            string(sqlite3_errmsg(db_)));
                    }
                    stmt_.reset(pStmt, sqlite3_finalize);
                } else {
                    // take existing sqlite3_stmt from the cache pool
                    stmt_ = cache.pool.back();
                    cache.pool.pop_back();
                }

                if (sqlite3_bind_value(stmt_.get(), 1, argv[1]) != SQLITE_OK ||
                    sqlite3_bind_value(stmt_.get(), 2, argv[2]) != SQLITE_OK ||
                    sqlite3_bind_value(stmt_.get(), 3, argv[3]) != SQLITE_OK) {
                    throw runtime_error("GenomicSQLite: error binding GRI query parameters");
                }
                // later we'll ReturnStmtToCache()
                return Next(); // step to first result row
            } catch (std::exception &exn) {
                Error(exn.what());
            }
        }
        return SQLITE_ERROR;
    }

    int Next() override {
        if (stmt_) {
            int rc = sqlite3_step(stmt_.get());
            if (rc != SQLITE_ROW) {
                if (rc != SQLITE_DONE) {
                    assert(rc != SQLITE_OK);
                    stmt_.reset();
                    return rc;
                }
                ReturnStmtToCache(); // successful EOF
            }
        }
        return SQLITE_OK;
    }

    int Eof() override { return !stmt_; }

    int Column(sqlite3_context *ctx, int colno) override {
        assert(stmt_);
        if (!stmt_)
            return SQLITE_ERROR;
        if (colno == 0) {
            sqlite3_result_value(ctx, sqlite3_column_value(stmt_.get(), 0));
        } else {
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    int Rowid(sqlite_int64 *pRowid) override {
        assert(stmt_);
        *pRowid = sqlite3_column_int64(stmt_.get(), 0);
        return SQLITE_OK;
    }

  private:
    sqlite3 *db_;
    stmt_cache &stmt_cache_;

    shared_ptr<sqlite3_stmt> stmt_;
    string table_name_;
    int ceiling_ = 15, floor_ = 0;

    void ReturnStmtToCache() {
        if (stmt_) {
            assert(floor_ >= 0 && ceiling_ >= floor_ && ceiling_ <= 15);
            auto &cache = stmt_cache_[table_name_];
            if (cache.ceiling == ceiling_ && cache.floor == floor_) {
                sqlite3_reset(stmt_.get());
                cache.pool.push_back(stmt_);
            }
            stmt_.reset();
        }
    }
};

class GenomicRangeRowidsTVF : public SQLiteVirtualTable {
    // cache is shared at the module level. we don't need a mutex as each connection will have its
    // own instance.
    GenomicRangeRowidsCursor::stmt_cache stmt_cache_;

    unique_ptr<SQLiteVirtualTableCursor> NewCursor() override {
        return unique_ptr<SQLiteVirtualTableCursor>(new GenomicRangeRowidsCursor(db_, stmt_cache_));
    }

  public:
    GenomicRangeRowidsTVF(sqlite3 *db) : SQLiteVirtualTable(db) {}

    int BestIndex(sqlite3_index_info *info) override {
        int rc = BestIndexTVF(info, 1, 4, 6);
        if (rc != SQLITE_OK)
            return rc;
        info->orderByConsumed =
            info->nOrderBy == 0 ||
            (info->nOrderBy == 1 && info->aOrderBy[0].iColumn == 0 && !info->aOrderBy[0].desc);
        return SQLITE_OK;
    }

    static int Connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                       sqlite3_vtab **ppVTab, char **pzErr) {
        return SQLiteVirtualTable::SimpleConnect(
            db, pAux, argc, argv, ppVTab, pzErr,
            unique_ptr<SQLiteVirtualTable>(new GenomicRangeRowidsTVF(db)),
            "CREATE TABLE genomic_range_rowids(_rowid_ INTEGER, tableName HIDDEN, qrid HIDDEN, qbeg HIDDEN, qend HIDDEN, ceiling HIDDEN, floor HIDDEN)");
    }
};

// genomic_range_index_levels(tableName): inspect the GRI to detect the gri_ceiling and gri_floor
// of the (current snapshot of) the given table. (returns just one row)
class GenomicRangeIndexLevelsCursor : public SQLiteVirtualTableCursor {
    sqlite3 *db_;
    sqlite_int64 ceiling_ = -1, floor_ = -1;

  public:
    GenomicRangeIndexLevelsCursor(sqlite3 *db) : db_(db) {}

    int Filter(int idxNum, const char *idxStr, int argc, sqlite3_value **argv) override {
        ceiling_ = floor_ = -1;
        if (argc != 1) {
            Error("genomic_range_index_levels() expects 1 argument");
        } else if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
            Error("genomic_range_index_levels() expects table name");
        } else {
            string table_name = (const char *)sqlite3_value_text(argv[0]);
            // TODO: sanitize table_name
            try {
                auto p = DetectLevelRange(db_, table_name);
                floor_ = p.first;
                ceiling_ = p.second;
                assert(floor_ >= 0 && ceiling_ >= floor_ && ceiling_ <= 15);
                return SQLITE_OK;
            } catch (std::exception &exn) {
                Error(exn.what());
            }
        }
        return SQLITE_ERROR;
    }

    int Next() override {
        ceiling_ = floor_ = -1;
        return SQLITE_OK;
    }

    int Eof() override { return floor_ < 0; }

    int Column(sqlite3_context *ctx, int colno) override {
        assert(floor_ >= 0 && ceiling_ >= floor_);
        switch (colno) {
        case 0:
            sqlite3_result_int64(ctx, ceiling_);
            break;
        case 1:
            sqlite3_result_int64(ctx, floor_);
            break;
        default:
            sqlite3_result_null(ctx);
        }
        return SQLITE_OK;
    }

    int Rowid(sqlite_int64 *pRowid) override {
        assert(floor_ >= 0);
        *pRowid = 1;
        return SQLITE_OK;
    }
};

class GenomicRangeIndexLevelsTVF : public SQLiteVirtualTable {
    unique_ptr<SQLiteVirtualTableCursor> NewCursor() override {
        return unique_ptr<SQLiteVirtualTableCursor>(new GenomicRangeIndexLevelsCursor(db_));
    }

  public:
    GenomicRangeIndexLevelsTVF(sqlite3 *db) : SQLiteVirtualTable(db) {}

    int BestIndex(sqlite3_index_info *info) override {
        int rc = BestIndexTVF(info, 2, 1, 1);
        if (rc != SQLITE_OK)
            return rc;
        info->orderByConsumed = 1;
        info->estimatedCost = 1;
        info->estimatedRows = 1;
        info->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
        return SQLITE_OK;
    }

    static int Connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                       sqlite3_vtab **ppVTab, char **pzErr) {
        return SQLiteVirtualTable::SimpleConnect(
            db, pAux, argc, argv, ppVTab, pzErr,
            unique_ptr<SQLiteVirtualTable>(new GenomicRangeIndexLevelsTVF(db)),
            "CREATE TABLE genomic_range_index_levels(_gri_ceiling INTEGER, _gri_floor INTEGER, tableName HIDDEN)");
    }
};

/**************************************************************************************************
 * reference sequence metadata (_gri_refseq) helpers
 **************************************************************************************************/

static string gri_refseq_ddl(const string &schema) {
    string schema_prefix;
    if (!schema.empty()) {
        schema_prefix = schema + ".";
    }
    ostringstream out;
    out << "CREATE TABLE IF NOT EXISTS " << schema_prefix << "_gri_refseq"
        << "(_gri_rid INTEGER NOT NULL PRIMARY KEY, gri_refseq_name TEXT NOT NULL, gri_assembly TEXT,"
        << " gri_refget_id TEXT UNIQUE, gri_refseq_length INTEGER NOT NULL, gri_refseq_meta_json TEXT NOT NULL DEFAULT '{}', "
        << "UNIQUE(gri_assembly,gri_refseq_name))"
        << ";\nCREATE INDEX IF NOT EXISTS " << schema_prefix << "_gri_refseq_name ON "
        << "_gri_refseq(gri_refseq_name)";
    return out.str();
}

string PutGenomicReferenceSequenceSQL(const string &name, sqlite3_int64 length,
                                      const string &assembly, const string &refget_id,
                                      const string &meta_json, sqlite3_int64 rid,
                                      const string &schema, bool with_ddl) {
    string schema_prefix;
    if (!schema.empty()) {
        schema_prefix = schema + ".";
    }
    ostringstream out;
    if (with_ddl) {
        out << gri_refseq_ddl(schema) << ";\n";
    }
    out << "INSERT INTO " << schema_prefix
        << "_gri_refseq(_gri_rid,gri_refseq_name,gri_assembly,gri_refget_id,gri_refseq_length,gri_refseq_meta_json) VALUES("
        << (rid >= 0 ? std::to_string(rid) : "NULL") << "," << sqlquote(name) << ","
        << (assembly.empty() ? "NULL" : sqlquote(assembly)) << ","
        << (refget_id.empty() ? "NULL" : sqlquote(refget_id)) << "," << std::to_string(length)
        << "," << sqlquote(meta_json.empty() ? string("{}") : meta_json) << ")";
    return out.str();
}

string PutGenomicReferenceSequenceSQL(const string &name, sqlite3_int64 length,
                                      const string &assembly, const string &refget_id,
                                      const string &meta_json, sqlite3_int64 rid,
                                      const string &schema) {
    return PutGenomicReferenceSequenceSQL(name, length, assembly, refget_id, meta_json, rid, schema,
                                          true);
}

extern "C" char *put_genomic_reference_sequence_sql(const char *name, sqlite3_int64 length,
                                                    const char *assembly, const char *refget_id,
                                                    const char *meta_json, sqlite3_int64 rid,
                                                    const char *schema) {
    C_WRAPPER(PutGenomicReferenceSequenceSQL(
        name, length, assembly ? assembly : "", refget_id ? refget_id : "",
        meta_json ? meta_json : "{}", rid, schema ? schema : "", true));
}

static void sqlfn_put_genomic_reference_sequence_sql(sqlite3_context *ctx, int argc,
                                                     sqlite3_value **argv) {
    string name, assembly, refget_id, meta_json = "{}", schema;
    sqlite3_int64 length, rid = -1;
    assert(argc >= 2 && argc <= 7);
    ARG_TEXT(name, 0)
    ARG(length, 1, SQLITE_INTEGER, int64)
    ARG_TEXT_OPTIONAL(assembly, 2)
    ARG_TEXT_OPTIONAL(refget_id, 3)
    ARG_TEXT_OPTIONAL(meta_json, 4)
    ARG_OPTIONAL(rid, 5, SQLITE_INTEGER, int64)
    ARG_TEXT_OPTIONAL(schema, 6);
    SQL_WRAPPER(
        PutGenomicReferenceSequenceSQL(name, length, assembly, refget_id, meta_json, rid, schema))
}

string PutGenomicReferenceAssemblySQL(const string &assembly, const string &schema) {
    const hardcoded_refseq_t *hardcoded_refseqs = nullptr;
    size_t nr_hardcoded_refseqs = 0;
    if (assembly == "GRCh38_no_alt_analysis_set") {
        hardcoded_refseqs = GRCh38_no_alt_analysis_set;
        nr_hardcoded_refseqs = sizeof(GRCh38_no_alt_analysis_set) / sizeof(hardcoded_refseq_t);
    } else {
        throw std::invalid_argument("put_genomic_reference_assembly_sql: unknown assembly");
    }
    ostringstream out;
    for (size_t i = 0; i < nr_hardcoded_refseqs; ++i) {
        if (i) {
            out << ";\n";
        }
        const hardcoded_refseq_t &hcrs = hardcoded_refseqs[i];
        out << PutGenomicReferenceSequenceSQL(string(hcrs.name), hcrs.length, assembly,
                                              hcrs.refget_id ? string(hcrs.refget_id) : string(),
                                              string("{}"), -1, schema, i == 0);
    }
    return out.str();
}

extern "C" char *put_genomic_reference_assembly_sql(const char *assembly, const char *schema) {
    C_WRAPPER(PutGenomicReferenceAssemblySQL(string(assembly), schema ? schema : ""));
}

static void sqlfn_put_genomic_reference_assembly_sql(sqlite3_context *ctx, int argc,
                                                     sqlite3_value **argv) {
    string assembly, schema;
    assert(argc == 1 || argc == 2);
    ARG_TEXT(assembly, 0)
    ARG_TEXT_OPTIONAL(schema, 1)
    SQL_WRAPPER(PutGenomicReferenceAssemblySQL(assembly, schema))
}

map<long long, gri_refseq_t>
GetGenomicReferenceSequencesByRid(sqlite3 *dbconn, const string &assembly, const string &schema) {
    map<long long, gri_refseq_t> ans;
    string schema_prefix = schema.empty() ? "" : (schema + ".");

    string query =
        "SELECT _gri_rid, gri_refseq_name, gri_refseq_length, gri_assembly, gri_refget_id, gri_refseq_meta_json FROM " +
        schema_prefix + "_gri_refseq";
    if (!assembly.empty()) {
        query += " WHERE gri_assembly = ?";
    }
    shared_ptr<sqlite3_stmt> stmt;
    {
        sqlite3_stmt *pStmt = nullptr;
        if (sqlite3_prepare_v3(dbconn, query.c_str(), -1, 0, &pStmt, nullptr) != SQLITE_OK) {
            throw runtime_error("GenomicSQLite: error querying reference sequences");
        }
        stmt = shared_ptr<sqlite3_stmt>(pStmt, sqlite3_finalize);
    }
    if (!assembly.empty()) {
        if (sqlite3_bind_text(stmt.get(), 1, assembly.c_str(), -1, nullptr) != SQLITE_OK) {
            throw runtime_error("GenomicSQLite: error querying reference sequences");
        }
    }
    int rc;
    while ((rc = sqlite3_step(stmt.get())) == SQLITE_ROW) {
        gri_refseq_t item;
        item.rid = sqlite3_column_int64(stmt.get(), 0);
        item.name = (const char *)sqlite3_column_text(stmt.get(), 1);
        item.length = sqlite3_column_int64(stmt.get(), 2);
        if (sqlite3_column_type(stmt.get(), 3) == SQLITE_TEXT) {
            item.assembly = (const char *)sqlite3_column_text(stmt.get(), 3);
        }
        if (sqlite3_column_type(stmt.get(), 4) == SQLITE_TEXT) {
            item.refget_id = (const char *)sqlite3_column_text(stmt.get(), 4);
        }
        if (sqlite3_column_type(stmt.get(), 4) == SQLITE_TEXT) {
            item.meta_json = (const char *)sqlite3_column_text(stmt.get(), 5);
        }
        ans[item.rid] = item;
    }
    if (rc != SQLITE_DONE) {
        throw runtime_error("GenomicSQLite: error querying reference sequences");
    }
    return ans;
}

map<string, gri_refseq_t>
GetGenomicReferenceSequencesByName(sqlite3 *dbconn, const string &assembly, const string &schema) {
    map<string, gri_refseq_t> ans;
    for (const auto &p : GetGenomicReferenceSequencesByRid(dbconn, assembly, schema)) {
        const gri_refseq_t &item = p.second;
        if (ans.find(item.name) != ans.end()) {
            throw runtime_error("GenomicSQLite: reference sequence names are not unique");
        }
        ans[item.name] = item;
    }
    return ans;
}

/**************************************************************************************************
 * SQL helper functions for compactly storing DNA/RNA sequences
 **************************************************************************************************/

/*
crumbs = []
for c in (chr(i) for i in range(256)):
    if c in "TtUu":
        crumbs.append(0)
    elif c in "Cc":
        crumbs.append(1)
    elif c in "Aa":
        crumbs.append(2)
    elif c in "Gg":
        crumbs.append(3)
    else:
        crumbs.append(0xFF)
print(crumbs)
*/
const unsigned char dna_crumb_table[] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 2,    0xFF, 1,    0xFF, 0xFF, 0xFF, 3,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    0,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 2,    0xFF, 1,    0xFF, 0xFF, 0xFF, 3,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0,    0,    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

static vector<unsigned char> dna_twobit(const char *seq, size_t len) {
    if (!len) {
        return {};
    }

    // header byte: the low two bits specify how many crumbs at the end of the buffer must be
    // ignored by the decoder (0, 1, 2, or 3)
    auto trailing_crumbs = (4 - len % 4) % 4;
    assert(trailing_crumbs >= 0 && trailing_crumbs <= 3);

    vector<unsigned char> ans;
    ans.reserve(len / 4 + 2);
    ans.push_back((unsigned char)trailing_crumbs);

    unsigned char byte = 0;
    for (size_t i = 0; i < len; ++i) {
        const char c_i = seq[i];
        if (c_i <= 0) {
            throw runtime_error("non-ASCII input to dna_twobit()");
        }
        assert(c_i >= 0 && c_i < 128);
        const unsigned char crumb = dna_crumb_table[c_i];
        if (crumb > 3) {
            return {};
        }
        assert((byte >> 6) == 0);
        byte = (byte << 2) | crumb;
        if (i % 4 == 3) {
            ans.push_back(byte);
            byte = 0;
        }
    }

    if (trailing_crumbs) {
        assert((byte >> (2 * (4 - trailing_crumbs))) == 0);
        byte <<= (2 * trailing_crumbs);
        ans.push_back(byte);
    } else {
        assert(byte == 0);
    }

    return ans;
}

static void sqlfn_dna_twobit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return sqlite3_result_null(ctx);
    }
    ARG_TYPE(0, SQLITE_TEXT);
    auto seqlen = sqlite3_value_bytes(argv[0]);
    assert(seqlen >= 0);
    const char *seq = (const char *)sqlite3_value_text(argv[0]);

    try {
        auto crumbs = dna_twobit(seq, (size_t)seqlen);
        if (crumbs.empty()) {
            sqlite3_result_value(ctx, argv[0]);
        } else {
            sqlite3_result_blob64(ctx, crumbs.data(), crumbs.size(), SQLITE_TRANSIENT);
        }
    } catch (std::bad_alloc &) {
        sqlite3_result_error_nomem(ctx);
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

static void sqlfn_rna_twobit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    sqlfn_dna_twobit(ctx, argc, argv);
}

static size_t twobit_dna_length(const void *crumbs, size_t sz) {
    if (sz < 2) {
        return 0;
    }
    const unsigned char *bytes = (const unsigned char *)crumbs;
    unsigned char trailing_crumbs = bytes[0] & 0b11;
    size_t ans = 4 * (sz - 1) - trailing_crumbs;
    if (ans > 2147483647) { // https://www.sqlite.org/limits.html
        throw SQLite::Exception("BLOB is too large for twobit_dna()", SQLITE_TOOBIG);
    }
    return ans;
}

static void sqlfn_twobit_dna_length(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    assert(argc == 1);
    try {
        if (sqlite3_value_type(argv[0]) == SQLITE_BLOB) {
            size_t sz = sqlite3_value_bytes(argv[0]);
            sqlite3_result_int64(ctx, twobit_dna_length(sqlite3_value_blob(argv[0]), sz));
        } else if (sqlite3_value_type(argv[0]) == SQLITE_TEXT) {
            sqlite3_result_int64(ctx, sqlite3_value_bytes(argv[0]));
        } else if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
        } else {
            sqlite3_result_error(ctx, "twobit_dna_length() expected BLOB or TEXT", -1);
        }
    } catch (std::bad_alloc &) {
        sqlite3_result_error_nomem(ctx);
    } catch (SQLite::Exception &exn) {
        if (exn.getErrorCode() == SQLITE_TOOBIG) {
            sqlite3_result_error_toobig(ctx);
        } else {
            sqlite3_result_error(ctx, exn.getErrorStr(), -1);
        }
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

static void sqlfn_twobit_rna_length(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    sqlfn_twobit_dna_length(ctx, argc, argv);
}

const char crumb_dna_table[] = {'T', 'C', 'A', 'G'};
const char crumb_rna_table[] = {'U', 'C', 'A', 'G'};

static void twobit_dna_impl(sqlite3_context *ctx, int argc, sqlite3_value **argv, bool rna) {
    assert(argc >= 1 && argc <= 3);
    bool blob = sqlite3_value_type(argv[0]) == SQLITE_BLOB;
    if (!blob) {
        if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
            return sqlite3_result_null(ctx);
        }
        if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
            return sqlite3_result_error(ctx, "twobit_dna() expected BLOB or TEXT", -1);
        }
    }

    // Y and Z are as https://sqlite.org/lang_corefunc.html#substr
    ssize_t Y = 0, Zval = -1;
    ARG_OPTIONAL(Y, 1, SQLITE_INTEGER, int);
    ARG_OPTIONAL(Zval, 2, SQLITE_INTEGER, int);
    ssize_t *Z = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) ? &Zval : nullptr;

    try {
        size_t sz = (size_t)sqlite3_value_bytes(argv[0]);
        size_t len = blob ? twobit_dna_length(sqlite3_value_blob(argv[0]), sz) : sz;

        // based on Y and Z, explicate the zero-based offset & length of the desired substring.
        // see this convoluted logic:
        //   https://github.com/sqlite/sqlite/blob/d924e7bc78a4ca604bce0f8d9d0390d3feddba01/src/func.c#L299
        size_t sub_ofs = 0;
        if (Y > 0) {
            sub_ofs = Y - 1;
        } else if (Y < 0) {
            sub_ofs = (size_t)max(0L, Y + (ssize_t)len);
        }
        if (sub_ofs > len) {
            return sqlite3_result_text(ctx, "", 0, SQLITE_STATIC);
        }
        size_t sub_len = len - sub_ofs;
        if (Z) {
            if (*Z < 0) {
                sub_len = (size_t)(0 - *Z);
                sub_len = min(sub_ofs, sub_len);
                sub_ofs -= sub_len;
            } else {
                sub_len = (size_t)*Z;
                if (Y == 0) {
                    sub_len -= min(1UL, sub_len);
                } else if (0 - Y > (ssize_t)len) {
                    sub_len -= min(sub_len, 0 - Y - len);
                }
                sub_len = min(sub_len, len - sub_ofs);
            }
        }
        if (sub_len == 0) {
            return sqlite3_result_text(ctx, "", 0, SQLITE_STATIC);
        }
        assert(sub_ofs + sub_len <= len);

        if (blob) {
            // decode crumbs
            string ans;
            ans.reserve(sub_len);
            const char *table = rna ? crumb_rna_table : crumb_dna_table;
            const unsigned char *byte =
                ((const unsigned char *)sqlite3_value_blob(argv[0])) + 1 + sub_ofs / 4;
            for (auto which_crumb = sub_ofs % 4; sub_len > 0; --sub_len) {
                unsigned char crumb = (*byte >> (2 * (3 - which_crumb))) & 0b11;
                ans.push_back(table[crumb]);
                if (++which_crumb == 4) {
                    ++byte;
                    which_crumb = 0;
                }
            }
            sqlite3_result_text(ctx, ans.c_str(), ans.size(), SQLITE_TRANSIENT);
        } else if (sub_ofs == 0 && sub_len == len) {
            // pass through complete text
            sqlite3_result_value(ctx, argv[0]);
        } else {
            // substr of text
            sqlite3_result_text(ctx, ((const char *)sqlite3_value_text(argv[0])) + sub_ofs, sub_len,
                                SQLITE_TRANSIENT);
        }
    } catch (std::bad_alloc &) {
        sqlite3_result_error_nomem(ctx);
    } catch (SQLite::Exception &exn) {
        if (exn.getErrorCode() == SQLITE_TOOBIG) {
            sqlite3_result_error_toobig(ctx);
        } else {
            sqlite3_result_error(ctx, exn.getErrorStr(), -1);
        }
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

static void sqlfn_twobit_dna(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    twobit_dna_impl(ctx, argc, argv, false);
}

static void sqlfn_twobit_rna(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    twobit_dna_impl(ctx, argc, argv, true);
}

/**************************************************************************************************
 * SQLite loadable extension initialization
 **************************************************************************************************/

extern "C" int genomicsqliteJson1Register(sqlite3 *db);

static int register_genomicsqlite_functions(sqlite3 *db, const char **pzErrMsg,
                                            const sqlite3_api_routines *pApi) {
#define FPNM(fn) #fn, sqlfn_##fn
    static const struct {
        const char *fn;
        void (*fp)(sqlite3_context *, int, sqlite3_value **);
        int nArg;
        int flags;
    } fntab[] = {{FPNM(genomicsqlite_version), 0, 0},
                 {FPNM(genomicsqlite_default_config_json), 0, 0},
                 {FPNM(genomicsqlite_uri), 1, 0},
                 {FPNM(genomicsqlite_uri), 2, 0},
                 {FPNM(genomicsqlite_tuning_sql), 0, 0},
                 {FPNM(genomicsqlite_tuning_sql), 1, 0},
                 {FPNM(genomicsqlite_attach_sql), 2, 0},
                 {FPNM(genomicsqlite_attach_sql), 3, 0},
                 {FPNM(genomicsqlite_vacuum_into_sql), 1, 0},
                 {FPNM(genomicsqlite_vacuum_into_sql), 2, 0},
                 {FPNM(create_genomic_range_index_sql), 4, 0},
                 {FPNM(create_genomic_range_index_sql), 5, 0},
                 {FPNM(genomic_range_rowids_sql), 1, 0},
                 {FPNM(genomic_range_rowids_sql), 2, 0},
                 {FPNM(genomic_range_rowids_sql), 3, 0},
                 {FPNM(genomic_range_rowids_sql), 4, 0},
                 {FPNM(genomic_range_rowids_sql), 5, 0},
                 {FPNM(genomic_range_rowids_sql), 6, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 2, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 3, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 4, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 5, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 6, 0},
                 {FPNM(put_genomic_reference_sequence_sql), 7, 0},
                 {FPNM(put_genomic_reference_assembly_sql), 1, 0},
                 {FPNM(put_genomic_reference_assembly_sql), 2, 0},
                 {FPNM(dna_twobit), 1, SQLITE_DETERMINISTIC},
                 {FPNM(rna_twobit), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna_length), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna_length), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 2, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 3, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 2, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 3, SQLITE_DETERMINISTIC}};

    int rc;
    for (int i = 0; i < sizeof(fntab) / sizeof(fntab[0]); ++i) {
        rc =
            sqlite3_create_function_v2(db, fntab[i].fn, fntab[i].nArg, SQLITE_UTF8 | fntab[i].flags,
                                       nullptr, fntab[i].fp, nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            if (pzErrMsg) {
                *pzErrMsg =
                    sqlite3_mprintf("Genomics Extension failed to register %s", fntab[i].fn);
            }
            return rc;
        }
    }
    rc = RegisterSQLiteVirtualTable<GenomicRangeIndexLevelsTVF>(db, "genomic_range_index_levels");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg =
                sqlite3_mprintf("Genomics Extension failed to register genomic_range_index_levels");
        }
        return rc;
    }
    rc = RegisterSQLiteVirtualTable<GenomicRangeRowidsTVF>(db, "genomic_range_rowids");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg =
                sqlite3_mprintf("Genomics Extension failed to register genomic_range_rowids");
        }
        return rc;
    }
    // genomicsqliteJson1Register() may return SQLITE_BUSY if JSON1 (possibly another version
    // thereof) is already loaded, and the extension is being loaded by SELECT load_extension().
    // That is tolerable.
    rc = genomicsqliteJson1Register(db);
    if (rc != SQLITE_OK && rc != SQLITE_BUSY) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("Genomics Extension failed to register JSON1");
        }
        return rc;
    }
    return SQLITE_OK;
}

/*
** This routine is called when the extension is loaded.
*/
extern "C" int sqlite3_genomicsqlite_init(sqlite3 *db, char **pzErrMsg,
                                          const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    // The newest SQLite feature currently required is "Generated Columns"
    const int MIN_SQLITE_VERSION_NUMBER = 3031000;
    const string MIN_SQLITE_VERSION = "3.31.0";
    if (sqlite3_libversion_number() < MIN_SQLITE_VERSION_NUMBER) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf(
                "SQLite library version %s is older than %s required by Genomics Extension %s",
                sqlite3_libversion(), MIN_SQLITE_VERSION.c_str(), GIT_REVISION);
        }
        return SQLITE_ERROR;
    }

    int rc = (new ZstdVFS())->Register("zstd");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("Genomics Extension failed initializing zstd_vfs");
        }
        return rc;
    }
    rc = register_genomicsqlite_functions(db, (const char **)pzErrMsg, pApi);
    if (rc != SQLITE_OK)
        return rc;
    rc = sqlite3_auto_extension((void (*)(void))register_genomicsqlite_functions);
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("Genomics Extension failed sqlite3_auto_extension");
        }
        return rc;
    }
    return SQLITE_OK_LOAD_PERMANENTLY;
}
