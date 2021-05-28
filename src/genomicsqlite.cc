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
#include "web_vfs.h"
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
    "outer_page_KiB": 32,
    "web_log": 2,
    "web_insecure": false,
    "web_dbi_url": "",
    "web_nodbi": false
})";
}

// Helper for extracting options from JSON configuration. Uses JSON1 for parsing.
// Don't use SQLiteCpp in case it uses a different SQLite library without JSON1.
class ConfigParser {
    sqlite3 *db_ = nullptr;
    sqlite3_stmt *patch_ = nullptr, *extract_ = nullptr;

  public:
    ConfigParser(const string &config_json) {
        int rc;
        if ((rc = sqlite3_open_v2(":memory:", &db_, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
                                  nullptr)) != SQLITE_OK) {
            throw SQLite::Exception("GenomicSQLite::ConfigParser()::sqlite3_open_v2()", rc);
        }

        string default_json = GenomicSQLiteDefaultConfigJSON();
        const char *merged_config_json = default_json.c_str();
        if (config_json.size() > 2) { // "{}"
            // merge config_json into defaults
            if ((rc = sqlite3_prepare_v2(db_, "SELECT json_patch(?,?)", -1, &patch_, nullptr)) !=
                SQLITE_OK)
                throw SQLite::Exception(db_, rc);
            if ((rc = sqlite3_bind_text(patch_, 1, default_json.c_str(), -1, SQLITE_TRANSIENT)) !=
                SQLITE_OK)
                throw SQLite::Exception(db_, rc);
            if ((rc = sqlite3_bind_text(patch_, 2, config_json.c_str(), -1, SQLITE_TRANSIENT)) !=
                SQLITE_OK)
                throw SQLite::Exception(db_, rc);
            if ((rc = sqlite3_step(patch_)) != SQLITE_ROW ||
                !(merged_config_json = (const char *)sqlite3_column_text(patch_, 0)) ||
                merged_config_json[0] != '{') {
                throw SQLite::Exception("error parsing config JSON", rc);
            }
        }

        if ((rc = sqlite3_prepare_v2(db_, "SELECT json_extract(?,?)", -1, &extract_, nullptr)) !=
            SQLITE_OK)
            throw SQLite::Exception(db_, rc);
        if ((rc = sqlite3_bind_text(extract_, 1, merged_config_json, -1, SQLITE_TRANSIENT)) !=
            SQLITE_OK)
            throw SQLite::Exception(db_, rc);
    }

    virtual ~ConfigParser() {
        if (extract_) {
            sqlite3_finalize(extract_);
        }
        if (patch_) {
            sqlite3_finalize(patch_);
        }
        if (db_) {
            sqlite3_close_v2(db_);
        }
    }

    string GetString(const char *path, const char *default_string = nullptr) {
        int rc;
        if ((rc = sqlite3_reset(extract_)) != SQLITE_OK)
            throw SQLite::Exception(db_, rc);
        if ((rc = sqlite3_bind_text(extract_, 2, path, -1, SQLITE_TRANSIENT)) != SQLITE_OK)
            throw SQLite::Exception(db_, rc);
        if (sqlite3_step(extract_) != SQLITE_ROW)
            throw SQLite::Exception(string("error parsing config ") + path, SQLITE_EMPTY);
        if (sqlite3_column_type(extract_, 0) == SQLITE_NULL && default_string)
            return default_string;
        if (sqlite3_column_type(extract_, 0) != SQLITE_TEXT)
            throw SQLite::Exception(string("expected text for config ") + path, SQLITE_MISMATCH);
        return (const char *)sqlite3_column_text(extract_, 0);
    }

    int GetInt(const char *path) {
        int rc;
        if ((rc = sqlite3_reset(extract_)) != SQLITE_OK)
            throw SQLite::Exception(db_, rc);
        if ((rc = sqlite3_bind_text(extract_, 2, path, -1, SQLITE_TRANSIENT)) != SQLITE_OK)
            throw SQLite::Exception(db_, rc);
        if (sqlite3_step(extract_) != SQLITE_ROW)
            throw SQLite::Exception(string("error parsing config ") + path, SQLITE_EMPTY);
        if (sqlite3_column_type(extract_, 0) != SQLITE_INTEGER)
            throw SQLite::Exception(string("expected integer for config ") + path, SQLITE_MISMATCH);
        return sqlite3_column_int(extract_, 0);
    }

    bool GetBool(const char *path) { return GetInt(path) != 0; }
};

extern "C" char *genomicsqlite_default_config_json() { C_WRAPPER(GenomicSQLiteDefaultConfigJSON()) }

static void sqlfn_genomicsqlite_default_config_json(sqlite3_context *ctx, int argc,
                                                    sqlite3_value **argv){
    SQL_WRAPPER(GenomicSQLiteDefaultConfigJSON())}

string GenomicSQLiteURI(const string &dbfile, const string &config_json = "") {
    ConfigParser cfg(config_json);

    bool web = dbfile.substr(0, 5) == "http:" || dbfile.substr(0, 6) == "https:";
    ostringstream uri;
    uri << "file:" << (web ? "/__web__" : SQLiteNested::urlencode(dbfile, true)) << "?vfs=zstd";
    if (web) {
        uri << "&mode=ro&immutable=1&web_url=" << SQLiteNested::urlencode(dbfile)
            << "&web_log=" << cfg.GetInt("$.web_log");
        if (cfg.GetBool("$.web_insecure")) {
            uri << "&web_insecure=1";
        }
        if (cfg.GetBool("$.web_nodbi")) {
            uri << "&web_nodbi=1";
        } else {
            auto web_dbi_url = cfg.GetString("$.web_dbi_url");
            if (!web_dbi_url.empty()) {
                uri << "&web_dbi_url=" << SQLiteNested::urlencode(web_dbi_url);
            }
        }
    }
    int threads = cfg.GetInt("$.threads");
    uri << "&outer_cache_size=" << to_string(-64 * cfg.GetInt("$.page_cache_MiB"))
        << "&threads=" << to_string(threads);
    if (threads > 1 && cfg.GetInt("$.inner_page_KiB") < 16 && !cfg.GetBool("$.force_prefetch")) {
        // prefetch is usually counterproductive if inner_page_KiB < 16
        uri << "&noprefetch=1";
    }
    if (!web) {
        string mode = cfg.GetString("$.mode", "");
        if (!mode.empty()) {
            uri << "&mode=" << mode;
        }
        uri << "&outer_page_size=" << to_string(cfg.GetInt("$.outer_page_KiB") * 1024);
        uri << "&level=" << to_string(cfg.GetInt("$.zstd_level"));
        if (cfg.GetBool("$.immutable")) {
            uri << "&immutable=1";
        }
        if (cfg.GetBool("$.unsafe_load")) {
            uri << "&nolock=1&outer_unsafe";
        }
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
    ConfigParser cfg(config_json);

    string schema_prefix;
    if (!schema.empty()) {
        schema_prefix = schema + ".";
    }
    map<string, string> pragmas;
    pragmas[schema_prefix + "cache_size"] = to_string(-960 * cfg.GetInt("$.page_cache_MiB"));
    pragmas[schema_prefix + "max_page_count"] = "2147483646";
    if (schema_prefix.empty()) {
        int threads = cfg.GetInt("$.threads");
        pragmas["threads"] =
            to_string(threads >= 0 ? threads : std::min(8, (int)thread::hardware_concurrency()));
    }
    if (cfg.GetBool("$.unsafe_load")) {
        pragmas[schema_prefix + "journal_mode"] = "OFF";
        pragmas[schema_prefix + "synchronous"] = "OFF";
        pragmas[schema_prefix + "locking_mode"] = "EXCLUSIVE";
    } else {
        // txn rollback after a crash is handled by zstd_vfs's "outer" database, so we can set
        // the following to avoid writing redundant journals, without loss of safety.
        pragmas[schema_prefix + "journal_mode"] = "MEMORY";
    }
    ostringstream out;
    // must go first:
    out << "PRAGMA " << schema_prefix
        << "page_size=" << to_string(cfg.GetInt("$.inner_page_KiB") * 1024);
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

void GenomicSQLiteInit(int (*open_v2)(const char *, sqlite3 **, int, const char *),
                       int (*enable_load_extension)(sqlite3 *, int),
                       int (*load_extension)(sqlite3 *, const char *, const char *, char **)) {
    static bool ext_loaded = false;
    if (!ext_loaded) {

        sqlite3 *memdb = nullptr;
        int rc = open_v2(":memory:", &memdb, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_close(memdb);
            throw runtime_error("GenomicSQLiteInit() unable to open temporary SQLite connection");
        }
        enable_load_extension(memdb, 1);
        char *zErrMsg = nullptr;
        rc = load_extension(memdb, "libgenomicsqlite", nullptr, &zErrMsg);
        if (rc != SQLITE_OK) {
            string err = "GenomicSQLiteInit() unable to load the extension" +
                         (zErrMsg ? ": " + string(zErrMsg) : "");
            sqlite3_free(zErrMsg);
            sqlite3_close(memdb);
            throw runtime_error(err);
        }
        sqlite3_free(zErrMsg);
        rc = sqlite3_close(memdb);
        if (rc != SQLITE_OK) {
            throw runtime_error("GenomicSQLiteInit() unable to close temporary SQLite connection");
        }
    }
    if (open_v2 != sqlite3_api->open_v2) {
        throw std::runtime_error(
            "GenomicSQLiteInit() saw inconsistent libsqlite3/libgenomicsqlite library linkage in this process");
    }
    ext_loaded = true;
}

extern "C" int genomicsqlite_init(int (*open_v2)(const char *, sqlite3 **, int, const char *),
                                  int (*enable_load_extension)(sqlite3 *, int),
                                  int (*load_extension)(sqlite3 *, const char *, const char *,
                                                        char **),
                                  char **pzErrMsg) {
    try {
        GenomicSQLiteInit(open_v2, enable_load_extension, load_extension);
        return SQLITE_OK;
    } catch (std::bad_alloc &) {
        return SQLITE_NOMEM;
    } catch (std::exception &exn) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("%s", exn.what());
        }
        return SQLITE_ERROR;
    }
}

int GenomicSQLiteOpen(const string &dbfile, sqlite3 **ppDb, string &errmsg_out, int flags,
                      const string &config_json) noexcept {
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
    string desturi = GenomicSQLiteURI(destfile, config_json) + "&outer_unsafe=true";

    ConfigParser cfg(config_json);
    ostringstream ans;
    ans << "PRAGMA page_size = " << (cfg.GetInt("$.inner_page_KiB") * 1024) << ";\nVACUUM INTO "
        << sqlquote(desturi);
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
  public:
    struct cached_levels {
        uint32_t data_version = UINT32_MAX;
        int db_total_changes = INT_MAX, ceiling = 15, floor = 0;
    };
    using levels_cache = map<string, cached_levels>;
    GenomicRangeIndexLevelsCursor(sqlite3 *db, levels_cache &cache) : db_(db), cache_(cache) {}

    int Filter(int idxNum, const char *idxStr, int argc, sqlite3_value **argv) override {
        ceiling_ = floor_ = -1;
        if (argc != 1) {
            Error("genomic_range_index_levels() expects 1 argument");
        } else if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
            Error("genomic_range_index_levels() expects table name");
        } else {
            string table_name = (const char *)sqlite3_value_text(argv[0]);
            // TODO: sanitize table_name
            auto schema_table = split_schema_table(table_name);
            string schema = schema_table.first;
            transform(schema.begin(), schema.end(), schema.begin(), ::tolower);

            uint32_t data_version = UINT32_MAX;
            int db_total_changes = INT_MAX;
            bool main = schema.empty() || schema == "main.";
            if (main) {
                // cache levels for tables of the main database, invalidated when database changes
                // are indicated by SQLITE_FCNTL_DATA_VERSION and/or sqlite3_total_changes().
                // Exclude attached databases because we can't know if a schema name could have
                // been reattached to a different file between invocations.
                int rc =
                    sqlite3_file_control(db_, nullptr, SQLITE_FCNTL_DATA_VERSION, &data_version);
                if (rc != SQLITE_OK) {
                    Error("genomic_range_index_levels(): error in SQLITE_FCNTL_DATA_VERSION");
                    return rc;
                }
                db_total_changes = sqlite3_total_changes(db_);
                auto cached = cache_.find(schema_table.second);
                if (cached != cache_.end() && data_version == cached->second.data_version &&
                    db_total_changes == cached->second.db_total_changes) {
                    floor_ = cached->second.floor;
                    ceiling_ = cached->second.ceiling;
                    _DBG << "genomic_range_index_levels() cache hit on " << table_name
                         << " ceiling = " << ceiling_ << " floor = " << floor_ << endl;
                    return SQLITE_OK;
                }
            }

            try {
                auto p = DetectLevelRange(db_, table_name);
                floor_ = p.first;
                ceiling_ = p.second;
            } catch (std::exception &exn) {
                Error(exn.what());
                return SQLITE_ERROR;
            }
            assert(floor_ >= 0 && ceiling_ >= floor_ && ceiling_ <= 15);

            if (main) {
                auto cached = cache_.find(schema_table.second);
                if (cached == cache_.end()) {
                    cache_[schema_table.second] = cached_levels();
                    cached = cache_.find(schema_table.second);
                    assert(cached != cache_.end());
                }
                cached->second.data_version = data_version;
                cached->second.db_total_changes = db_total_changes;
                cached->second.ceiling = ceiling_;
                cached->second.floor = floor_;
            }

            return SQLITE_OK;
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

  private:
    sqlite3 *db_;
    levels_cache &cache_;
    sqlite_int64 ceiling_ = -1, floor_ = -1;
};

class GenomicRangeIndexLevelsTVF : public SQLiteVirtualTable {
    GenomicRangeIndexLevelsCursor::levels_cache cache_;

    unique_ptr<SQLiteVirtualTableCursor> NewCursor() override {
        return unique_ptr<SQLiteVirtualTableCursor>(new GenomicRangeIndexLevelsCursor(db_, cache_));
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

extern "C" int nucleotides_twobit(const char *seq, size_t len, void *out) {
    if (len == 0) {
        return 0;
    }

    unsigned char *outbyte = (unsigned char *)out;

    // Header byte: the low two bits specify how many crumbs at the end of the buffer must be
    // ignored by the decoder (0, 1, 2, or 3). Exception: if len == 1 then these low two bits
    // encode the nucleotide directly.
    auto trailing_crumbs = (4 - len % 4) % 4;
    assert(trailing_crumbs >= 0 && trailing_crumbs <= 3);
    if (len > 1) {
        *(outbyte++) = trailing_crumbs;
    }

    unsigned char byte = 0;
    for (size_t i = 0; i < len; ++i) {
        const char c_i = seq[i];
        if (c_i <= 0) {
            return -2;
        }
        assert(c_i >= 0 && c_i < 128);
        const unsigned char crumb = dna_crumb_table[(unsigned char)c_i];
        if (crumb > 3) {
            return -1;
        }
        assert((byte >> 6) == 0);
        byte = (byte << 2) | crumb;
        if (i % 4 == 3) {
            *(outbyte++) = byte;
            byte = 0;
        }
    }

    if (trailing_crumbs) {
        assert(len && (byte >> (2 * (4 - trailing_crumbs))) == 0);
        if (len > 1) {
            byte <<= (2 * trailing_crumbs);
        }
        *(outbyte++) = byte;
    } else {
        assert(byte == 0);
    }

    return (outbyte - (unsigned char *)out);
}

static void sqlfn_nucleotides_twobit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    assert(argc == 1);
    auto arg0ty = sqlite3_value_type(argv[0]);
    if (arg0ty == SQLITE_NULL) {
        return sqlite3_result_null(ctx);
    } else if (arg0ty != SQLITE_TEXT) {
        return sqlite3_result_error(ctx, "nucleotides_twobit() expected TEXT", -1);
    }

    auto seqlen = sqlite3_value_bytes(argv[0]);
    assert(seqlen >= 0);
    if (seqlen <= 0) {
        return sqlite3_result_value(ctx, argv[0]);
    }

    auto seq = (const char *)sqlite3_value_text(argv[0]);
    if (!seq) {
        return sqlite3_result_error_nomem(ctx);
    }

    try {
        std::unique_ptr<unsigned char[]> buf(new unsigned char[(seqlen + 7) / 4]);
        int rc = nucleotides_twobit(seq, (size_t)seqlen, buf.get());
        if (rc == -2) {
            return sqlite3_result_error(ctx, "non-ASCII input to nucleotides_twobit()", -1);
        } else if (rc < 0) {
            return sqlite3_result_text(ctx, seq, seqlen, SQLITE_TRANSIENT);
        }
        return sqlite3_result_blob64(ctx, buf.get(), rc, SQLITE_TRANSIENT);
    } catch (std::bad_alloc &) {
        return sqlite3_result_error_nomem(ctx);
    }
}

extern "C" size_t twobit_length(const void *data, size_t sz) {
    if (sz < 2) {
        return sz;
    }
    const unsigned char *bytes = (const unsigned char *)data;
    unsigned char trailing_crumbs = bytes[0] & 0b11;
    return 4 * (sz - 1) - trailing_crumbs;
}

static void sqlfn_twobit_length(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    assert(argc == 1);
    try {
        if (sqlite3_value_type(argv[0]) == SQLITE_BLOB) {
            size_t sz = sqlite3_value_bytes(argv[0]);
            size_t ans = twobit_length(sqlite3_value_blob(argv[0]), sz);
            if (ans > 2147483647) { // https://www.sqlite.org/limits.html
                sqlite3_result_error_toobig(ctx);
            } else {
                sqlite3_result_int64(ctx, ans);
            }
        } else if (sqlite3_value_type(argv[0]) == SQLITE_TEXT) {
            sqlite3_result_int64(ctx, sqlite3_value_bytes(argv[0]));
        } else if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
        } else {
            sqlite3_result_error(ctx, "twobit_length() expected BLOB or TEXT", -1);
        }
    } catch (std::bad_alloc &) {
        sqlite3_result_error_nomem(ctx);
    }
}

/*
letters = ('T', 'C', 'A', 'G')
dna4mers = []
for byte in range(256):
    rev4mer = []
    for _ in range(4):
        rev4mer.append(letters[byte & 0b11])
        byte = byte >> 2
    dna4mers.append(''.join(reversed(rev4mer)))

print(dna4mers)
*/
const char *twobit_dna4mers[] = {
    "TTTT", "TTTC", "TTTA", "TTTG", "TTCT", "TTCC", "TTCA", "TTCG", "TTAT", "TTAC", "TTAA", "TTAG",
    "TTGT", "TTGC", "TTGA", "TTGG", "TCTT", "TCTC", "TCTA", "TCTG", "TCCT", "TCCC", "TCCA", "TCCG",
    "TCAT", "TCAC", "TCAA", "TCAG", "TCGT", "TCGC", "TCGA", "TCGG", "TATT", "TATC", "TATA", "TATG",
    "TACT", "TACC", "TACA", "TACG", "TAAT", "TAAC", "TAAA", "TAAG", "TAGT", "TAGC", "TAGA", "TAGG",
    "TGTT", "TGTC", "TGTA", "TGTG", "TGCT", "TGCC", "TGCA", "TGCG", "TGAT", "TGAC", "TGAA", "TGAG",
    "TGGT", "TGGC", "TGGA", "TGGG", "CTTT", "CTTC", "CTTA", "CTTG", "CTCT", "CTCC", "CTCA", "CTCG",
    "CTAT", "CTAC", "CTAA", "CTAG", "CTGT", "CTGC", "CTGA", "CTGG", "CCTT", "CCTC", "CCTA", "CCTG",
    "CCCT", "CCCC", "CCCA", "CCCG", "CCAT", "CCAC", "CCAA", "CCAG", "CCGT", "CCGC", "CCGA", "CCGG",
    "CATT", "CATC", "CATA", "CATG", "CACT", "CACC", "CACA", "CACG", "CAAT", "CAAC", "CAAA", "CAAG",
    "CAGT", "CAGC", "CAGA", "CAGG", "CGTT", "CGTC", "CGTA", "CGTG", "CGCT", "CGCC", "CGCA", "CGCG",
    "CGAT", "CGAC", "CGAA", "CGAG", "CGGT", "CGGC", "CGGA", "CGGG", "ATTT", "ATTC", "ATTA", "ATTG",
    "ATCT", "ATCC", "ATCA", "ATCG", "ATAT", "ATAC", "ATAA", "ATAG", "ATGT", "ATGC", "ATGA", "ATGG",
    "ACTT", "ACTC", "ACTA", "ACTG", "ACCT", "ACCC", "ACCA", "ACCG", "ACAT", "ACAC", "ACAA", "ACAG",
    "ACGT", "ACGC", "ACGA", "ACGG", "AATT", "AATC", "AATA", "AATG", "AACT", "AACC", "AACA", "AACG",
    "AAAT", "AAAC", "AAAA", "AAAG", "AAGT", "AAGC", "AAGA", "AAGG", "AGTT", "AGTC", "AGTA", "AGTG",
    "AGCT", "AGCC", "AGCA", "AGCG", "AGAT", "AGAC", "AGAA", "AGAG", "AGGT", "AGGC", "AGGA", "AGGG",
    "GTTT", "GTTC", "GTTA", "GTTG", "GTCT", "GTCC", "GTCA", "GTCG", "GTAT", "GTAC", "GTAA", "GTAG",
    "GTGT", "GTGC", "GTGA", "GTGG", "GCTT", "GCTC", "GCTA", "GCTG", "GCCT", "GCCC", "GCCA", "GCCG",
    "GCAT", "GCAC", "GCAA", "GCAG", "GCGT", "GCGC", "GCGA", "GCGG", "GATT", "GATC", "GATA", "GATG",
    "GACT", "GACC", "GACA", "GACG", "GAAT", "GAAC", "GAAA", "GAAG", "GAGT", "GAGC", "GAGA", "GAGG",
    "GGTT", "GGTC", "GGTA", "GGTG", "GGCT", "GGCC", "GGCA", "GGCG", "GGAT", "GGAC", "GGAA", "GGAG",
    "GGGT", "GGGC", "GGGA", "GGGG"};
const char *twobit_rna4mers[] = {
    "UUUU", "UUUC", "UUUA", "UUUG", "UUCU", "UUCC", "UUCA", "UUCG", "UUAU", "UUAC", "UUAA", "UUAG",
    "UUGU", "UUGC", "UUGA", "UUGG", "UCUU", "UCUC", "UCUA", "UCUG", "UCCU", "UCCC", "UCCA", "UCCG",
    "UCAU", "UCAC", "UCAA", "UCAG", "UCGU", "UCGC", "UCGA", "UCGG", "UAUU", "UAUC", "UAUA", "UAUG",
    "UACU", "UACC", "UACA", "UACG", "UAAU", "UAAC", "UAAA", "UAAG", "UAGU", "UAGC", "UAGA", "UAGG",
    "UGUU", "UGUC", "UGUA", "UGUG", "UGCU", "UGCC", "UGCA", "UGCG", "UGAU", "UGAC", "UGAA", "UGAG",
    "UGGU", "UGGC", "UGGA", "UGGG", "CUUU", "CUUC", "CUUA", "CUUG", "CUCU", "CUCC", "CUCA", "CUCG",
    "CUAU", "CUAC", "CUAA", "CUAG", "CUGU", "CUGC", "CUGA", "CUGG", "CCUU", "CCUC", "CCUA", "CCUG",
    "CCCU", "CCCC", "CCCA", "CCCG", "CCAU", "CCAC", "CCAA", "CCAG", "CCGU", "CCGC", "CCGA", "CCGG",
    "CAUU", "CAUC", "CAUA", "CAUG", "CACU", "CACC", "CACA", "CACG", "CAAU", "CAAC", "CAAA", "CAAG",
    "CAGU", "CAGC", "CAGA", "CAGG", "CGUU", "CGUC", "CGUA", "CGUG", "CGCU", "CGCC", "CGCA", "CGCG",
    "CGAU", "CGAC", "CGAA", "CGAG", "CGGU", "CGGC", "CGGA", "CGGG", "AUUU", "AUUC", "AUUA", "AUUG",
    "AUCU", "AUCC", "AUCA", "AUCG", "AUAU", "AUAC", "AUAA", "AUAG", "AUGU", "AUGC", "AUGA", "AUGG",
    "ACUU", "ACUC", "ACUA", "ACUG", "ACCU", "ACCC", "ACCA", "ACCG", "ACAU", "ACAC", "ACAA", "ACAG",
    "ACGU", "ACGC", "ACGA", "ACGG", "AAUU", "AAUC", "AAUA", "AAUG", "AACU", "AACC", "AACA", "AACG",
    "AAAU", "AAAC", "AAAA", "AAAG", "AAGU", "AAGC", "AAGA", "AAGG", "AGUU", "AGUC", "AGUA", "AGUG",
    "AGCU", "AGCC", "AGCA", "AGCG", "AGAU", "AGAC", "AGAA", "AGAG", "AGGU", "AGGC", "AGGA", "AGGG",
    "GUUU", "GUUC", "GUUA", "GUUG", "GUCU", "GUCC", "GUCA", "GUCG", "GUAU", "GUAC", "GUAA", "GUAG",
    "GUGU", "GUGC", "GUGA", "GUGG", "GCUU", "GCUC", "GCUA", "GCUG", "GCCU", "GCCC", "GCCA", "GCCG",
    "GCAU", "GCAC", "GCAA", "GCAG", "GCGU", "GCGC", "GCGA", "GCGG", "GAUU", "GAUC", "GAUA", "GAUG",
    "GACU", "GACC", "GACA", "GACG", "GAAU", "GAAC", "GAAA", "GAAG", "GAGU", "GAGC", "GAGA", "GAGG",
    "GGUU", "GGUC", "GGUA", "GGUG", "GGCU", "GGCC", "GGCA", "GGCG", "GGAU", "GGAC", "GGAA", "GGAG",
    "GGGU", "GGGC", "GGGA", "GGGG"};

static void twobit_nucleotides(const void *data, size_t sz, size_t ofs, size_t len, bool rna,
                               char *out) {
    const char **table = rna ? twobit_rna4mers : twobit_dna4mers;
    // special cases for length-0 and 1 blobs
    if (sz < 2) {
        if (len == 0) {
            out[0] = 0;
            return;
        }
        assert(ofs == 0 && len == 1 && sz == 1);
        out[0] = table[*(const unsigned char *)data & 0b11][3];
        out[1] = 0;
        return;
    }
    const unsigned char *pbyte = ((const unsigned char *)data) + 1 + ofs / 4;
    size_t out_cursor = 0;
    // decode first payload byte (maybe only part of it) crumb-by-crumb
    for (auto crumb = ofs % 4; crumb < 4 && out_cursor < len;) {
        out[out_cursor++] = table[*pbyte][crumb++];
    }
    // decode internal bytes to 4-mers
    for (++pbyte; out_cursor + 4 <= len; out_cursor += 4) {
        memcpy(out + out_cursor, table[*(pbyte++)], 4);
    }
    // decode last payload byte crumb-by-crumb, if needed
    for (size_t crumb = 0; out_cursor < len;) {
        assert(crumb < 4);
        out[out_cursor++] = table[*pbyte][crumb++];
    }
    assert(out_cursor == len);
    assert(pbyte - (const unsigned char *)data <= sz);
    out[out_cursor] = 0;
}

extern "C" void twobit_dna(const void *data, size_t sz, size_t ofs, size_t len, char *out) {
    return twobit_nucleotides(data, sz, ofs, len, false, out);
}

extern "C" void twobit_rna(const void *data, size_t sz, size_t ofs, size_t len, char *out) {
    return twobit_nucleotides(data, sz, ofs, len, true, out);
}

static void twobit_nucleotides(sqlite3_context *ctx, int argc, sqlite3_value **argv, bool rna) {
    assert(argc >= 1 && argc <= 3);
    bool blob = false;
    switch (sqlite3_value_type(argv[0])) {
    case SQLITE_TEXT:
        break;
    case SQLITE_BLOB:
        blob = true;
        break;
    case SQLITE_NULL:
        return sqlite3_result_null(ctx);
    default:
        return sqlite3_result_error(ctx, "twobit_dna() expected BLOB or TEXT", -1);
    }

    // Y and Z are as https://sqlite.org/lang_corefunc.html#substr
    ssize_t Y = 0, Zval = -1;
    ARG_OPTIONAL(Y, 1, SQLITE_INTEGER, int);
    ARG_OPTIONAL(Zval, 2, SQLITE_INTEGER, int);
    ssize_t *Z = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) ? &Zval : nullptr;

    try {
        size_t sz = (size_t)sqlite3_value_bytes(argv[0]);
        size_t len = blob ? twobit_length(sqlite3_value_blob(argv[0]), sz) : sz;

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
        assert(sub_ofs + sub_len <= (blob ? len : sz));

        if (blob) {
            // decode two-bit-encoded BLOB
            unique_ptr<char[]> buf(new char[sub_len + 1]);
            twobit_nucleotides(sqlite3_value_blob(argv[0]), sz, sub_ofs, sub_len, rna, buf.get());
            sqlite3_result_text(ctx, buf.get(), sub_len, SQLITE_TRANSIENT);
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
    twobit_nucleotides(ctx, argc, argv, false);
}

static void sqlfn_twobit_rna(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    twobit_nucleotides(ctx, argc, argv, true);
}

/*
complement = [0xFF for i in range(256)]
for l,r in (
    ('A','T'), ('C','G'), ('G','C'), ('T','A'),
    ('a','t'), ('c','g'), ('g','c'), ('t','a'),
):
    complement[ord(l)] = r

print(complement)
*/
const unsigned char dna_complement_table[] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 'T',  0xFF, 'G',  0xFF, 0xFF, 0xFF, 'C',  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 'A',  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 't',  0xFF, 'g',  0xFF, 0xFF, 0xFF, 'c',  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 'a',  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

extern "C" int dna_revcomp(const char *dna, size_t len, char *out) {
    for (; len; --len, ++out)
        if ((*out = dna_complement_table[(unsigned char)dna[len - 1]]) == 0xFF)
            return -1;
    *out = 0;
    return 0;
}

static void sqlfn_dna_revcomp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    assert(argc == 1);
    const char *seq = nullptr;
    ARG_TEXT_OPTIONAL(seq, 0);
    if (!seq) {
        return sqlite3_value_type(argv[0]) == SQLITE_NULL ? sqlite3_result_null(ctx)
                                                          : sqlite3_result_error_nomem(ctx);
    }

    auto seqlen = sqlite3_value_bytes(argv[0]);
    assert(seqlen >= 0);
    if (seqlen <= 0) {
        return sqlite3_result_value(ctx, argv[0]);
    }

    try {
        std::unique_ptr<char[]> buf(new char[seqlen + 1]);
        if (dna_revcomp(seq, seqlen, buf.get()) < 0) {
            return sqlite3_result_error(ctx, "non-DNA input to dna_revcomp()", -1);
        }
        return sqlite3_result_text(ctx, buf.get(), seqlen, SQLITE_TRANSIENT);
    } catch (std::bad_alloc &) {
        return sqlite3_result_error_nomem(ctx);
    }
}

/**************************************************************************************************
 * parse_genomic_range_*()
 **************************************************************************************************/

static uint64_t parse_genomic_range_pos(const string &txt, size_t ofs1, size_t ofs2) {
    assert(ofs1 < ofs2);
    assert(ofs2 <= txt.size());
    uint64_t ans = 0;
    for (size_t i = ofs1; i < ofs2; ++i) {
        auto c = txt[i];
        if (c >= '0' && c <= '9') {
            if (ans > 922337203685477579ULL) { // (2**63-10)//10
                throw std::runtime_error("parse_genomic_range(): position overflow in `" + txt +
                                         "`");
            }
            ans *= 10;
            ans += c - '0';
        } else if (c == ',') {
            continue;
        } else {
            throw std::runtime_error("parse_genomic_range(): can't read `" + txt + "`");
        }
    }
    return ans;
}

std::tuple<string, uint64_t, uint64_t> parse_genomic_range(const string &txt) {
    auto p1 = txt.find(':');
    auto p2 = txt.find('-');
    if (p1 == string::npos || p2 == string::npos || p1 < 1 || p2 < p1 + 2 || p2 >= txt.size() - 1) {
        throw std::runtime_error("parse_genomic_range(): can't read `" + txt + "`");
    }
    string chrom = txt.substr(0, p1);
    for (size_t i = 0; i < chrom.size(); ++i) {
        if (std::isspace(chrom[i])) {
            throw std::runtime_error(
                "parse_genomic_range(): invalid sequence/chromosome name in `" + txt + "`");
        }
    }
    auto begin_pos = parse_genomic_range_pos(txt, p1 + 1, p2),
         end_pos = parse_genomic_range_pos(txt, p2 + 1, txt.size());
    if (begin_pos < 1 || begin_pos > end_pos) {
        throw std::runtime_error("parse_genomic_range(): invalid one-based positions in `" + txt +
                                 "`");
    }
    return std::make_tuple(chrom, begin_pos - 1, end_pos);
}

static void sqlfn_parse_genomic_range_sequence(sqlite3_context *ctx, int argc,
                                               sqlite3_value **argv) {
    const char *txt = nullptr;
    ARG_TEXT_OPTIONAL(txt, 0);
    if (!txt) {
        return sqlite3_value_type(argv[0]) == SQLITE_NULL ? sqlite3_result_null(ctx)
                                                          : sqlite3_result_error_nomem(ctx);
    }
    try {
        auto t = parse_genomic_range(string(txt, sqlite3_value_bytes(argv[0])));
        auto &chrom = get<0>(t);
        return sqlite3_result_text(ctx, chrom.c_str(), chrom.size(), SQLITE_TRANSIENT);
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

static void sqlfn_parse_genomic_range_begin(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    const char *txt = nullptr;
    ARG_TEXT_OPTIONAL(txt, 0);
    if (!txt) {
        return sqlite3_value_type(argv[0]) == SQLITE_NULL ? sqlite3_result_null(ctx)
                                                          : sqlite3_result_error_nomem(ctx);
    }
    try {
        auto t = parse_genomic_range(string(txt, sqlite3_value_bytes(argv[0])));
        return sqlite3_result_int64(ctx, get<1>(t));
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

static void sqlfn_parse_genomic_range_end(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    const char *txt = nullptr;
    ARG_TEXT_OPTIONAL(txt, 0);
    if (!txt) {
        return sqlite3_value_type(argv[0]) == SQLITE_NULL ? sqlite3_result_null(ctx)
                                                          : sqlite3_result_error_nomem(ctx);
    }
    try {
        auto t = parse_genomic_range(string(txt, sqlite3_value_bytes(argv[0])));
        return sqlite3_result_int64(ctx, get<2>(t));
    } catch (std::exception &exn) {
        sqlite3_result_error(ctx, exn.what(), -1);
    }
}

/**************************************************************************************************
 * SQLite loadable extension initialization
 **************************************************************************************************/

extern "C" int genomicsqliteJson1Register(sqlite3 *db);
extern "C" int genomicsqlite_uint_init(sqlite3 *db);

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
                 {FPNM(nucleotides_twobit), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_length), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 2, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_dna), 3, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 1, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 2, SQLITE_DETERMINISTIC},
                 {FPNM(twobit_rna), 3, SQLITE_DETERMINISTIC},
                 {FPNM(dna_revcomp), 1, SQLITE_DETERMINISTIC},
                 {FPNM(parse_genomic_range_sequence), 1, SQLITE_DETERMINISTIC},
                 {FPNM(parse_genomic_range_begin), 1, SQLITE_DETERMINISTIC},
                 {FPNM(parse_genomic_range_end), 1, SQLITE_DETERMINISTIC}};

    int rc;
    for (int i = 0; i < sizeof(fntab) / sizeof(fntab[0]); ++i) {
        rc =
            sqlite3_create_function_v2(db, fntab[i].fn, fntab[i].nArg, SQLITE_UTF8 | fntab[i].flags,
                                       nullptr, fntab[i].fp, nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            if (pzErrMsg) {
                *pzErrMsg = sqlite3_mprintf("Genomics Extension %s failed to register %s",
                                            GIT_REVISION, fntab[i].fn);
            }
            return rc;
        }
    }
    rc = RegisterSQLiteVirtualTable<GenomicRangeIndexLevelsTVF>(db, "genomic_range_index_levels");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf(
                "Genomics Extension %s failed to register genomic_range_index_levels",
                GIT_REVISION);
        }
        return rc;
    }
    rc = RegisterSQLiteVirtualTable<GenomicRangeRowidsTVF>(db, "genomic_range_rowids");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf(
                "Genomics Extension %s failed to register genomic_range_rowids", GIT_REVISION);
        }
        return rc;
    }
    // other extensions may return SQLITE_BUSY if another version is already loaded; that is
    // tolerable.
    rc = genomicsqliteJson1Register(db);
    if (rc != SQLITE_OK && rc != SQLITE_BUSY) {
        if (pzErrMsg) {
            *pzErrMsg =
                sqlite3_mprintf("Genomics Extension %s failed to register JSON1", GIT_REVISION);
        }
        return rc;
    }
    rc = genomicsqlite_uint_init(db);
    if (rc != SQLITE_OK && rc != SQLITE_BUSY) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("Genomics Extension %s failed to register UINT collation",
                                        GIT_REVISION);
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

    /* disabled b/c JDBC bindings "legitimately" entail two different dynamically-linked sqlite libs!
    // Check that SQLiteCpp is using the same SQLite library that's loading us. This may not be the
    // case when different versions of SQLite are linked into the running process, one static and
    // one dynamic.
    string static_version(pApi->libversion()), dynamic_version("UNKNOWN");
    {
        SQLite::Database tmpdb(":memory:", SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
        SQLite::Statement query(tmpdb, "SELECT sqlite_version()");
        if (query.executeStep() && query.getColumnCount() == 1) {
            dynamic_version = query.getColumn(0).getText("UNKNOWN");
        }
    }
    if (static_version != dynamic_version) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf(
                "Two distinct versions of SQLite (%s & %s) detected by Genomics Extension %s. Eliminate static linking of SQLite from the main executable.",
                static_version.c_str(), dynamic_version.c_str(), GIT_REVISION);
        }
        return SQLITE_ERROR;
    }
    */

    int rc = (new WebVFS::VFS())->Register("web");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg =
                sqlite3_mprintf("Genomics Extension %s failed initializing web_vfs", GIT_REVISION);
        }
        return rc;
    }
    rc = (new ZstdVFS())->Register("zstd");
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg =
                sqlite3_mprintf("Genomics Extension %s failed initializing zstd_vfs", GIT_REVISION);
        }
        return rc;
    }
    rc = register_genomicsqlite_functions(db, (const char **)pzErrMsg, pApi);
    if (rc != SQLITE_OK)
        return rc;
    rc = sqlite3_auto_extension((void (*)(void))register_genomicsqlite_functions);
    if (rc != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("Genomics Extension %s failed sqlite3_auto_extension",
                                        GIT_REVISION);
        }
        return rc;
    }
    return SQLITE_OK_LOAD_PERMANENTLY;
}
