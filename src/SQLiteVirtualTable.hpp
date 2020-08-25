// https://sqlite.org/vtab.html
// https://sqlite.org/src/file/ext/misc/templatevtab.c
// https://sqlite.org/src/file/ext/misc/series.c
#pragma once

#include <assert.h>
#include <memory>
#include <sqlite3ext.h>
#include <string.h>
#include <string>
#include <type_traits>

class SQLiteVirtualTableCursor {
  public:
    virtual int Close() {
        delete this;
        return SQLITE_OK;
    }

    virtual int Filter(int idxNum, const char *idxStr, int argc, sqlite3_value **argv) {
        return SQLITE_OK;
    }

    virtual int Next() { return SQLITE_ERROR; }

    virtual int Eof() { return 1; }

    virtual int Column(sqlite3_context *ctx, int colno) { return SQLITE_ERROR; }

    virtual int Rowid(sqlite_int64 *pRowid) { return SQLITE_ERROR; }

    SQLiteVirtualTableCursor() {
        memset(&handle_, 0, sizeof(Handle));
        handle_.that = this;
    }

    virtual ~SQLiteVirtualTableCursor() {}

    struct Handle {
        sqlite3_vtab_cursor vtab_cursor;
        SQLiteVirtualTableCursor *that;
    };

  protected:
    friend class SQLiteVirtualTable;
    Handle handle_;

    SQLiteVirtualTableCursor(SQLiteVirtualTableCursor &rhs) = delete;
};

class SQLiteVirtualTable {
  public:
    virtual int Disconnect() {
        delete this;
        return SQLITE_OK;
    }

    virtual int BestIndex(sqlite3_index_info *info) {
        info->idxNum = -1;
        info->idxStr = nullptr;
        info->estimatedRows = 25;
        return SQLITE_OK;
    }

    virtual int Open(sqlite3_vtab_cursor **ppCursor) {
        auto cursor = NewCursor();
        cursor->handle_.vtab_cursor.pVtab = &(handle_.vtab);
        *ppCursor = &(cursor.release()->handle_.vtab_cursor);
        return SQLITE_OK;
    }

    static int Connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                       sqlite3_vtab **ppVTab, char **pzErr) {
        return SimpleConnect(db, pAux, argc, argv, ppVTab, pzErr,
                             std::unique_ptr<SQLiteVirtualTable>(new SQLiteVirtualTable(db)),
                             "CREATE TABLE xxx(x BLOB)");
    }

    static int SetErr(int rc, const std::string &msg, char **pzErr) {
        if (pzErr) {
            assert(!*pzErr);
            *pzErr = reinterpret_cast<char *>(sqlite3_malloc(msg.size() + 1));
            if (*pzErr) {
                strcpy(*pzErr, msg.c_str());
            }
        }
        return rc;
    }

    virtual ~SQLiteVirtualTable() {}

    struct Handle {
        sqlite3_vtab vtab;
        SQLiteVirtualTable *that;
    };

  protected:
    Handle handle_;
    sqlite3 *db_;

    virtual std::unique_ptr<SQLiteVirtualTableCursor> NewCursor() {
        return std::unique_ptr<SQLiteVirtualTableCursor>(new SQLiteVirtualTableCursor());
    }

    SQLiteVirtualTable(sqlite3 *db) : db_(db) {
        memset(&handle_, 0, sizeof(Handle));
        handle_.that = this;
    }
    SQLiteVirtualTable(SQLiteVirtualTable &rhs) = delete;

    static int SimpleConnect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                             sqlite3_vtab **ppVTab, char **pzErr,
                             std::unique_ptr<SQLiteVirtualTable> that, const std::string &ddl) {
        assert(that);
        int rc = sqlite3_declare_vtab(db, ddl.c_str());
        if (rc != SQLITE_OK) {
            return rc;
        }
        that->handle_.vtab.pModule = reinterpret_cast<sqlite3_module *>(pAux);
        *ppVTab = &(that.release()->handle_.vtab);
        return SQLITE_OK;
    }

    // Helper to implement xBestIndex for table-valued functions, which have a certain number of
    // visible columns, and some other number of hidden columns which serve as function arguments.
    //
    // visible_cols = # non-hidden columns returned by the function
    //     min_args = minimum # of arguments (hidden columns) acceptable to the function
    //     max_args = total # of hidden columns
    //
    // If the function receives < max_args arguments, they form a prefix of the hidden columns.
    int BestIndexTVF(sqlite3_index_info *info, int visible_cols, int min_args, int max_args) {
        assert(visible_cols >= 0 && min_args >= 0 && min_args <= max_args &&
               visible_cols + max_args <= 62);
        if (info->nConstraint < min_args || info->nConstraint > max_args) {
            return SQLITE_CONSTRAINT;
        }
        long long arg_bitmap = 0;
        for (int i = 0; i < info->nConstraint; ++i) {
            auto &constraint = info->aConstraint[i];
            int arg = constraint.iColumn - visible_cols;
            // each entry should be a usable equality constraint, at most one per hidden column.
            if (arg < 0 || arg >= max_args || arg_bitmap & (1 << arg) ||
                constraint.op != SQLITE_INDEX_CONSTRAINT_EQ || !constraint.usable) {
                return SQLITE_CONSTRAINT;
            }
            arg_bitmap |= (1 << arg);
            // have argument passed in the proper order to SQLiteVirtualTableCursor::Filter()
            info->aConstraintUsage[i].argvIndex = arg + 1;
            info->aConstraintUsage[i].omit = true;
        }
        // constrained columns should form a prefix of the hidden columns
        if (arg_bitmap != (1 << info->nConstraint) - 1) {
            return SQLITE_CONSTRAINT;
        }
        return SQLITE_OK;
    }
};

template <class TableImpl> int RegisterSQLiteVirtualTable(sqlite3 *db, const char *zName) {
    sqlite3_module *p = new sqlite3_module;
    memset(p, 0, sizeof(sqlite3_module));
    p->iVersion = 1;
    p->xConnect = [](sqlite3 *db, void *pAux, int argc, const char *const *argv,
                     sqlite3_vtab **ppVTab, char **pzErr) {
        if (*pzErr) {
            *pzErr = nullptr;
        }
        try {
            return TableImpl::Connect(db, pAux, argc, argv, ppVTab, pzErr);
        } catch (std::bad_alloc &) {
            return SQLITE_NOMEM;
        } catch (std::exception &exn) {
            if (pzErr && !*pzErr) {
                return SQLiteVirtualTable::SetErr(SQLITE_ERROR, exn.what(), pzErr);
            }
        } catch (...) {
        }
        return SQLITE_ERROR;
    };
#define __STUB_CATCH(delegate)                                                                     \
    try {                                                                                          \
        return delegate;                                                                           \
    } catch (std::bad_alloc &) {                                                                   \
        return SQLITE_NOMEM;                                                                       \
    } catch (...) {                                                                                \
    }                                                                                              \
    return SQLITE_ERROR;
    p->xBestIndex = [](sqlite3_vtab *pVTab, sqlite3_index_info *info) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTable::Handle *>(pVTab)->that->BestIndex(info));
    };
    p->xDisconnect = [](sqlite3_vtab *pVTab) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTable::Handle *>(pVTab)->that->Disconnect());
    };
    p->xDestroy = p->xDisconnect;
    p->xOpen = [](sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTable::Handle *>(pVTab)->that->Open(ppCursor));
    };
    p->xClose = [](sqlite3_vtab_cursor *pCursor) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Close());
    };
    p->xFilter = [](sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc,
                    sqlite3_value **argv) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Filter(
            idxNum, idxStr, argc, argv));
    };
    p->xNext = [](sqlite3_vtab_cursor *pCursor) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Next());
    };
    p->xEof = [](sqlite3_vtab_cursor *pCursor) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Eof());
    };
    p->xColumn = [](sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int colno) noexcept {
        __STUB_CATCH(reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Column(
            ctx, colno));
    };
    p->xRowid = [](sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid) noexcept {
        __STUB_CATCH(
            reinterpret_cast<SQLiteVirtualTableCursor::Handle *>(pCursor)->that->Rowid(pRowid));
    };
#undef __STUB_CATCH
    return sqlite3_create_module_v2(db, zName, p, p,
                                    [](void *q) { delete reinterpret_cast<sqlite3_module *>(q); });
}
