#pragma once

#define SQLITE_CORE
#include "SQLiteCpp/SQLiteCpp.h"
#include "genomicsqlite.h"
#include "strlcpy.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <sqlite3ext.h>
#include <string>
#include <thread>
#include <vector>

#ifndef NDEBUG
#define _DBG cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && cerr
#endif

using namespace std;

// split s on delim; s is damaged by side-effect
template <typename Out>
void split(char *s, char delim, Out result, uint64_t maxsplit = ULLONG_MAX) {
    string delims("\n");
    delims[0] = delim;
    char *cursor = s;
    char *token = strsep(&cursor, delims.c_str());
    char *last_token = token;
    uint64_t i = 0;
    while (token) {
        *(result++) = last_token = token;
        if (++i < maxsplit) {
            token = strsep(&cursor, delims.c_str());
        } else {
            if (cursor) {
                *result = last_token = cursor;
            }
            break;
        }
    }
}

template <typename Out>
void split(string &s, char delim, Out result, uint64_t maxsplit = ULLONG_MAX) {
    split(&s[0], delim, result, maxsplit);
}

// because ostringstream is too slow :(
class OStringStream {
  public:
    OStringStream(size_t initial_capacity) : buf_size_(initial_capacity), cursor_(0) {
        buf_.reset(new char[buf_size_ + 1]);
        buf_[0] = 0;
    }
    OStringStream() : OStringStream(64) {}
    OStringStream(const OStringStream &) = delete;

    inline void Add(char c) {
        if (remaining() == 0) {
            grow();
            assert(remaining() > 0);
        }

        buf_[cursor_++] = c;
        buf_[cursor_] = 0;
    }
    inline OStringStream &operator<<(char c) {
        Add(c);
        return *this;
    }

    void Add(const char *s) {
        size_t rem = remaining();
        size_t len = strlcpy(&buf_[cursor_], s, rem + 1);
        if (len <= rem) {
            cursor_ += len;
            assert(buf_[cursor_] == 0);
        } else {
            cursor_ += rem;
            assert(buf_[cursor_] == 0);
            grow(len - rem);
            return Add(s + rem);
        }
    }
    inline OStringStream &operator<<(const char *s) {
        Add(s);
        return *this;
    }
    inline OStringStream &operator<<(const string &s) { return *this << s.c_str(); }

    inline const char *Get() const {
        assert(buf_[cursor_] == 0);
        return &buf_[0];
    }

    inline size_t Size() const {
        assert(buf_[cursor_] == 0);
        return cursor_;
    }

    void Clear() { buf_[0] = cursor_ = 0; }

  private:
    inline size_t remaining() const {
        assert(buf_[cursor_] == 0);
        assert(cursor_ <= buf_size_);
        return buf_size_ - cursor_;
    }

    void grow(size_t hint = 0) {
        buf_size_ = max(2 * buf_size_, hint + buf_size_);
        unique_ptr<char[]> buf(new char[buf_size_ + 1]);
        memcpy(&buf[0], &buf_[0], cursor_);
        buf[cursor_] = 0;
        swap(buf_, buf);
    }

    // actual size of buf_ should always be buf_size_+1 to accommodate NUL
    // invariants: cursor_ <= buf_size_ && buf_[cursor_] == 0
    unique_ptr<char[]> buf_;
    size_t buf_size_, cursor_;
};

// Producer-consumer pattern: background producer thread preprocessing "Items" to be consumed by
// the main thread. Queues up to `ringsize` prepared items.
template <class Item> class BackgroundProducer {
  protected:
    // Will be called on the background thread to populate the next item. Fills it out in-place
    // from undefined initial state (avoids reallocating); returns true on success, false if the
    // item stream is successfully complete, or throws an exception.
    virtual bool Produce(Item &) = 0;

  private:
    // ~constant:
    int R_;
    unique_ptr<thread> worker_;

    // writable by either thread:
    atomic<bool> stop_;

    // writable by background producer thread:
    vector<Item> ring_;
    atomic<long long> p_; // # produced
    string errmsg_;
    chrono::time_point<chrono::high_resolution_clock> t0_;
    chrono::duration<double> p_blocked_ = chrono::duration<double>::zero();

    // writable by consumer thread:
    atomic<long long> c_; // if c_>0 then item (c_-1)%R is currently being consumed
    const Item *item_ = nullptr;
    chrono::duration<double> c_blocked_ = chrono::duration<double>::zero();

    void background_thread() {
        t0_ = chrono::high_resolution_clock::now();
        long long p = 0;
        do {
            bool ok = false;
            try {
                ok = Produce(ring_[p % R_]);
            } catch (exception &exn) {
                errmsg_ = exn.what();
                if (errmsg_.empty()) {
                    errmsg_ = "unknown error on producer thread";
                }
                stop_.store(true, memory_order_release);
            }
            if (ok) {
                p_.store(++p, memory_order_release);
                if (p - max(c_.load(memory_order_acquire), 1LL) == R_ - 1) {
                    // Ring is full; enter semi-busy wait with adaptive sleep. Goal is to keep the
                    // consumer well-fed, without excessively wasteful busy-loop, using atomics for
                    // coordination. Underlying assumption: producer is usually faster than the
                    // consumer, while the ring provides a buffer if it has the occasional hiccup.
                    auto t_spin = chrono::high_resolution_clock::now();
                    long long spin = 0;
                    do {
                        this_thread::sleep_for(
                            chrono::nanoseconds(10000 + 990000 * min(spin++, 100LL) / 100));
                    } while (!stop_.load(memory_order_relaxed) &&
                             (p - max(c_.load(memory_order_acquire), 1LL) == R_ - 1));
                    p_blocked_ += chrono::high_resolution_clock::now() - t_spin;
                }
            } else {
                stop_.store(true, memory_order_relaxed);
            }
        } while (!stop_.load(memory_order_relaxed));
    }

  public:
    BackgroundProducer(int ringsize) : R_(ringsize) {
        assert(R_ > 1);
        stop_ = false;
        p_ = 0;
        c_ = 0;
    }

    virtual ~BackgroundProducer() { abort(); }

    // advance to next item for consumption & return true, return false when item stream has ended
    // with success, or throw an exception.
    bool next() {
        if (!worker_) {
            while (ring_.size() < R_) {
                ring_.emplace_back();
            }
            worker_.reset(new thread([this]() { this->background_thread(); }));
        }
        long long p = p_.load(memory_order_acquire), c = c_.load(memory_order_relaxed);
        if (c == p) {
            // Ring is empty; enter busy wait
            auto t_spin = chrono::high_resolution_clock::now();
            while (c == p && !stop_.load(memory_order_relaxed)) {
                this_thread::yield();
                p = p_.load(memory_order_acquire);
            }
            c_blocked_ += chrono::high_resolution_clock::now() - t_spin;
        }
        if (c == p && stop_.load(memory_order_acquire)) {
            if (!errmsg_.empty()) {
                throw runtime_error(errmsg_);
            }
            return false;
        }
        assert(c < p);
        item_ = &ring_[c % R_];
        c_.store(c + 1, memory_order_release);
        return true;
    }

    // get current item for consumption; defined only after next() returned true
    const Item &item() { return *item_; }

    void abort() {
        stop_.store(true, memory_order_relaxed);
        if (worker_) {
            worker_->join();
        }
    }

    string log() {
        chrono::duration<double> elapsed = chrono::high_resolution_clock::now() - t0_;
        OStringStream ans;
        ans << to_string(c_) << " item(s) processed in " << to_string(elapsed.count()) << "s"
            << "; producer blocked for " << to_string(p_blocked_.count()) << "s"
            << "; consumer blocked for " << to_string(c_blocked_.count()) << "s";
        return string(ans.Get());
    }
};

/*
The following BS is needed for our executables to use SQLiteCpp even though it's built for use
within a loadable extension (SQLITECPP_IN_EXTENSION). The sqlite3Apis definition is copied from
    https://github.com/sqlite/sqlite/blob/master/src/loadext.c
and must match the build environment's SQLite version. Take care to keep the small declaration
following the sqlite3Apis definition.
*/
static const sqlite3_api_routines sqlite3Apis = {
    sqlite3_aggregate_context,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_aggregate_count,
#else
    0,
#endif
    sqlite3_bind_blob,
    sqlite3_bind_double,
    sqlite3_bind_int,
    sqlite3_bind_int64,
    sqlite3_bind_null,
    sqlite3_bind_parameter_count,
    sqlite3_bind_parameter_index,
    sqlite3_bind_parameter_name,
    sqlite3_bind_text,
    sqlite3_bind_text16,
    sqlite3_bind_value,
    sqlite3_busy_handler,
    sqlite3_busy_timeout,
    sqlite3_changes,
    sqlite3_close,
    sqlite3_collation_needed,
    sqlite3_collation_needed16,
    sqlite3_column_blob,
    sqlite3_column_bytes,
    sqlite3_column_bytes16,
    sqlite3_column_count,
    sqlite3_column_database_name,
    sqlite3_column_database_name16,
    sqlite3_column_decltype,
    sqlite3_column_decltype16,
    sqlite3_column_double,
    sqlite3_column_int,
    sqlite3_column_int64,
    sqlite3_column_name,
    sqlite3_column_name16,
    sqlite3_column_origin_name,
    sqlite3_column_origin_name16,
    sqlite3_column_table_name,
    sqlite3_column_table_name16,
    sqlite3_column_text,
    sqlite3_column_text16,
    sqlite3_column_type,
    sqlite3_column_value,
    sqlite3_commit_hook,
    sqlite3_complete,
    sqlite3_complete16,
    sqlite3_create_collation,
    sqlite3_create_collation16,
    sqlite3_create_function,
    sqlite3_create_function16,
    sqlite3_create_module,
    sqlite3_data_count,
    sqlite3_db_handle,
    sqlite3_declare_vtab,
    sqlite3_enable_shared_cache,
    sqlite3_errcode,
    sqlite3_errmsg,
    sqlite3_errmsg16,
    sqlite3_exec,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_expired,
#else
    0,
#endif
    sqlite3_finalize,
    sqlite3_free,
    sqlite3_free_table,
    sqlite3_get_autocommit,
    sqlite3_get_auxdata,
    sqlite3_get_table,
    0, /* Was sqlite3_global_recover(), but that function is deprecated */
    sqlite3_interrupt,
    sqlite3_last_insert_rowid,
    sqlite3_libversion,
    sqlite3_libversion_number,
    sqlite3_malloc,
    sqlite3_mprintf,
    sqlite3_open,
    sqlite3_open16,
    sqlite3_prepare,
    sqlite3_prepare16,
    sqlite3_profile,
    sqlite3_progress_handler,
    sqlite3_realloc,
    sqlite3_reset,
    sqlite3_result_blob,
    sqlite3_result_double,
    sqlite3_result_error,
    sqlite3_result_error16,
    sqlite3_result_int,
    sqlite3_result_int64,
    sqlite3_result_null,
    sqlite3_result_text,
    sqlite3_result_text16,
    sqlite3_result_text16be,
    sqlite3_result_text16le,
    sqlite3_result_value,
    sqlite3_rollback_hook,
    sqlite3_set_authorizer,
    sqlite3_set_auxdata,
    sqlite3_snprintf,
    sqlite3_step,
    sqlite3_table_column_metadata,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_thread_cleanup,
#else
    0,
#endif
    sqlite3_total_changes,
    sqlite3_trace,
#ifndef SQLITE_OMIT_DEPRECATED
    sqlite3_transfer_bindings,
#else
    0,
#endif
    sqlite3_update_hook,
    sqlite3_user_data,
    sqlite3_value_blob,
    sqlite3_value_bytes,
    sqlite3_value_bytes16,
    sqlite3_value_double,
    sqlite3_value_int,
    sqlite3_value_int64,
    sqlite3_value_numeric_type,
    sqlite3_value_text,
    sqlite3_value_text16,
    sqlite3_value_text16be,
    sqlite3_value_text16le,
    sqlite3_value_type,
    sqlite3_vmprintf,
    /*
    ** The original API set ends here.  All extensions can call any
    ** of the APIs above provided that the pointer is not NULL.  But
    ** before calling APIs that follow, extension should check the
    ** sqlite3_libversion_number() to make sure they are dealing with
    ** a library that is new enough to support that API.
    *************************************************************************
    */
    sqlite3_overload_function,

    /*
    ** Added after 3.3.13
    */
    sqlite3_prepare_v2,
    sqlite3_prepare16_v2,
    sqlite3_clear_bindings,

    /*
    ** Added for 3.4.1
    */
    sqlite3_create_module_v2,

    /*
    ** Added for 3.5.0
    */
    sqlite3_bind_zeroblob,
    sqlite3_blob_bytes,
    sqlite3_blob_close,
    sqlite3_blob_open,
    sqlite3_blob_read,
    sqlite3_blob_write,
    sqlite3_create_collation_v2,
    sqlite3_file_control,
    sqlite3_memory_highwater,
    sqlite3_memory_used,
#ifdef SQLITE_MUTEX_OMIT
    0,
    0,
    0,
    0,
    0,
#else
    sqlite3_mutex_alloc,
    sqlite3_mutex_enter,
    sqlite3_mutex_free,
    sqlite3_mutex_leave,
    sqlite3_mutex_try,
#endif
    sqlite3_open_v2,
    sqlite3_release_memory,
    sqlite3_result_error_nomem,
    sqlite3_result_error_toobig,
    sqlite3_sleep,
    sqlite3_soft_heap_limit,
    sqlite3_vfs_find,
    sqlite3_vfs_register,
    sqlite3_vfs_unregister,

    /*
    ** Added for 3.5.8
    */
    sqlite3_threadsafe,
    sqlite3_result_zeroblob,
    sqlite3_result_error_code,
    sqlite3_test_control,
    sqlite3_randomness,
    sqlite3_context_db_handle,

    /*
    ** Added for 3.6.0
    */
    sqlite3_extended_result_codes,
    sqlite3_limit,
    sqlite3_next_stmt,
    sqlite3_sql,
    sqlite3_status,

    /*
    ** Added for 3.7.4
    */
    sqlite3_backup_finish,
    sqlite3_backup_init,
    sqlite3_backup_pagecount,
    sqlite3_backup_remaining,
    sqlite3_backup_step,
#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
    sqlite3_compileoption_get,
    sqlite3_compileoption_used,
#else
    0,
    0,
#endif
    sqlite3_create_function_v2,
    sqlite3_db_config,
    sqlite3_db_mutex,
    sqlite3_db_status,
    sqlite3_extended_errcode,
    sqlite3_log,
    sqlite3_soft_heap_limit64,
    sqlite3_sourceid,
    sqlite3_stmt_status,
    sqlite3_strnicmp,
#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
    sqlite3_unlock_notify,
#else
    0,
#endif
#ifndef SQLITE_OMIT_WAL
    sqlite3_wal_autocheckpoint,
    sqlite3_wal_checkpoint,
    sqlite3_wal_hook,
#else
    0,
    0,
    0,
#endif
    sqlite3_blob_reopen,
    sqlite3_vtab_config,
    sqlite3_vtab_on_conflict,
    sqlite3_close_v2,
    sqlite3_db_filename,
    sqlite3_db_readonly,
    sqlite3_db_release_memory,
    sqlite3_errstr,
    sqlite3_stmt_busy,
    sqlite3_stmt_readonly,
    sqlite3_stricmp,
    sqlite3_uri_boolean,
    sqlite3_uri_int64,
    sqlite3_uri_parameter,
    sqlite3_vsnprintf,
    sqlite3_wal_checkpoint_v2,
    /* Version 3.8.7 and later */
    sqlite3_auto_extension,
    sqlite3_bind_blob64,
    sqlite3_bind_text64,
    sqlite3_cancel_auto_extension,
    sqlite3_load_extension,
    sqlite3_malloc64,
    sqlite3_msize,
    sqlite3_realloc64,
    sqlite3_reset_auto_extension,
    sqlite3_result_blob64,
    sqlite3_result_text64,
    sqlite3_strglob,
    /* Version 3.8.11 and later */
    (sqlite3_value *(*)(const sqlite3_value *))sqlite3_value_dup,
    sqlite3_value_free,
    sqlite3_result_zeroblob64,
    sqlite3_bind_zeroblob64,
    /* Version 3.9.0 and later */
    sqlite3_value_subtype,
    sqlite3_result_subtype,
    /* Version 3.10.0 and later */
    sqlite3_status64,
    sqlite3_strlike,
    sqlite3_db_cacheflush,
    /* Version 3.12.0 and later */
    sqlite3_system_errno,
    /* Version 3.14.0 and later */
    sqlite3_trace_v2,
    sqlite3_expanded_sql,
    /* Version 3.18.0 and later */
    sqlite3_set_last_insert_rowid,
    /* Version 3.20.0 and later */
    sqlite3_prepare_v3,
    sqlite3_prepare16_v3,
    sqlite3_bind_pointer,
    sqlite3_result_pointer,
    sqlite3_value_pointer,
    /* Version 3.22.0 and later */
    sqlite3_vtab_nochange,
    sqlite3_value_nochange,
    sqlite3_vtab_collation,
    /* Version 3.24.0 and later */
    sqlite3_keyword_count,
    sqlite3_keyword_name,
    sqlite3_keyword_check,
    sqlite3_str_new,
    sqlite3_str_finish,
    sqlite3_str_appendf,
    sqlite3_str_vappendf,
    sqlite3_str_append,
    sqlite3_str_appendall,
    sqlite3_str_appendchar,
    sqlite3_str_reset,
    sqlite3_str_errcode,
    sqlite3_str_length,
    sqlite3_str_value,
    /* Version 3.25.0 and later */
    sqlite3_create_window_function,
/* Version 3.26.0 and later */
#ifdef SQLITE_ENABLE_NORMALIZE
    sqlite3_normalized_sql,
#else
    0,
#endif
    /* Version 3.28.0 and later */
    sqlite3_stmt_isexplain,
    sqlite3_value_frombind,
/* Version 3.30.0 and later */
#ifndef SQLITE_OMIT_VIRTUALTABLE
    sqlite3_drop_modules,
#else
    0,
#endif
    /* Version 3.31.0 and later */
    sqlite3_hard_heap_limit64,
    sqlite3_uri_key,
    sqlite3_filename_database,
    sqlite3_filename_journal,
    sqlite3_filename_wal,
};

extern "C" {
const sqlite3_api_routines *sqlite3_api = &sqlite3Apis;
}
