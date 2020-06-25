#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "genomicsqlite.h"
#include "strlcpy.h"
#include <htslib/vcf.h>
#include <iostream>
#include <memory>
#include <sqlite3.h>
#include <string>

#ifndef NDEBUG
#define _DBG cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define _DBG false && cerr
#endif

using namespace std;

// split s on delim & return strlen(s). s is damaged by side-effect
template <typename Out>
size_t split(char *s, char delim, Out result, uint64_t maxsplit = ULLONG_MAX) {
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
    return last_token ? (last_token + strlen(last_token) - s) : 0;
}

template <typename Out>
size_t split(string &s, char delim, Out result, uint64_t maxsplit = ULLONG_MAX) {
    return split(&s[0], delim, result, maxsplit);
}

// because std::ostringstream is too slow :(
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
    inline OStringStream &operator<<(const std::string &s) { return *this << s.c_str(); }

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

unique_ptr<SQLite::Database> GenomicSQLiteOpen(const string &dbfile, int flags, int zstd_level = 3,
                                               sqlite3_int64 page_cache_size = -1, int threads = -1,
                                               bool unsafe_load = false) {
    static bool loaded = false;
    unique_ptr<SQLite::Database> db;
    if (!loaded) {
        db.reset(new SQLite::Database(":memory:", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE));
        try {
            db->loadExtension("libgenomicsqlite", nullptr);
        } catch (std::exception &exn) {
            throw std::runtime_error("failed loading libgenomicsqlite shared library: " +
                                     string(exn.what()));
        }
        auto msg = GenomicSQLiteVersionCheck();
        if (msg) {
            throw runtime_error(msg);
        }
        loaded = true;
    }
    db.reset(new SQLite::Database(GenomicSQLiteURI(dbfile, zstd_level, threads, unsafe_load),
                                  SQLITE_OPEN_URI | flags));
    db->exec(GenomicSQLiteTuning(page_cache_size, threads, unsafe_load));
    return db;
}
