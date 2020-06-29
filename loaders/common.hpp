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
