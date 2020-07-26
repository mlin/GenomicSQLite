#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include "genomicsqlite.h"
#include "strlcpy.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <sqlite3.h>
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
    vector<Item> ring_;
    int R_;
    unique_ptr<thread> worker_;
    atomic<bool> stop_;
    string errmsg_;
    atomic<long long> p_, // produced
        c_;               // if c_>0 then item (c_-1)%R is currently being consumed
    chrono::duration<double> p_blocked_ = chrono::duration<double>::zero(),
                             c_blocked_ = chrono::duration<double>::zero();
    chrono::time_point<chrono::high_resolution_clock> t0_;

    bool empty() { return p_ == c_; }

    bool full() { return p_ - max(c_.load(), 1LL) == R_ - 1; }

    void background_thread() {
        t0_ = chrono::high_resolution_clock::now();
        do {
            assert(p_ >= c_);
            bool ok = false;
            try {
                ok = Produce(ring_[p_ % R_]);
            } catch (exception &exn) {
                errmsg_ = exn.what();
                if (errmsg_.empty()) {
                    errmsg_ = "unknown error on producer thread";
                }
                stop_ = true;
            }
            if (ok) {
                ++p_;
                if (full()) {
                    auto t_spin = chrono::high_resolution_clock::now();
                    do {
                        this_thread::sleep_for(chrono::milliseconds(1));
                    } while (!stop_ && full());
                    p_blocked_ += chrono::high_resolution_clock::now() - t_spin;
                }
            } else {
                stop_ = true;
            }
        } while (!stop_);
    }

  public:
    BackgroundProducer(int ringsize) : R_(ringsize) {
        assert(R_ > 1);
        stop_ = false;
        p_ = 0;
        c_ = 0;
    }

    virtual ~BackgroundProducer() { abort(); }

    // advance to next item for consumption, return false when item stream has ended successfully,
    // or throw an exception.
    bool next() {
        if (!worker_) {
            while (ring_.size() < R_) {
                ring_.push_back(Item());
            }
            worker_.reset(new thread([this]() { this->background_thread(); }));
        }
        if (empty()) {
            auto t_start = chrono::high_resolution_clock::now();
            while (!stop_ && empty()) {
                this_thread::yield();
            }
            c_blocked_ += chrono::high_resolution_clock::now() - t_start;
        }
        if (stop_ && empty()) {
            if (!errmsg_.empty()) {
                throw runtime_error(errmsg_);
            }
            return false;
        }
        assert(c_ < p_);
        c_++;
        return true;
    }

    // get current item for consumption; defined only after next() returned true
    Item &item() {
        assert(c_ && errmsg_.empty());
        return ring_[(c_ - 1) % R_];
    }

    void abort() {
        stop_ = true;
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
