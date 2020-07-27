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
        do {
            auto p = p_.load(memory_order_acquire);
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
                ++p;
                p_.store(p, memory_order_release);
                if (p - max(c_.load(memory_order_acquire), 1LL) == R_ - 1) {
                    auto t_spin = chrono::high_resolution_clock::now();
                    do {
                        // assumption -- producer will usually be faster than the consumer, and
                        // ringsize_ provides buffer if that's occasionally not the case
                        this_thread::sleep_for(chrono::milliseconds(1));
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

    // advance to next item for consumption, return false when item stream has ended successfully,
    // or throw an exception.
    bool next() {
        if (!worker_) {
            while (ring_.size() < R_) {
                ring_.emplace_back();
            }
            worker_.reset(new thread([this]() { this->background_thread(); }));
        }
        auto c = c_.load(memory_order_acquire), p = p_.load(memory_order_acquire);
        if (c == p) {
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
