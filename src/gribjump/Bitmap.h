#pragma once

#include <vector>
#include <stddef.h>
#include <memory>
#include <algorithm>
#include <unordered_map>
#include <cassert>
#include <numeric>

#include "eckit/io/Buffer.h"
#include "compression/Range.h"
#include "compression/DataAccessor.h"
#include "Interval.h"
#include "GribHandleData.h"

namespace gribjump {

class Bitmap {
public:
    Bitmap(std::shared_ptr<mc::DataAccessor> bitmap_accessor, size_t nBits, size_t n_chunks);
    Bitmap(std::shared_ptr<mc::DataAccessor> bitmap_accessor, size_t nBits, const std::vector<Interval>& intervals, const std::vector<size_t>& countMissingsInChunks, size_t chunkSize);

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = bool;
        using difference_type = std::ptrdiff_t;
        using pointer = const bool*;
        using reference = const bool&;

        Iterator(const Bitmap& bitmap, size_t pos) : bitmap_(bitmap), pos_(pos) {}

        Iterator& operator++() {
            ++pos_;
            return *this;
        }
        Iterator operator++(int) {
            Iterator tmp = *this;
            ++pos_;
            return tmp;
        }
        Iterator& operator+(size_t n) {
            pos_ += n;
            return *this;
        }
        bool operator==(const Iterator& other) const {
            return pos_ == other.pos_;
        }
        bool operator!=(const Iterator& other) const {
            return pos_ != other.pos_;
        }
        bool operator*() const {
            return bitmap_.cachedBits_[pos_];
        }
        bool operator <(const Iterator& other) const {
            return pos_ < other.pos_;
        }

    private:
        const Bitmap& bitmap_;
        size_t pos_;
    };

    Iterator begin() const {return Iterator(*this, 0);}
    Iterator end() const {return Iterator(*this, nBits_);}

    size_t countMissingsBeforePos(size_t pos) const;
    std::vector<size_t> countMissingsInChunks() const;
    void dump() const;
    size_t chunkSize() const {return chunkSize_;}

private:
    void cacheRange(const mc::Range& rangeBytes);
    size_t countMissings() const;
    size_t nBits_;

    size_t chunkSize_;
    std::shared_ptr<mc::DataAccessor> bitmapAccessor_;
    std::vector<char> cachedBits_;
    std::vector<size_t> countMissingsBeforeChunk_;
};

} // namespace gribjump
