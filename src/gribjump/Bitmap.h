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
    Bitmap(std::shared_ptr<mc::DataAccessor> bitmap_accessor, size_t nBits, const std::vector<Interval>& intervals, const std::vector<size_t>& countMissingsInChunks, size_t chunk_size);

    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = bool;
        using difference_type = std::ptrdiff_t;
        using pointer = const bool*;
        using reference = const bool&;

        iterator(const Bitmap& bitmap, size_t pos) : bitmap_(bitmap), pos_(pos) {}

        iterator& operator++() {
            ++pos_;
            return *this;
        }
        iterator operator++(int) {
            iterator tmp = *this;
            ++pos_;
            return tmp;
        }
        iterator& operator+(size_t n) {
            pos_ += n;
            return *this;
        }
        bool operator==(const iterator& other) const {
            return pos_ == other.pos_;
        }
        bool operator!=(const iterator& other) const {
            return pos_ != other.pos_;
        }
        bool operator*() const {
            return bitmap_.cached_bits_[pos_];
        }
        bool operator <(const iterator& other) const {
            return pos_ < other.pos_;
        }

    private:
        const Bitmap& bitmap_;
        size_t pos_;
    };

    iterator begin() const {return iterator(*this, 0);}
    iterator end() const {return iterator(*this, nBits_);}

    size_t countMissingsBeforePos(size_t pos) const;
    std::vector<size_t> countMissingsInChunks() const {
        std::vector<size_t> countMissings;
        for (size_t i = 1; i < countMissingsBeforeChunk_.size(); i++) {
            countMissings.push_back(countMissingsBeforeChunk_[i] - countMissingsBeforeChunk_[i - 1]);
        }
        return countMissings;
    }

    void dump() const {
        size_t count = 0;
        for (size_t i = 0; i < nBits_; i++) {
            std::cerr << (int) cached_bits_[i];
            if (i % chunk_size_ == 0) {
                if (count % 10 == 0) {
                    std::cerr << "\n";
                }
                else {
                    std::cerr << " ";
                }
                count++;
            }
        }
        std::cerr << std::endl;
    }

    size_t chunk_size() const {return chunk_size_;}

private:
    void cacheRange(const mc::Range& rangeBytes);
    size_t countMissings() const;
    size_t nBits_;

    size_t chunk_size_;
    std::shared_ptr<mc::DataAccessor> bitmap_accessor_;
    std::vector<char> cached_bits_;
    std::vector<size_t> countMissingsBeforeChunk_;
};

} // namespace gribjump
