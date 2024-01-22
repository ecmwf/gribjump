#include "Bitmap.h"
#include <algorithm>
#include <array>
#include <cstdlib>

namespace gribjump {

Bitmap::Bitmap(std::shared_ptr<mc::DataAccessor> bitmap_accessor, size_t nBits, size_t n_chunks) :
    nBits_(nBits),
    bitmap_accessor_(bitmap_accessor),
    cached_bits_((nBits_ + 7) / 8 * 8, 0)
{
    cacheRange(mc::Range{0, (nBits_ + 7) / 8});

    std::vector<size_t> countMissings;
    chunk_size_ = (nBits_ + n_chunks - 1) / n_chunks;

    size_t n_full_chunks = nBits_ / chunk_size_;
    size_t n_remainder = nBits_ % chunk_size_;

    for (size_t i = 0; i < n_full_chunks; i++) {
        countMissings.push_back(std::count(cached_bits_.begin() + i * chunk_size_, cached_bits_.begin() + (i + 1) * chunk_size_, false));
    }

    if (n_remainder > 0) {
        countMissings.push_back(std::count(cached_bits_.begin() + n_full_chunks * chunk_size_, cached_bits_.begin() + nBits_, false));
    }

    assert(countMissings.size() > 0);

    countMissingsBeforeChunk_.push_back(0);
    std::partial_sum(countMissings.begin(), countMissings.end(), std::back_inserter(countMissingsBeforeChunk_));
}


Bitmap::Bitmap(std::shared_ptr<mc::DataAccessor> bitmap_accessor, size_t nBits, const std::vector<Interval>& intervals, const std::vector<size_t>& countMissingsBeforeChunk, size_t chunk_size)
    :
    nBits_(nBits),
    chunk_size_(chunk_size),
    bitmap_accessor_(bitmap_accessor),
    cached_bits_((nBits_ + 7) / 8 * 8, 0),
    countMissingsBeforeChunk_(countMissingsBeforeChunk)
{
    std::vector<mc::Range> accessedChunksIdx;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(accessedChunksIdx), [&](const auto& interval) {
        auto [begin, end] = interval;
        size_t startChunkIdx = begin / chunk_size_;
        size_t endChunkIdx = end / chunk_size_;
        return mc::Range{startChunkIdx, endChunkIdx - startChunkIdx + 1};
    });

    std::unordered_map<mc::Range, Bitmap> ranges_map;

    // Combine intervals
    mc::RangeBuckets bucketsBits;
    for (const auto& chunkRange : accessedChunksIdx) {
        bucketsBits << chunkRange;
    }

    // Cache chunks
    for (const auto& bucket : bucketsBits) {
        const auto& [chunkRangeIdx, _] = bucket;
        const auto& [offsetIdx, sizeIdx] = chunkRangeIdx;
        size_t beginBytes = offsetIdx * chunk_size_ / 8;
        size_t endBytes = ((offsetIdx + sizeIdx) * chunk_size_ + 7) / 8;
        if (endBytes > bitmap_accessor_->eof()) {
            endBytes = bitmap_accessor_->eof();
        }

        cacheRange(mc::Range{beginBytes, endBytes - beginBytes});
    }
}


void Bitmap::cacheRange(const mc::Range& rangeBytes) {
    static constexpr std::array<std::array<char, 8>, 256> lookupTable = []() constexpr {
        std::array<std::array<char, 8>, 256> table{};
        for (int i = 0; i < 256; ++i) {
            for (int j = 0; j < 8; ++j) {
                table[i][j] = (i >> (7 - j)) & 1;
            }
        }
        return table;
    }();

    eckit::Buffer binaryBitmap = bitmap_accessor_->read(rangeBytes);
    const uint8_t* rawBitmap = reinterpret_cast<const uint8_t*>(binaryBitmap.data());

    const auto& [offsetBytes, sizeBytes] = rangeBytes;
    assert((offsetBytes + sizeBytes) * 8 <= cached_bits_.size());
    for (size_t i = 0; i < sizeBytes; i++) {
        const auto& entry = lookupTable[rawBitmap[i]];
        std::copy(entry.begin(), entry.end(), cached_bits_.begin() + (offsetBytes + i) * 8);
    }
}


size_t Bitmap::countMissingsBeforePos(size_t pos) const {
    size_t chunk_idx = pos / chunk_size_;
    size_t chunk_pos = chunk_idx * chunk_size_;
    assert(chunk_pos <= nBits_);
    assert(pos <= nBits_);
    size_t count = countMissingsBeforeChunk_[chunk_idx] + std::count(cached_bits_.begin() + chunk_pos, cached_bits_.begin() + pos, false);
    assert(count < nBits_);
    return count;
}

} // namespace gribjump
