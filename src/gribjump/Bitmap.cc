#include "Bitmap.h"
#include <algorithm>
#include <array>
#include <cstdlib>

namespace gribjump {

Bitmap::Bitmap(std::shared_ptr<mc::DataAccessor> bitmapAccessor, size_t nBits, size_t n_chunks) :
    nBits_(nBits),
    bitmapAccessor_(bitmapAccessor),
    cachedBits_((nBits_ + 7) / 8 * 8, 0)
{
    cacheRange(mc::Range{0, (nBits_ + 7) / 8});

    std::vector<size_t> countMissings;
    chunkSize_ = (nBits_ + n_chunks - 1) / n_chunks;

    size_t nFullChunks = nBits_ / chunkSize_;
    size_t nRemainder = nBits_ % chunkSize_;

    for (size_t i = 0; i < nFullChunks; i++) {
        countMissings.push_back(std::count(cachedBits_.begin() + i * chunkSize_, cachedBits_.begin() + (i + 1) * chunkSize_, false));
    }

    if (nRemainder > 0) {
        countMissings.push_back(std::count(cachedBits_.begin() + nFullChunks * chunkSize_, cachedBits_.begin() + nBits_, false));
    }

    assert(countMissings.size() > 0);

    countMissingsBeforeChunk_.push_back(0);
    std::partial_sum(countMissings.begin(), countMissings.end(), std::back_inserter(countMissingsBeforeChunk_));
}


Bitmap::Bitmap(std::shared_ptr<mc::DataAccessor> bitmapAccessor, size_t nBits, const std::vector<Interval>& intervals, const std::vector<size_t>& countMissingsBeforeChunk, size_t chunkSize)
    :
    nBits_(nBits),
    chunkSize_(chunkSize),
    bitmapAccessor_(bitmapAccessor),
    cachedBits_((nBits_ + 7) / 8 * 8, 0),
    countMissingsBeforeChunk_(countMissingsBeforeChunk)
{
    std::vector<mc::Range> accessedChunksIdx;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(accessedChunksIdx), [&](const auto& interval) {
        auto [begin, end] = interval;
        size_t startChunkIdx = begin / chunkSize_;
        size_t endChunkIdx = end / chunkSize_;
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
        size_t beginBytes = offsetIdx * chunkSize_ / 8;
        size_t endBytes = ((offsetIdx + sizeIdx) * chunkSize_ + 7) / 8;
        if (endBytes > bitmapAccessor_->eof()) {
            endBytes = bitmapAccessor_->eof();
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

    eckit::Buffer binaryBitmap = bitmapAccessor_->read(rangeBytes);
    const uint8_t* rawBitmap = reinterpret_cast<const uint8_t*>(binaryBitmap.data());

    const auto& [offsetBytes, sizeBytes] = rangeBytes;
    assert((offsetBytes + sizeBytes) * 8 <= cachedBits_.size());
    for (size_t i = 0; i < sizeBytes; i++) {
        const auto& entry = lookupTable[rawBitmap[i]];
        std::copy(entry.begin(), entry.end(), cachedBits_.begin() + (offsetBytes + i) * 8);
    }
}


size_t Bitmap::countMissingsBeforePos(size_t pos) const {
    size_t chunkIdx = pos / chunkSize_;
    size_t chunkPos = chunkIdx * chunkSize_;
    assert(chunkPos <= nBits_);
    assert(pos <= nBits_);
    size_t count = countMissingsBeforeChunk_[chunkIdx] + std::count(cachedBits_.begin() + chunkPos, cachedBits_.begin() + pos, false);
    assert(count < nBits_);
    return count;
}


std::vector<size_t> Bitmap::countMissingsInChunks() const {
    std::vector<size_t> countMissings;
    for (size_t i = 1; i < countMissingsBeforeChunk_.size(); i++) {
        countMissings.push_back(countMissingsBeforeChunk_[i] - countMissingsBeforeChunk_[i - 1]);
    }
    return countMissings;
}


void Bitmap::dump() const {
    size_t count = 0;
    for (size_t i = 0; i < nBits_; i++) {
        std::cerr << (int) cachedBits_[i];
        if (i % chunkSize_ == 0) {
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


} // namespace gribjump
