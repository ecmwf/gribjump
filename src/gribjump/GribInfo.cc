/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
/// @author Tiago Quintino

#include <queue>
#include <chrono>
#include <memory>
#include <numeric>
#include <cassert>
#include <unordered_map>
#include <cmath> // isnan. Temp, only for debug, remove later.
#include <memory>

#include "eckit/io/DataHandle.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/utils/MD5.h"

#include "metkit/codes/GribAccessor.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJumpException.h"

#include "compression/Range.h"
#include "compression/compressors/Simple.h"
#include "compression/compressors/Ccsds.h"


using namespace eckit;
using namespace metkit::grib;

extern "C" {
    unsigned long grib_decode_unsigned_long(const unsigned char* p, long* offset, int bits);
    double grib_power(long s, long n);
}

namespace gribjump {

// Convert ranges to intervals
// TODO(maee): Simplification: Switch to intervals or ranges
std::vector<mc::Range> to_ranges(const std::vector<Interval>& intervals) {
    std::vector<mc::Range> ranges;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(ranges), [](auto interval){
        auto [begin, end] = interval;
        return mc::Range{begin, end - begin};
    });
    return ranges;
}

// Check that intervals are sorted and non-overlapping
bool check_intervals(const std::vector<Interval>& intervals) {
    std::vector<char> check;
    std::transform(intervals.begin(), intervals.end() - 1, intervals.begin() + 1, std::back_inserter(check), [](const auto& a, const auto& b) {
        return a.second <= b.first;
    });
    return std::all_of(check.begin(), check.end(), [](char c) { return c; });
}

std::vector<std::bitset<64>> to_bitset(const Bitmap& bitmap) {
    const size_t size = bitmap.size();
    std::vector<std::bitset<64>> masks;
    for (size_t i = 0; i < (size + 63) / 64; i++) {
        std::bitset<64> mask64;
        for (size_t j = 0; j < 64; j++) {
            if (i * 64 + j < size) {
                mask64[j] = bitmap[i * 64 + j] > 0;
            }
        }
        masks.push_back(mask64);
    }
    assert(masks.size() == (size + 63) / 64);
    return masks;
}


Bitmap bin_to_bitmap_64(const eckit::Buffer& binaryBitmap)
{
    size_t nFullBuckets = binaryBitmap.size() / sizeof(uint64_t);
    size_t lastBucket = binaryBitmap.size() % sizeof(uint64_t);

    Bitmap bitmap(binaryBitmap.size() * 8);
    const uint64_t* bitmap_raw = reinterpret_cast<const uint64_t*>(binaryBitmap.data());

    size_t j;
    uint64_t b64;
    uint64_t b64_tmp;
    for (j = 0; j < nFullBuckets; ++j) {
        b64 = bitmap_raw[j];
        if constexpr (BYTE_ORDER == LITTLE_ENDIAN) {
            b64 = __builtin_bswap64(b64);
            //b64 = ((b64_tmp & 0x00000000FFFFFFFF) << 32) | ((b64_tmp & 0xFFFFFFFF00000000) >> 32);
            //b64 = ((b64_tmp & 0x0000FFFF0000FFFF) << 16) | ((b64_tmp & 0xFFFF0000FFFF0000) >> 16);
            //b64 = ((b64_tmp & 0x00FF00FF00FF00FF) << 8)  | ((b64_tmp & 0xFF00FF00FF00FF00) >> 8);
        }

        for (size_t i = 0; i < 64; ++i) {
            bitmap[j * 64 + i] = (b64 >> (63 - i)) & 1;
        }
    }

    if (lastBucket > 0) {
        b64 = bitmap_raw[nFullBuckets];
        if constexpr (BYTE_ORDER == LITTLE_ENDIAN) {
            b64 = __builtin_bswap64(b64);
            //b64 = ((b64_tmp & 0x00000000FFFFFFFF) << 32) | ((b64_tmp & 0xFFFFFFFF00000000) >> 32);
            //b64 = ((b64_tmp & 0x0000FFFF0000FFFF) << 16) | ((b64_tmp & 0xFFFF0000FFFF0000) >> 16);
            //b64 = ((b64_tmp & 0x00FF00FF00FF00FF) << 8)  | ((b64_tmp & 0xFF00FF00FF00FF00) >> 8);
        }
        for (size_t i = 0; i < lastBucket * 8; ++i) {
            bitmap[nFullBuckets * 64 + i] = (b64 >> (63 - i)) & 1;
        }
    }
    return bitmap;
}


Bitmap bin_to_bitmap_8(const eckit::Buffer& binaryBitmap)
{
    size_t nFullBuckets = binaryBitmap.size();

    Bitmap bitmap(binaryBitmap.size() * 8);
    const uint8_t* bitmap_raw = reinterpret_cast<const uint8_t*>(binaryBitmap.data());

    size_t j;
    for (j = 0; j < nFullBuckets; ++j) {
        const uint8_t b8 = bitmap_raw[j];
        for (size_t i = 0; i < 8; ++i) {
            bitmap[j * 8 + i] = b8 >> (7 - i) & 1;
        }
    }

    return bitmap;
}




static GribAccessor<long>          editionNumber("editionNumber");
static GribAccessor<long>          bitmapPresent("bitmapPresent");
static GribAccessor<long>          binaryScaleFactor("binaryScaleFactor");
static GribAccessor<long>          decimalScaleFactor("decimalScaleFactor");
static GribAccessor<unsigned long> bitsPerValue("bitsPerValue");
static GribAccessor<unsigned long> ccsdsFlags("ccsdsFlags", true);
static GribAccessor<unsigned long> ccsdsBlockSize("ccsdsBlockSize", true);
static GribAccessor<unsigned long> ccsdsRsi("ccsdsRsi", true);
static GribAccessor<double>        referenceValue("referenceValue");
static GribAccessor<unsigned long> offsetBeforeData("offsetBeforeData");
static GribAccessor<unsigned long> offsetAfterData("offsetAfterData");
static GribAccessor<unsigned long> offsetBeforeBitmap("offsetBeforeBitmap");
static GribAccessor<unsigned long> numberOfValues("numberOfValues");
static GribAccessor<unsigned long> numberOfDataPoints("numberOfDataPoints");
static GribAccessor<long>          sphericalHarmonics("sphericalHarmonics", true);
static GribAccessor<unsigned long> totalLength("totalLength");
static GribAccessor<unsigned long> offsetBSection6("offsetBSection6");
static GribAccessor<std::string> md5GridSection("md5GridSection");
static GribAccessor<std::string> packingType("packingType");

// TODO(maee): Why do we need this?
static Mutex mutex;

// GRIB does not in general specify what do use in place of missing value.
constexpr double MISSING_VALUE = std::numeric_limits<double>::quiet_NaN();
constexpr size_t MISSING_INDEX = -1;

static int bits[65536] = {
#include "metkit/pointdb/bits.h"
};

JumpInfo JumpInfo::fromFile(const eckit::PathName& path, uint16_t msg_id) {
    // File has a vector of offsets to each info object.
    // Read the offsets, then read the msg_id'th info object.

    JumpInfo info;
    eckit::FileStream f(path, "r");
    size_t n;
    f >> n;
    for (size_t i = 0; i < n; i++) {
        JumpInfo info(f);
        if (i == msg_id) {
            f.close();
            return info;
        }
    }
    f.close();

    // hit eof without returning
    std::stringstream ss;
    ss << "msg_id " << msg_id << " not found in file " << path;
    throw JumpException(ss.str(), Here());
}

JumpInfo::JumpInfo():version_(currentVersion_), numberOfValues_(0) {
}

JumpInfo::JumpInfo(const GribHandle& h) : version_(currentVersion_), numberOfValues_(0) {
    update(h);
}


void JumpInfo::update(const GribHandle& h) {
    editionNumber_      = editionNumber(h);
    if (editionNumber_ != 1 && editionNumber_ != 2) {
        std::stringstream ss;
        ss << "Unsupported GRIB edition number: " << editionNumber_;
        throw JumpException(ss.str(), Here());
    }
    binaryScaleFactor_  = binaryScaleFactor(h);
    decimalScaleFactor_ = decimalScaleFactor(h);
    bitsPerValue_       = bitsPerValue(h);
    ccsdsFlags_         = ccsdsFlags(h);
    ccsdsBlockSize_     = ccsdsBlockSize(h);
    ccsdsRsi_           = ccsdsRsi(h);
    referenceValue_     = referenceValue(h);
    offsetBeforeData_   = offsetBeforeData(h);
    offsetAfterData_    = offsetAfterData(h);
    numberOfDataPoints_ = numberOfDataPoints(h);
    numberOfValues_     = numberOfValues(h);
    sphericalHarmonics_ = sphericalHarmonics(h); // todo: make quiet
    totalLength_        = totalLength(h);
    md5GridSection_     = md5GridSection(h);
    packingType_        = packingType(h);

    if (!(packingType_ == "grid_ccsds" || packingType_ == "grid_simple")) {
        std::stringstream ss;
        ss << "Unsupported packing type: " << packingType_;
        throw JumpException(ss.str(), Here());
    }

    bitmapPresent_ = bitmapPresent(h);
    constexpr size_t offsetToBitmap = 6;
    if (bitmapPresent_) {
        offsetBeforeBitmap_ = editionNumber_ == 1? offsetBeforeBitmap(h) : offsetBSection6(h) + offsetToBitmap;
    }
    else {
        offsetBeforeBitmap_ = 0;
    }

    binaryMultiplier_ = grib_power(binaryScaleFactor_, 2);
    decimalMultiplier_ = grib_power(-decimalScaleFactor_, 10);
}

JumpInfo::JumpInfo(eckit::Stream& s) {
    s >> version_;
    if (version_ != currentVersion_) {
        std::stringstream ss;
        ss << "Bad JumpInfo version found:";
        ss << "Expected " << +currentVersion_ << ", found " << +version_ << std::endl;
        throw JumpException(ss.str(), Here());
    }
    s >> editionNumber_;
    s >> binaryScaleFactor_;
    s >> decimalScaleFactor_;
    s >> bitsPerValue_;
    s >> referenceValue_;
    s >> offsetBeforeData_;
    s >> offsetAfterData_;
    s >> numberOfDataPoints_;
    s >> numberOfValues_;
    s >> offsetBeforeBitmap_;
    s >> sphericalHarmonics_;
    s >> binaryMultiplier_;
    s >> decimalMultiplier_;
    s >> totalLength_;
    s >> msgStartOffset_;
    std::string md5;
    std::string packing;
    s >> md5;
    s >> packing;
    md5GridSection_ = md5;
    packingType_ = packing;
    s >> ccsdsFlags_;
    s >> ccsdsBlockSize_;
    s >> ccsdsRsi_;
    s >> ccsdsOffsets_;
    s >> chunkSizeN_;
    s >> countMissings_;
}

void JumpInfo::encode(eckit::Stream& s) const {
    s << currentVersion_;
    s << editionNumber_;
    s << binaryScaleFactor_;
    s << decimalScaleFactor_;
    s << bitsPerValue_;
    s << referenceValue_;
    s << offsetBeforeData_;
    s << offsetAfterData_;
    s << numberOfDataPoints_;
    s << numberOfValues_;
    s << offsetBeforeBitmap_;
    s << sphericalHarmonics_;
    s << binaryMultiplier_;
    s << decimalMultiplier_;
    s << totalLength_;
    s << msgStartOffset_;
    s << md5GridSection_;
    s << packingType_;
    s << ccsdsFlags_;
    s << ccsdsBlockSize_;
    s << ccsdsRsi_;
    s << ccsdsOffsets_;
    s << chunkSizeN_;
    s << countMissings_;
}

size_t JumpInfo::streamSize() const {
    size_t size = 0;
    size += sizeof(version_);
    size += sizeof(editionNumber_);
    size += sizeof(binaryScaleFactor_);
    size += sizeof(decimalScaleFactor_);
    size += sizeof(bitsPerValue_);
    size += sizeof(referenceValue_);
    size += sizeof(offsetBeforeData_);
    size += sizeof(numberOfDataPoints_);
    size += sizeof(numberOfValues_);
    size += sizeof(offsetBeforeBitmap_);
    size += sizeof(sphericalHarmonics_);
    size += sizeof(binaryMultiplier_);
    size += sizeof(decimalMultiplier_);
    size += sizeof(totalLength_);
    size += sizeof(msgStartOffset_);
    size += md5GridSection_.size();
    size += packingType_.size();
    size += sizeof(ccsdsFlags_);
    size += sizeof(ccsdsBlockSize_);
    size += sizeof(ccsdsRsi_);
    size += sizeof(size_t) * ccsdsOffsets_.size();
    size += sizeof(chunkSizeN_);
    size += sizeof(size_t) * countMissings_.size();

    // Assert that the type of elements in ccsdsOffsets_ is size_t.
    // As this may change in the future.
    static_assert(std::is_same_v<decltype(ccsdsOffsets_)::value_type, size_t>);
    return size;
}

void JumpInfo::print(std::ostream& s) const {
    s << "JumpInfo[";
    s << "version=" << +version_ << ",";
    s << "editionNumber=" << editionNumber_ << ",";
    s << "binaryScaleFactor=" << binaryScaleFactor_ << ",";
    s << "decimalScaleFactor=" << decimalScaleFactor_ << ",";
    s << "bitsPerValue=" << bitsPerValue_ << ",";
    s << "ccsdsFlags=" << ccsdsFlags_ << ",";
    s << "ccsdsBlockSize=" << ccsdsBlockSize_ << ",";
    s << "ccsdsRsi=" << ccsdsRsi_ << ",";
    s << "referenceValue=" << referenceValue_ << ",";
    s << "offsetBeforeData=" << offsetBeforeData_ << ",";
    s << "offsetAfterData=" << offsetAfterData_ << ",";
    s << "numberOfDataPoints=" << numberOfDataPoints_ << ",";
    s << "numberOfValues=" << numberOfValues_ << ",";
    s << "offsetBeforeBitmap=" << offsetBeforeBitmap_ << ",";
    s << "sphericalHarmonics=" << sphericalHarmonics_ << ",";
    s << "binaryMultiplier=" << binaryMultiplier_ << ",";
    s << "decimalMultiplier=" << decimalMultiplier_ << ",";
    s << "totalLength=" << totalLength_ << ",";
    s << "msgStartOffset=" << msgStartOffset_ << ",";
    s << "md5GridSection=" << md5GridSection_  << ",";
    s << "packingType=" << packingType_;
    s << "]";
    s << std::endl;
}

// n: 64-bit word from bitmap
// count: number of set bits in previous words
// newIndex: vector to push new indexes to
// edges: queue of bit positions where range starts/ends
// inRange: true if currently in a range
// bp: bit position in bitmap
void accumulateIndexes(uint64_t &n, size_t &count, std::vector<size_t> &newIndex, std::queue<size_t> &edges, bool &inRange, size_t &bp) {
    // Used by extractRanges to parse bitmap and calculate new indexes.
    // Counts set bits in n, and pushes new indexes to newIndex.

    ASSERT(!edges.empty());
    ASSERT(bp%64 == 0); // at start of new word

    constexpr uint64_t msb64 = 0x8000000000000000; // 0b100...0
    size_t endbit = bp + 64;
    while (bp < endbit) {
        if (bp == edges.front()) {
            inRange = !inRange;
            edges.pop();
            if (edges.empty()) break;
        }
        bool set = n & msb64;
        if (inRange) newIndex.push_back(set ? count : MISSING_INDEX);
        count += set ? 1 : 0;
        n <<= 1;
        ++bp;
    }
}

void JumpInfo::updateCcsdsOffsets(const JumpHandle& f){
    std::cerr << "WARNING: compute Offsets from data" << std::endl;
    if (packingType_ != "grid_ccsds") return;

    auto data_range = mc::Range{msgStartOffset_ + offsetBeforeData_, offsetAfterData_ - offsetBeforeData_};

    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, data_range);
    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);
    ccsds.n_elems(numberOfValues_);

    auto encoded = data_accessor->read();
    if (encoded.size() == 0)
        throw std::runtime_error("encoded.size() == 0");

    ccsds.decode(encoded); // Decoding the entire message to get offsets

    ccsdsOffsets_ = ccsds.offsets().value();
}


void JumpInfo::updateMissingValues(const JumpHandle& f) {
    std::cerr << "WARNING: compute missing values from data" << std::endl;
    auto bitmap = get_bitmap(f);
    size_t nChunks = 60;
    size_t chunkSizeN = (bitmap.size() + nChunks - 1) / nChunks;

    nChunks = (bitmap.size() + chunkSizeN - 1) / chunkSizeN;

    std::vector<Bitmap> bitmapChunks;
    for (size_t i = 0; i < nChunks; i++) {
        size_t begin = i * chunkSizeN;
        size_t end = std::min(bitmap.size(), (i + 1) * chunkSizeN);
        assert(end <= numberOfDataPoints_);
        bitmapChunks.push_back(Bitmap{bitmap.begin() + begin, bitmap.begin() + end});
    }

    std::transform(bitmapChunks.begin(), bitmapChunks.end(), std::back_inserter(countMissings_), [](const auto& chunk) {
        return std::count(chunk.begin(), chunk.end(), false);
    });
}


std::vector<Values> JumpInfo::get_ccsds_values(const JumpHandle& f, const std::vector<Interval> &intervals) const {
    auto ranges = to_ranges(intervals);
    auto data_range = mc::Range{msgStartOffset_ + offsetBeforeData_, offsetAfterData_ - offsetBeforeData_};
    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, data_range);

    mc::CcsdsDecompressor<double> ccsds{};

    // We assume offsets have already been read by now.
    ASSERT(ccsdsOffsets_.size() > 0);

    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_)
        .offsets(ccsdsOffsets_);

    return ccsds.decode(data_accessor, ranges);
}

std::vector<Values> JumpInfo::get_simple_values(const JumpHandle& f, const std::vector<Interval> &intervals) const {
    // TODO(maee): Optimize this
    auto ranges = to_ranges(intervals);

    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, mc::Range{msgStartOffset_ + offsetBeforeData_, offsetAfterData_ - offsetBeforeData_});

    mc::SimpleDecompressor<double> simple{};
    simple
        .bits_per_value(bitsPerValue_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);

    return simple.decode(data_accessor, ranges);
}

// Read the entire bitmap from a GRIB file
// TODO(maee): optimization: read only the bitmap for the requested interval
Bitmap JumpInfo::get_bitmap(const JumpHandle& f) const {
    eckit::Offset offset = msgStartOffset_ + offsetBeforeBitmap_;
    auto bitmapSize = (numberOfDataPoints_ + 7) / 8;

    if (bitmapSize == 0)
        return Bitmap{};

    if (bitsPerValue_ == 0)
        return Bitmap(numberOfDataPoints_, true);

    eckit::Buffer binaryBitmap(numberOfDataPoints_);
    if (f.seek(offset) != offset)
        throw std::runtime_error("Bitmap seek failed");

    if (f.read(binaryBitmap, bitmapSize) != bitmapSize)
        throw std::runtime_error("Bitmap read failed");

    Bitmap bitmap;
    for (size_t i = 0; i < numberOfDataPoints_; i++) {
        bitmap.push_back(binaryBitmap[i / 8] & (1 << (7 - i % 8)));
    }

    return bitmap;
}


class BitmapInfo {
public:
    BitmapInfo(const std::vector<size_t>& countMissingsBeforeChunk,
               const mc::Range& requestedRangeBits,
               const mc::Range& bucketRangeBits,
               const Bitmap& bucketBitmap,
               size_t chunkSizeN) :
        requestedRangeBits_{requestedRangeBits},
        bucketRangeBits_{bucketRangeBits},
        bucketBitmap_{bucketBitmap},
        chunkSizeN_{chunkSizeN},
        countMissingsBeforeChunk_{countMissingsBeforeChunk}
    {}

    size_t get_chunk_idx(size_t pos) {
        return pos / chunkSizeN_;
    }


    size_t count_missings(size_t pos, size_t bucket_pos, size_t chunk_size) {
        size_t chunk_idx = get_chunk_idx(pos);
        size_t bucket_idx = get_chunk_idx(bucket_pos);
        assert(bucket_pos % chunk_size == 0);

        size_t offset = (chunk_idx - bucket_idx) * chunk_size;
        size_t size = pos % chunk_size;

        //std::cerr << "bitmap: " << std::endl;
        //for (const auto& b : bucketBitmap_) {
        //    std::cerr << b << "";
        //}
        //std::cerr << std::endl;

        //std::cerr << "pos: " << pos << std::endl;
        //std::cerr << "chunk_idx: " << chunk_idx << std::endl;
        //std::cerr << "bucket_idx: " << bucket_idx << std::endl;
        //std::cerr << "chunk_size: " << chunk_size << std::endl;
        //std::cerr << "offset: " << offset << std::endl;
        //std::cerr << "size: " << size << std::endl;
        //std::cerr << "bucketRangeBits_: " << bucketRangeBits_.first << " " << bucketRangeBits_.second << std::endl;
        //std::cerr << "bucketBitmap_.size(): " << bucketBitmap_.size() << std::endl;
        //std::cerr << "countMissingsBeforeChunk_[chunk_idx]: " << countMissingsBeforeChunk_[chunk_idx] << std::endl;
        //std::cerr << "std::count(bucketBitmap_.begin() + offset, bucketBitmap_.begin() + offset + size, false): " << std::count(bucketBitmap_.begin() + offset, bucketBitmap_.begin() + offset + size, false) << std::endl;

        assert(offset <= bucketBitmap_.size() + 1);
        assert(size <= bucketBitmap_.size());

        size_t n =
            countMissingsBeforeChunk_[chunk_idx] +
            std::count(bucketBitmap_.begin() + offset, bucketBitmap_.begin() + offset + size, false);

        //std::cerr << "count_missings: " << n << std::endl;

        return n;
    }

    Interval get_new_interval() {
        const auto [requestedOffsetBits, requestedSizeBits] = requestedRangeBits_;
        const auto [bucketOffsetBits, _] = bucketRangeBits_;

        //std::cerr << "requestedRange_: " << requestedRangeBits_.first << " " << requestedRangeBits_.second << std::endl;
        size_t countMissingBeforeStart = count_missings(requestedOffsetBits, bucketOffsetBits, chunkSizeN_);
        size_t countMissingBeforeEnd = count_missings(requestedOffsetBits + requestedSizeBits, bucketOffsetBits, chunkSizeN_);

        Range newIntervalBits{requestedOffsetBits - countMissingBeforeStart, requestedOffsetBits + requestedSizeBits - countMissingBeforeEnd};

        //std::cerr << "requestedRange_: " << requestedRangeBits_.first << " " << requestedRangeBits_.second << std::endl;
        //std::cerr << "new interval: " << newIntervalBits.first << " " << newIntervalBits.second << std::endl;
        assert(newIntervalBits.second - newIntervalBits.first <= requestedSizeBits);
        return newIntervalBits;
    }

    Bitmap get_new_bitmap() {
        const auto [requestedOffsetBits, requestedSizeBits] = requestedRangeBits_;
        size_t chunkOffsetIdx = requestedOffsetBits / chunkSizeN_;
        size_t chunkSizeIdx = (requestedSizeBits + chunkSizeN_ - 1) / chunkSizeN_;
        auto [bucketOffsetBits, bucketSizeBits] = bucketRangeBits_;
        size_t bucketOffsetIdx = bucketOffsetBits / chunkSizeN_;
        auto chunkOffsetN = (chunkOffsetIdx - bucketOffsetIdx) * chunkSizeN_;

        auto startN = chunkOffsetN + requestedOffsetBits % chunkSizeN_;
        auto endN = startN + requestedSizeBits;

        return Bitmap{bucketBitmap_.begin() + startN, bucketBitmap_.begin() + endN};
    }

private:
    mc::Range requestedRangeBits_;
    mc::Range bucketRangeBits_;
    Bitmap bucketBitmap_;
    size_t chunkSizeN_;
    std::vector<size_t> countMissingsBeforeChunk_;
};




// Read the entire bitmap from a GRIB file
// TODO(maee): optimization: read only the bitmap for the requested interval
std::pair<std::vector<Interval>, std::vector<Bitmap>> JumpInfo::calculate_intervals(const JumpHandle& f, const std::vector<Interval>& intervals, std::vector<size_t> countMissingsBeforeChunk, size_t chunkSizeN) const {
    eckit::Offset offset = msgStartOffset_ + offsetBeforeBitmap_;
    auto maxBitmapSizeBytes = (numberOfDataPoints_ + 7) / 8;

    std::shared_ptr<GribJumpDataAccessor> accessor = std::make_shared<GribJumpDataAccessor>(&f, mc::Range{offset, maxBitmapSizeBytes});

    // Convert Intervals to Ranges
    std::vector<mc::Range> requestedRangesBits;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(requestedRangesBits), [](const auto& interval) {
        auto [begin, end] = interval;
        return mc::Range{begin, end - begin};
    });


    std::unordered_map<Range, std::pair<Range, std::shared_ptr<Bitmap>>> ranges_map;

    // Combine chunkRangesIdx
    // find which sub_ranges are in which buckets
    mc::RangeBuckets bucketsBits;
    for (const auto& requestedRangeBits : requestedRangesBits) {
        bucketsBits << requestedRangeBits;
    }

    // precise chorno timer
    static double timer_sum = 0.0;
    static double io_sum = 0.0;

    // Read chunks
    // inverse buckets and associate data with chunkRangesIdx
    for (const auto& [bitmapRangeBits, bitmapSubRangesBits] : bucketsBits) {
        // print subranges
        std::cerr << "bitmap Interval:" << std::endl;
        std::cerr << "\t" << bitmapRangeBits.first << " " << bitmapRangeBits.second + bitmapRangeBits.first << std::endl;
        //std::cerr << "bitmab Sub-Intervals: " << std::endl;
        //for (const auto& bitmapSubRangeBits : bitmapSubRangesBits) {
        //    std::cerr << "\tSubinterval: " << bitmapSubRangeBits.first << " " << bitmapSubRangeBits.second  + bitmapSubRangeBits.first<< std::endl;
        //}
        //std::cerr << std::endl;

        auto [binaryBitmapOffsetBits, binaryBitmapSizeBits] = bitmapRangeBits;
        auto bitmapOffsetBits = binaryBitmapOffsetBits / chunkSizeN * chunkSizeN;
        auto bitmapOffsetBytes = bitmapOffsetBits / 8;
        auto bitmapOffsetDiffBits = bitmapOffsetBits - bitmapOffsetBytes * 8;


        size_t chunkStartIdx = binaryBitmapOffsetBits / chunkSizeN;
        size_t chunkStartBytes = chunkStartIdx * chunkSizeN / 8;
        size_t diffStartBits = binaryBitmapOffsetBits - chunkStartBytes * 8;

        size_t chunkEndIdx = (binaryBitmapOffsetBits + binaryBitmapSizeBits + chunkSizeN - 1) / chunkSizeN;
        size_t chunkEndBytes = (chunkEndIdx * chunkSizeN + 7) / 8;

        size_t diffEndBits = 0;

        if (chunkEndBytes > accessor->eof()) {
            chunkEndBytes = accessor->eof();
        }

        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
        auto alignedBucketRangeBytes = mc::Range{chunkStartBytes, chunkEndBytes - chunkStartBytes};

        auto binaryBucketBitmap = accessor->read(alignedBucketRangeBytes);

        std::chrono::high_resolution_clock::time_point start_io = std::chrono::high_resolution_clock::now();
        //Bitmap bitmap_tmp = bin_to_bitmap_64(binaryBucketBitmap);
        Bitmap bitmap_tmp = bin_to_bitmap_8(binaryBucketBitmap);
        std::chrono::high_resolution_clock::time_point end_io = std::chrono::high_resolution_clock::now();
        io_sum += std::chrono::duration_cast<std::chrono::milliseconds>(end_io - start_io).count();

        // Fix bitmap size
        auto bitmap = std::make_shared<Bitmap>(
            Bitmap{
                bitmap_tmp.begin() + bitmapOffsetDiffBits,
                bitmap_tmp.begin() + bitmapOffsetDiffBits + binaryBitmapSizeBits + binaryBitmapOffsetBits});

        // print bitmap
        //std::cerr << "Created bitmap interval: " << std::endl;
        //std::cerr << "\t" << bitmapOffsetBits << " " << bitmapOffsetBits + bitmapRangeBits.second << std::endl;
        for (const auto& bitmapSubRangeBits : bitmapSubRangesBits) {
            ranges_map[bitmapSubRangeBits] = std::make_pair(Range{bitmapOffsetBits, bitmapRangeBits.second}, bitmap);
        }
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
        timer_sum += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cerr 
            << "\tread bitmap took: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " \\ " << timer_sum << " milliseconds" << std::endl
            << "\tio took " << std::chrono::duration_cast<std::chrono::milliseconds>(end_io - start_io).count() << " \\ " << io_sum << " milliseconds" << std::endl;

    }


    // Compute Intervals
    // assign data to chunkRangesBits
    std::vector<Bitmap> bitmaps;
    std::vector<Interval> new_intervals;
    for (size_t i = 0; i < ranges_map.size(); ++i) {

    //BitmapInfo(const std::vector<size_t>& countMissingsBeforeChunk,
    //           const mc::Range& requestedRangeBits,
    //           const mc::Range& bucketRangeBits,
    //           const Bitmap& bucketBitmap,
    //           size_t chunkSizeN) :

        BitmapInfo bitmapInfo(
            countMissingsBeforeChunk,
            requestedRangesBits[i],
            ranges_map[requestedRangesBits[i]].first,
            *ranges_map[requestedRangesBits[i]].second,
            chunkSizeN);
        new_intervals.push_back(bitmapInfo.get_new_interval());
        bitmaps.push_back(bitmapInfo.get_new_bitmap());
    }

    assert(bitmaps.size() == new_intervals.size());

    return std::make_pair(new_intervals, bitmaps);
}






ExtractionResult JumpInfo::extractRanges(const JumpHandle& f, const std::vector<Interval>& intervals) const {
    ASSERT(check_intervals(intervals));
    ASSERT(!sphericalHarmonics_);
    // TODO(maee): do we need this check?
    //for (const auto& i : intervals) {
    //    ASSERT(i.first < i.second);
    //    ASSERT(i.second <= numberOfDataPoints_);
    //}

    std::vector<std::vector<std::bitset<64>>> all_masks;
    std::vector<Values> all_values;

    if (intervals.size() == 0) {
        return ExtractionResult(all_values, all_masks);
    }

    // XXX can constant fields have a bitmap? We explicitly assuming not. TODO handle constant fields with bitmaps
    if (bitsPerValue_ == 0) { // constant field
        // ASSERT(!offsetBeforeBitmap_);
        std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_values), [this](const auto& interval) {
            return Values(interval.second - interval.first, referenceValue_);
        });
        std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_masks), [](const auto& interval) {
            return to_bitset(std::vector<BitmapValueType>(interval.second - interval.first, true));
        });
        return ExtractionResult(all_values, all_masks);
    }

    std::vector<Values> (JumpInfo::*get_values)(const JumpHandle&, const std::vector<Interval> &) const;

    if (packingType_ == "grid_ccsds")
        get_values = &JumpInfo::get_ccsds_values;
    else if (packingType_ == "grid_simple")
        get_values = &JumpInfo::get_simple_values;
    else
        throw std::runtime_error("Unsupported packing type \"" + std::string{packingType_} + "\"");

    if (!offsetBeforeBitmap_) { // no bitmap
        all_values = (this->*get_values)(f, intervals);
        std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_masks), [](const auto& interval) {
            return to_bitset(std::vector<BitmapValueType>(interval.second - interval.first, true));
        });
    }
    else { // bitmap
        std::vector<size_t> countMissingsBeforeChunk;
        size_t chunkSizeN = chunkSizeN_;

        countMissingsBeforeChunk.push_back(0);
        std::partial_sum(countMissings_.begin(), countMissings_.end(), std::back_inserter(countMissingsBeforeChunk));

        //std::cerr << "countMissingsBeforeChunk: ";
        //for (const auto& i : countMissingsBeforeChunk) {
        //    std::cerr << i << " ";
        //}
        //std::cerr << std::endl;

        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
        auto [new_intervals, new_bitmaps] = calculate_intervals(f, intervals, countMissingsBeforeChunk, chunkSizeN);
        std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
        std::cerr << "calculate_intervals took " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " milliseconds" << std::endl;

        auto all_decoded_values = (this->*get_values)(f, new_intervals);
        // print decoded values
        //std::cerr << "decoded values: ";
        //for (const auto& values : all_decoded_values) {
        //    for (const auto& v : values) {
        //        std::cerr << v << " ";
        //    }
        //}
        //std::cerr << std::endl;

        assert(new_bitmaps.size() == new_intervals.size());

        for (size_t i = 0; i < intervals.size(); ++i) {
            Values values;
            auto test_bitmap = new_bitmaps[i];
            auto test_interval = new_intervals[i];

            size_t nNonMissing = std::count(test_bitmap.begin(), test_bitmap.end(), true);
            if (nNonMissing != test_interval.second - test_interval.first) {
                std::cerr << "nNonMissing (Bitmap): " << nNonMissing << " test_bitmap_size: " << test_bitmap.size() << " ";
                for (const auto& b : test_bitmap) {
                    std::cerr << b << "";
                }
                std::cerr << std::endl;
                std::cerr << "nNonMissing (NewInternval): " << test_interval.second - test_interval.first << " ";
                std::cerr << " range: " << test_interval.first << " " << test_interval.second << std::endl;
                throw std::runtime_error("nNonMissing (Bitmap) != nNonMissing (NewInternval)");
            }

            for (size_t count = 0, j = 0; j < new_bitmaps[i].size(); ++j) {
                if (new_bitmaps[i][j])
                    values.push_back(all_decoded_values[i][count++]);
                else
                    values.push_back(MISSING_VALUE);
            }
            all_values.push_back(values);
            all_masks.push_back(to_bitset(new_bitmaps[i]));
            // print values
            //std::cerr << "values: ";
            //for (const auto& v : values) {
            //    std::cerr << v << " ";
            //}
            //std::cerr << std::endl;
        }
    }

    return ExtractionResult(all_values, all_masks);
}


} // namespace gribjump

