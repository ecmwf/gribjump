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

#include <queue>
#include <memory>
#include <numeric>
#include <cassert>
#include <unordered_map>
#include <cmath> // isnan. Temp, only for debug, remove later.
#include <memory>
#include "eckit/io/DataHandle.h"
#include "eckit/utils/MD5.h"
#include "metkit/codes/GribAccessor.h"
#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJumpException.h"

#include <compressor/compressor.h>
#include <compressor/range.h>
#include <compressor/simple_compressor.h>
#include <compressor/ccsds_compressor.h>

// Convert ranges to intervals
// TODO(maee): Simplification: Switch to intervals or ranges
std::vector<mc::Range> to_ranges(std::vector<Interval> const& intervals) {
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


std::vector<std::bitset<64>> to_bitset(Bitmap const& bitmap) {
    std::vector<std::bitset<64>> mask;
    uint64_t b = 0;
    for (size_t i = 0; i < bitmap.size() / 64; ++i) {
        for (size_t j = 0; j < 64; ++j) {
            b |= bitmap[i * 64 + j];
        }
        mask.push_back(std::bitset<64>(b));
    }
    b = 0;
    for (size_t j = 0; j < bitmap.size() % 64; ++j) {
        b |= bitmap[bitmap.size() / 64 + j] << j;
    }
    mask.push_back(std::bitset<64>(b));
    return mask;
}


using namespace eckit;
using namespace metkit::grib;

extern "C" {
    unsigned long grib_decode_unsigned_long(const unsigned char* p, long* offset, int bits);
    double grib_power(long s, long n);
}

namespace gribjump {

static GribAccessor<long>          editionNumber("editionNumber");
static GribAccessor<long>          bitmapPresent("bitmapPresent");
static GribAccessor<long>          binaryScaleFactor("binaryScaleFactor");
static GribAccessor<long>          decimalScaleFactor("decimalScaleFactor");
static GribAccessor<unsigned long> bitsPerValue("bitsPerValue");
static GribAccessor<unsigned long> ccsdsFlags("ccsdsFlags");
static GribAccessor<unsigned long> ccsdsBlockSize("ccsdsBlockSize");
static GribAccessor<unsigned long> ccsdsRsi("ccsdsRsi");
static GribAccessor<double>        referenceValue("referenceValue");
static GribAccessor<unsigned long> offsetBeforeData("offsetBeforeData");
static GribAccessor<unsigned long> offsetAfterData("offsetAfterData");
static GribAccessor<unsigned long> offsetBeforeBitmap("offsetBeforeBitmap");
static GribAccessor<unsigned long> numberOfValues("numberOfValues");
static GribAccessor<unsigned long> numberOfDataPoints("numberOfDataPoints");
static GribAccessor<long>          sphericalHarmonics("sphericalHarmonics");
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

// clang, gcc 10+
#if defined(__has_builtin)
    #if __has_builtin(__builtin_popcountll)
        #define POPCOUNT_AVAILABLE 1
    #else
        #define POPCOUNT_AVAILABLE 0
    #endif
// gcc 3.4+
#elif defined(__GNUC__) && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4))
    #define POPCOUNT_AVAILABLE 1
#else
    #define POPCOUNT_AVAILABLE 0
#endif


//static inline int count_bits(unsigned long long n) {
//    if (POPCOUNT_AVAILABLE) {
//        return __builtin_popcountll(n);
//    }
//    // TODO(Chris): see also _mm_popcnt_u64 in <immintrin.h>, but not suitable for ARM
//    // fallback: lookup table
//    return bits[n         & 0xffffu]
//           +  bits[(n >> 16) & 0xffffu]
//           +  bits[(n >> 32) & 0xffffu]
//           +  bits[(n >> 48) & 0xffffu];
//}


//static inline uint64_t reverse_bytes(uint64_t n) {
//    return ((n & 0x00000000000000FF) << 56) |
//           ((n & 0x000000000000FF00) << 40) |
//           ((n & 0x0000000000FF0000) << 24) |
//           ((n & 0x00000000FF000000) << 8) |
//           ((n & 0x000000FF00000000) >> 8) |
//           ((n & 0x0000FF0000000000) >> 24) |
//           ((n & 0x00FF000000000000) >> 40) |
//           ((n & 0xFF00000000000000) >> 56);
//}


JumpInfo::JumpInfo():numberOfValues_(0) {}


JumpInfo::JumpInfo(const GribHandle& h):numberOfValues_(0) {
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

}

void JumpInfo::encode(eckit::Stream& s) const {
    s << currentVersion_;
    s << editionNumber_;
    s << binaryScaleFactor_;
    s << decimalScaleFactor_;
    s << bitsPerValue_;
    s << referenceValue_;
    s << offsetBeforeData_;
    s << numberOfDataPoints_;
    s << numberOfValues_;
    s << offsetBeforeBitmap_;
    s << sphericalHarmonics_;
    s << binaryMultiplier_;
    s << decimalMultiplier_;
    s << totalLength_;
    s << msgStartOffset_;
    s << md5GridSection_ ;
    s << packingType_;
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

void JumpInfo::toFile(eckit::PathName pathname, bool append){
    // TODO: Replace this to use encode.

    std::unique_ptr<DataHandle> dh(pathname.fileHandle());
    version_ = currentVersion_;

    append ? dh->openForAppend(0) : dh->openForWrite(0);

    dh->write(&version_, sizeof(version_));
    dh->write(&editionNumber_, sizeof(editionNumber_));
    dh->write(&binaryScaleFactor_, sizeof(binaryScaleFactor_));
    dh->write(&decimalScaleFactor_, sizeof(decimalScaleFactor_));
    dh->write(&bitsPerValue_, sizeof(bitsPerValue_));
    dh->write(&ccsdsFlags_, sizeof(ccsdsFlags_));
    dh->write(&ccsdsBlockSize_, sizeof(ccsdsBlockSize_));
    dh->write(&ccsdsRsi_, sizeof(ccsdsRsi_));
    dh->write(&referenceValue_, sizeof(referenceValue_));
    dh->write(&offsetBeforeData_, sizeof(offsetBeforeData_));
    dh->write(&offsetAfterData_, sizeof(offsetAfterData_));
    dh->write(&numberOfDataPoints_, sizeof(numberOfDataPoints_));
    dh->write(&numberOfValues_, sizeof(numberOfValues_));
    dh->write(&offsetBeforeBitmap_, sizeof(offsetBeforeBitmap_));
    dh->write(&sphericalHarmonics_, sizeof(sphericalHarmonics_));
    dh->write(&binaryMultiplier_, sizeof(binaryMultiplier_));
    dh->write(&decimalMultiplier_, sizeof(decimalMultiplier_));
    dh->write(&totalLength_, sizeof(totalLength_));
    dh->write(&msgStartOffset_, sizeof(msgStartOffset_));
    dh->write(md5GridSection_.data(), md5GridSection_.size());
    dh->write(packingType_.data(), packingType_.size());
    dh->close();
}


void JumpInfo::fromFile(eckit::PathName pathname, uint16_t msg_id){
    // TODO: Replace this to use the Stream constructor.

    std::unique_ptr<DataHandle> dh(pathname.fileHandle());

    dh->openForRead();
    dh->seek(msg_id*metadataSize);
    // make sure we aren't reading past the end of the file
    ASSERT(dh->position() + eckit::Offset(metadataSize) <= dh->size());
    dh->read(&version_, sizeof(version_));
    if (version_ != currentVersion_) {
        std::stringstream ss;
        ss << "Bad JumpInfo version found in " << pathname;
        dh->close();
        throw JumpException(ss.str(), Here());
    }
    dh->read(&editionNumber_, sizeof(editionNumber_));
    dh->read(&binaryScaleFactor_, sizeof(binaryScaleFactor_));
    dh->read(&decimalScaleFactor_, sizeof(decimalScaleFactor_));
    dh->read(&bitsPerValue_, sizeof(bitsPerValue_));
    dh->read(&ccsdsFlags_, sizeof(ccsdsFlags_));
    dh->read(&ccsdsBlockSize_, sizeof(ccsdsBlockSize_));
    dh->read(&ccsdsRsi_, sizeof(ccsdsRsi_));
    dh->read(&referenceValue_, sizeof(referenceValue_));
    dh->read(&offsetBeforeData_, sizeof(offsetBeforeData_));
    dh->read(&offsetAfterData_, sizeof(offsetAfterData_));
    dh->read(&numberOfDataPoints_, sizeof(numberOfDataPoints_));
    dh->read(&numberOfValues_, sizeof(numberOfValues_));
    dh->read(&offsetBeforeBitmap_, sizeof(offsetBeforeBitmap_));
    dh->read(&sphericalHarmonics_, sizeof(sphericalHarmonics_));
    dh->read(&binaryMultiplier_, sizeof(binaryMultiplier_));
    dh->read(&decimalMultiplier_, sizeof(decimalMultiplier_));
    dh->read(&totalLength_, sizeof(totalLength_));
    dh->read(&msgStartOffset_, sizeof(msgStartOffset_));
    dh->read(md5GridSection_.data(), md5GridSection_.size());
    dh->read(packingType_.data(), packingType_.size());
    dh->close();

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


std::vector<Values> JumpInfo::get_ccsds_values(const JumpHandle& f, const std::vector<Interval> &intervals) const {
    auto ranges = to_ranges(intervals);

    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, mc::Range{offsetBeforeData_, offsetAfterData_ - offsetBeforeData_ + 1});

    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);

    // TODO(maee): (IMPORTANT!!!) get offsets from metadata

    // WORKAROUND: no offsets found, try to get them from the data
    // TODO(maee): remove this workaround
    auto offsets = ccsds.offsets();
    if (!offsets) {
        size_t eof = data_accessor->eof();
        auto encoded = data_accessor->read({0, eof});

        if (encoded.size() == 0) {
            std::cerr << "encoded.size() == 0" << std::endl;
            throw std::runtime_error("encoded.size() == 0");
        }

        ccsds.decode(encoded);
        offsets = ccsds.offsets();

        if (!offsets) {
            std::cerr << "offsets = None" << std::endl;
            throw std::runtime_error("offsets is None");
        }
    } // WORKAROUND

    auto doubles_list = ccsds.decode(data_accessor, ranges);

    // TODO(maee): Optimize: Copying Decompressor::Values to GribJump::Values can be avoided
    std::vector<Values> result;
    for (auto &doubles : doubles_list) {
        Values values((double*) doubles.data(), (double*) doubles.data() + doubles.size());
        result.push_back(values);
    }

    return result;
}


std::vector<Values> JumpInfo::get_simple_values(const JumpHandle& f, const std::vector<Interval> &intervals) const {
    auto ranges = to_ranges(intervals);

    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, mc::Range{offsetBeforeData_, offsetAfterData_ - offsetBeforeData_ + 1});

    mc::SimpleDecompressor<double> simple{};
    simple
        .bits_per_value(bitsPerValue_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);

    auto doubles_list = simple.decode(data_accessor, ranges);

    // TODO(maee): Optimize: Copying Decompressor::Values to GribJump::Values can be avoided
    std::vector<Values> result;
    for (auto &doubles : doubles_list) {
        Values values((double*) doubles.data(), (double*) doubles.data() + doubles.size());
        result.push_back(values);
    }

    return result;
}


// Read the entire bitmap from a GRIB file
// TODO(maee): optimization: read only the bitmap for the requested interval
Bitmap JumpInfo::get_bitmap(const JumpHandle& f) const {
    eckit::Offset offset = msgStartOffset_ + offsetBeforeBitmap_;
    auto bitmapSize = (numberOfDataPoints_ + 7) / 8;

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


// Recalculate intervals using the bitmap
// This is necessary because data section does not contain missing values
// Intervals need to be shifted by the number of missing values before the interval
// TODO(maee): optimization: read only the bitmap for the requested interval
std::pair<std::vector<Interval>, std::vector<Bitmap>> JumpInfo::calculate_intervals(const std::vector<Interval>& intervals_tmp, const Bitmap& bitmap) const {
    struct ExtendedInterval {
        enum class Type {GAP, INTERVAL};
        Type type; // GAP or INTERVAL
        size_t begin; // begin is inclusive
        size_t end; // end is exclusive
        size_t missing_cum; // number of missing values before the interval
        size_t missing; // number of missing values in the interval
        Bitmap bitmap; // bitmap for the interval

        ExtendedInterval(Type type, size_t begin, size_t end, const Bitmap& all_bitmap) :
            type(type), begin(begin), end(end), missing_cum{0}, bitmap{all_bitmap.begin() + begin, all_bitmap.begin() + end}
        {
            missing = std::count(bitmap.begin(), bitmap.end(), false);
        }
    };

    std::vector<ExtendedInterval> intervals;
    std::transform(intervals_tmp.begin(), intervals_tmp.end(), std::back_inserter(intervals), [&bitmap](const auto& interval) {
        return ExtendedInterval{ExtendedInterval::Type::INTERVAL, interval.first, interval.second, bitmap};
    });

    assert(intervals.size() > 0);
    std::vector<ExtendedInterval> gaps;
    gaps.push_back(ExtendedInterval{ExtendedInterval::Type::GAP, 0, intervals.front().begin, bitmap});
    std::transform(intervals.begin(), intervals.end() - 1, intervals.begin() + 1, std::back_inserter(gaps), [&bitmap](const auto& a, const auto& b) {
        return ExtendedInterval{ExtendedInterval::Type::GAP, a.end, b.begin, bitmap};
    });

    std::vector<ExtendedInterval> intervals_and_gaps;
    std::merge(intervals.begin(), intervals.end(), gaps.begin(), gaps.end(), std::back_inserter(intervals_and_gaps), [](const auto& a, const auto& b) {
        return a.begin < b.begin;
    });

    std::sort(intervals_and_gaps.begin(), intervals_and_gaps.end(), [](const auto& a, const auto& b) {
        return a.begin < b.begin;
    });

    for (size_t i = 1; i < intervals_and_gaps.size(); i++) {
        intervals_and_gaps[i].missing_cum = intervals_and_gaps[i-1].missing_cum + intervals_and_gaps[i-1].missing;
    }

    std::vector<Interval> new_intervals;
    std::vector<Bitmap> new_interval_bitmaps;
    for (const auto& i : intervals_and_gaps) {
        if (i.type == ExtendedInterval::Type::INTERVAL) {
            new_intervals.push_back({i.begin - i.missing_cum, i.end - i.missing_cum - i.missing});
            new_interval_bitmaps.push_back(i.bitmap);
        }
    }

    return std::make_pair(new_intervals, new_interval_bitmaps);
}


ExtractionResult JumpInfo::extractRanges(const JumpHandle& f, std::vector<Interval> intervals) const {
    ASSERT(check_intervals(intervals));
    ASSERT(!sphericalHarmonics_);
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
    }
    else { // bitmap
        auto bitmap = get_bitmap(f);
        auto [new_intervals, new_bitmaps] = calculate_intervals(intervals, bitmap);
        auto all_decoded_values = (this->*get_values)(f, new_intervals);

        for (size_t i = 0; i < intervals.size(); ++i) {
            Values values;
            for (size_t count = 0, j = 0; j < new_bitmaps[i].size(); ++j) {
                if (new_bitmaps[i][j])
                    values.push_back(all_decoded_values[i][count++]);
                else
                    values.push_back(MISSING_VALUE);
            }
            all_values.push_back(values);
            all_masks.push_back(to_bitset(new_bitmaps[i]));
        }
    }

    return ExtractionResult(all_values, all_masks);
}


// TODO(maee): For testing purposes (can be removed?)
double JumpInfo::extractValue(const JumpHandle& f, size_t index) const {
    throw std::runtime_error("not implemented");
}

// TODO(maee): For testing purposes (can be removed?)
double JumpInfo::readDataValue(const JumpHandle& f, size_t index) const {
    throw std::runtime_error("not implemented");
}

} // namespace gribjump

