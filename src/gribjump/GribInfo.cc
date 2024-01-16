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
#include "eckit/serialisation/FileStream.h"
#include "eckit/utils/MD5.h"
#include "metkit/codes/GribAccessor.h"
#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJumpException.h"

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

JumpInfo::JumpInfo():version_(currentVersion_), numberOfValues_(0)
 {}


JumpInfo::JumpInfo(const GribHandle& h):version_(currentVersion_), numberOfValues_(0) {
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
            return to_bitset(std::vector(interval.second - interval.first, true));
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
            return to_bitset(std::vector(interval.second - interval.first, true));
        });
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


} // namespace gribjump

