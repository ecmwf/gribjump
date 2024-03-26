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

#include "eckit/io/DataHandle.h"

#include "gribjump/jumper/Jumper.h"
#include "gribjump/ExtractionItem.h"

namespace gribjump {
// -----------------------------------------------------------------------------

// GRIB does not in general specify what do use in place of missing value.
constexpr double MISSING_VALUE = std::numeric_limits<double>::quiet_NaN();


//  ----------------------------------------------------------------------------
// to remove

std::vector<std::bitset<64>> toBitset(const Bitmap& bitmap) {
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

// Convert ranges to intervals
// TODO(maee): Simplification: Switch to intervals or ranges
std::vector<mc::Range> toRanges(const std::vector<Interval>& intervals){
    std::vector<mc::Range> ranges;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(ranges), [](auto interval){
        auto [begin, end] = interval;
        return mc::Range{begin, end - begin};
    });
    return ranges;
}


bool checkIntervals(const std::vector<Interval>& intervals) {
    ASSERT(intervals.size() > 0);
    std::vector<char> check;
    std::transform(intervals.begin(), intervals.end() - 1, intervals.begin() + 1, std::back_inserter(check), [](const auto& a, const auto& b) {
        return a.second <= b.first;
    });
    return std::all_of(check.begin(), check.end(), [](char c) { return c; });
}
// -----------------------------------------------------------------------------


Jumper::Jumper() {}

Jumper::~Jumper() {}


void Jumper::extract(eckit::DataHandle& dh, const JumpInfo& info, ExtractionItem& extractionItem) {
    ASSERT(checkIntervals(extractionItem.intervals()));
    ASSERT(!info.sphericalHarmonics());

    /// @todo... actually put the contents into the extractionItem
    /// @todo... remove the other extract method

    // if (info.bitsPerValue() == 0) return extractConstant(info, intervals);

    // if (!info.offsetBeforeBitmap()) return extractNoMask(dh, info, intervals);

    // return extractMasked(dh, info, intervals);


    auto intervals = extractionItem.intervals();
    auto result = extract(dh, info, intervals);

    /// @XXX: copy into extractionItem

    const std::vector<std::vector<double>>& values = result->values();
    const std::vector<std::vector<std::bitset<64>>>& mask = result->mask();

    extractionItem.values(values);
    extractionItem.mask(mask);

    return;
}


ExtractionResult* Jumper::extract(eckit::DataHandle& dh, const JumpInfo& info, const std::vector<Interval>& intervals) {
    ASSERT(checkIntervals(intervals));
    ASSERT(!info.sphericalHarmonics());

    if (info.bitsPerValue() == 0) return extractConstant(info, intervals);

    if (!info.offsetBeforeBitmap()) return extractNoMask(dh, info, intervals);

    return extractMasked(dh, info, intervals);
}


ExtractionResult* Jumper::extractNoMask(eckit::DataHandle& dh, const JumpInfo& info, const std::vector<Interval>& intervals) {

    std::vector<Values> all_values = readValues(dh, info, intervals);

    std::vector<std::vector<std::bitset<64>>> all_masks; // all present

    std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_masks), [](const auto& interval) {
        return toBitset(std::vector(interval.second - interval.first, true));
    });

    return new ExtractionResult(all_values, all_masks);
    
}

ExtractionResult* Jumper::extractMasked(eckit::DataHandle& dh, const JumpInfo& info, const std::vector<Interval>& intervals) {
    auto fullbitmap = readBitmap(dh, info);
    auto [new_intervals, new_bitmaps] = calculateMaskedIntervals(intervals, fullbitmap);
    auto all_decoded_values = readValues(dh, info, new_intervals);

    // XXX: Eventually, the ExtractionItem object will store values and masks, and pre-reserve space based on the ranges
    std::vector<Values> all_values;
    std::vector<std::vector<std::bitset<64>>> all_masks;


    for (size_t i = 0; i < intervals.size(); ++i) {
        Values values;
        // reserve
        values.reserve(new_bitmaps[i].size());
        for (size_t count = 0, j = 0; j < new_bitmaps[i].size(); ++j) {
            values.push_back(new_bitmaps[i][j] ? all_decoded_values[i][count++] : MISSING_VALUE);
        }
        all_values.push_back(values);
        all_masks.push_back(toBitset(new_bitmaps[i]));
    }

    return new ExtractionResult(all_values, all_masks);
}

// Constant fields
ExtractionResult* Jumper::extractConstant(const JumpInfo& info, const std::vector<Interval>& intervals) {

    // ASSERT(!info.offsetBeforeBitmap()); // todo: handle constant fields with bitmaps <- It looks like eccodes ignores the bitmap?

    std::vector<std::vector<std::bitset<64>>> all_masks;
    std::vector<Values> all_values;

    auto referenceValue = info.referenceValue();
    
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_values), [referenceValue] (const Interval& interval) {
        return Values(interval.second - interval.first, referenceValue);
    });
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(all_masks), [](const Interval& interval) {
        return toBitset(std::vector(interval.second - interval.first, true));
    });

    return new ExtractionResult(all_values, all_masks);
}


// Read the entire bitmap from a GRIB file
// TODO(maee): optimization: read only the bitmap for the requested interval
Bitmap Jumper::readBitmap(eckit::DataHandle& dh, const JumpInfo& info) const {


    eckit::Offset offset = info.msgStartOffset() + info.offsetBeforeBitmap();
    auto bitmapSize = (info.numberOfDataPoints() + 7) / 8;

    if (bitmapSize == 0)
        return Bitmap{};

    if (info.bitsPerValue() == 0)
        return Bitmap(info.numberOfDataPoints(), true);

    eckit::Buffer binaryBitmap(info.numberOfDataPoints());

    if (dh.seek(offset) != offset)
        throw std::runtime_error("Bitmap seek failed");

    if (dh.read(binaryBitmap, bitmapSize) != bitmapSize)
        throw std::runtime_error("Bitmap read failed");

    Bitmap bitmap;
    for (size_t i = 0; i < info.numberOfDataPoints(); i++) {
        bitmap.push_back(binaryBitmap[i / 8] & (1 << (7 - i % 8)));
    }

    return bitmap;
}


// Recalculate intervals using the bitmap
// This is necessary because data section does not contain missing values
// Intervals need to be shifted by the number of missing values before the interval
// TODO(maee): optimization: read only the bitmap for the requested interval
std::pair<std::vector<Interval>, std::vector<Bitmap>> Jumper::calculateMaskedIntervals(const std::vector<Interval>& intervals_tmp, const Bitmap& bitmap) const {
    struct ExtendedInterval {
        enum class Type {GAP, INTERVAL};
        Type type;          // GAP or INTERVAL
        size_t begin;       // begin is inclusive
        size_t end;         // end is exclusive
        size_t missing_cum; // number of missing values before the interval
        size_t missing;     // number of missing values in the interval
        Bitmap bitmap;      // bitmap for the interval

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


} // namespace gribjump
