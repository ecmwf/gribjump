/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "DataAccessor.h"
#include "Range.h"

#include <eckit/io/Buffer.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace gribjump::mc {

template <typename ValueType>
class NumericDecompressor;

template <typename ValueType>
class NumericCompressor {
public:

    using CompressedData = eckit::Buffer;
    using Values         = std::vector<ValueType>;
    virtual std::pair<std::unique_ptr<NumericDecompressor<ValueType>>, CompressedData> encode(const Values&) = 0;
};

template <typename ValueType>
class NumericDecompressor {
public:

    using CompressedData = eckit::Buffer;
    using Values         = std::vector<ValueType>;
    using Offsets        = std::vector<size_t>;

    virtual Values decode(const CompressedData&)                             = 0;
    virtual Values decode(const std::shared_ptr<DataAccessor>, const Block&) = 0;
    virtual Offsets decode_offsets(const CompressedData&) { NOTIMP; }

    virtual std::vector<Values> decode(const std::shared_ptr<DataAccessor>& accessor,
                                       const std::vector<mc::Block>& ranges) {
        using Values = typename NumericDecompressor<ValueType>::Values;
        std::vector<Values> result;
        decode(accessor, ranges, result);
        return result;
    }

    virtual void decode(const std::shared_ptr<DataAccessor>& accessor, const std::vector<mc::Block>& ranges,
                        std::vector<Values>& result) {
        using Values = typename NumericDecompressor<ValueType>::Values;

        std::unordered_map<Block, std::pair<Block, std::shared_ptr<Values>>> ranges_map;

        // find which sub_ranges are in which buckets
        BlockBuckets buckets;
        for (const auto& range : ranges) {
            buckets << range;
        }

        // inverse buckets and associate data with ranges
        for (const auto& [bucket_range, bucket_sub_ranges] : buckets) {
            auto decoded = std::make_shared<Values>(decode(accessor, bucket_range));
            for (const auto& bucket_sub_range : bucket_sub_ranges) {
                ranges_map[bucket_sub_range] = std::make_pair(bucket_range, decoded);
            }
        }

        // assign data to ranges
        for (const auto& user_range : ranges) {
            const auto& [bucket_range, decoded]                  = ranges_map[user_range];
            const auto& [bucket_range_offset, bucket_range_size] = bucket_range;
            const auto& [user_range_offset, user_range_size]     = user_range;

            ValueType* data_start =
                reinterpret_cast<ValueType*>(decoded->data()) + (user_range_offset - bucket_range_offset);
            Values values(data_start, data_start + user_range_size);
            result.push_back(std::move(values));
        }
    }
};

/// @todo: this is copy pasted from eccodes - do we *really* need this?
/* Return n to the power of s */
template <typename T>
constexpr T codes_power(long s, long n) {
    T divisor = 1.0;
    if (s == 0)
        return 1.0;
    if (s == 1)
        return n;
    while (s < 0) {
        divisor /= n;
        s++;
    }
    while (s > 0) {
        divisor *= n;
        s--;
    }
    return divisor;
}

}  // namespace gribjump::mc
