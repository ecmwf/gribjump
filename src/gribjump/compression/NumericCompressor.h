#pragma once

#include "Range.h"
#include "DataAccessor.h"

#include <memory>
#include <vector>
#include <eckit/io/Buffer.h>
#include <unordered_map>

namespace mc {

template <typename ValueType>
class NumericDecompressor;

template <typename ValueType>
class NumericCompressor {
public:
  using CompressedData = eckit::Buffer;
  using Values = std::vector<ValueType>;
  virtual std::pair<std::unique_ptr<NumericDecompressor<ValueType>>, CompressedData> encode(const Values&) = 0;
};

template <typename ValueType>
class NumericDecompressor {
public:
  using CompressedData = eckit::Buffer;
  using Values = std::vector<ValueType>;
  virtual Values decode(const CompressedData&) = 0;
  virtual Values decode(const std::shared_ptr<DataAccessor>, const Range&) = 0;


  virtual std::vector<Values> decode(const std::shared_ptr<DataAccessor>& accessor, const std::vector<mc::Range>& ranges) {
    using Values = typename NumericDecompressor<ValueType>::Values;

    std::vector<Values> result;
    std::unordered_map<Range, std::pair<Range, std::shared_ptr<Values>>> ranges_map;

    // find which sub_ranges are in which buckets
    RangeBuckets buckets;
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
      const auto& [bucket_range, decoded] = ranges_map[user_range];
      const auto& [bucket_range_offset, bucket_range_size] = bucket_range;
      const auto& [user_range_offset, user_range_size] = user_range;

      ValueType* data_start = reinterpret_cast<ValueType*>(decoded->data()) + (user_range_offset - bucket_range_offset);
      Values values(data_start, data_start + user_range_size);
      result.push_back(std::move(values));
    }

    return result;

  }
};

} // namespace mc