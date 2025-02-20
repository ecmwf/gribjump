/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "Range.h"
#include <algorithm>
#include <cassert>

namespace gribjump::mc{

std::pair<size_t, size_t> begin_end(const Block& range)
{
    const auto [offset, size] = range;
    return {offset, offset + size};
}


Block operator+(const Block& r1, const Block& r2)
{
    auto [b1, e1] = begin_end(r1);
    auto [b2, e2] = begin_end(r2);
    assert(!((b1 > e2) && (b2 > e1)));
    return Block{std::min(b1, b2), std::max(e1, e2) - std::min(b1, b2)};
}


std::ostream& operator<<(std::ostream& os, const Block& range)
{
    auto [rb, re] = begin_end(range);
    os << "[" << rb << ", " << re << "]";
    return os;
}


std::ostream& operator<<(std::ostream& os, const BlockBucket& range)
{
    os << range.first <<  std::endl;
    for (const auto& r : range.second) {
        os << "\t" << r << std::endl;
    }
    os << std::endl;
    return os;
}


BlockBuckets& operator<<(BlockBuckets& buckets, const Block& r) {

    const Block sub_range{r};
    const auto [sub_start, sub_end] = begin_end(sub_range);

    // Find the position where the range might be inserted
    auto it = std::lower_bound(buckets.begin(), buckets.end(), sub_range,
        [](const BlockBucket& bucket, const Block& range) {
            const auto [bucket_start, bucket_end] = begin_end(bucket.first);
            return bucket_end < range.first;
        });

    Block merged_range = sub_range;
    SubBlocks merged_subranges = {sub_range};

    // Merge with any overlapping buckets before the insertion point
    while (it != buckets.begin()) {
        auto prev_it = std::prev(it);
        const auto [prev_start, prev_end] = begin_end(prev_it->first);

        if (prev_end < sub_start) break; // No overlap

        // Expand the merged range
        merged_range.first = std::min(merged_range.first, prev_start);
        merged_range.second = (std::max(prev_end, sub_end) - merged_range.first);

        merged_subranges.insert(merged_subranges.end(), prev_it->second.begin(), prev_it->second.end());

        it = buckets.erase(prev_it);
    }

    // Merge with any overlapping buckets after the insertion point
    while (it != buckets.end()) {
        const auto [next_start, next_end] = begin_end(it->first);

        if (next_start > sub_end) break; // No overlap

        // Expand the merged range
        merged_range.first = std::min(merged_range.first, next_start);
        merged_range.second = (std::max(next_end, sub_end) - merged_range.first);

        merged_subranges.insert(merged_subranges.end(), it->second.begin(), it->second.end());

        // Erase the current bucket and move the iterator forward
        it = buckets.erase(it);
    }

    buckets.insert(it, {merged_range, merged_subranges});
    return buckets;
}

} // namespace gribjump::mc

std::size_t std::hash<gribjump::mc::Block>::operator() (const gribjump::mc::Block& range) const
{
    static_assert(sizeof(std::size_t) == sizeof(std::uint64_t), "std::size_t must be 64 bits");
    const auto [offset, size] = range;
    return offset ^ (size << 32);
}

