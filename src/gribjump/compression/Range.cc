#include "Range.h"
#include <algorithm>
#include <cassert>

std::pair<size_t, size_t> begin_end(const mc::Range& range)
{
    const auto [offset, size] = range;
    return {offset, offset + size};
}


// Returns the range that covers both r1 and r2
mc::Range operator+(const mc::Range& r1, const mc::Range& r2)
{
    auto [b1, e1] = begin_end(r1);
    auto [b2, e2] = begin_end(r2);
    assert(!((b1 > e2) && (b2 > e1)));
    return mc::Range{std::min(b1, b2), std::max(e1, e2) - std::min(b1, b2)};
}

// Shifts the range by n
// e.g. [3, 5] + 2 = [5, 7]
mc::Range operator>>(const mc::Range& r, const size_t n) {
    auto [offset, size] = r;
    return mc::Range{offset + n, size};
}

mc::Range operator<<(const mc::Range& r, const size_t n) {
    auto [offset, size] = r;
    assert(offset >= n);
    return mc::Range{offset - n, size};
}

// Scales the range by n
// e.g. [3, 5] * 2 = [6, 10]
mc::Range operator*(const mc::Range& r, const size_t n) {
    auto [offset, size] = r;
    return mc::Range{offset * n, size * n};
}

mc::Range operator/(const mc::Range& r, const size_t n) {
    auto [offset, size] = r;
    assert(size % n == 0);
    assert(offset % n == 0);
    return mc::Range{offset / n, size / n};
}


std::ostream& operator<<(std::ostream& os, const mc::Range& range)
{
    auto [rb, re] = begin_end(range);
    os << "[" << rb << ", " << re << "]";
    return os;
}


std::ostream& operator<<(std::ostream& os, const mc::RangeBucket& range)
{
    os << range.first <<  std::endl;
    for (const auto& r : range.second) {
        os << "\t" << r << std::endl;
    }
    os << std::endl;
    return os;
}


// Combines the range r with the existing buckets
// If the range r overlaps with an existing bucket, the bucket is extended
// e.g. [3, 2] + [4, 3] = [3, 4]
// If the range r is adjacent to an existing bucket, the bucket is extended
// e.g. [3, 2] + [5, 3] = [3, 5]
// If the range r is not adjacent to an existing bucket, a new bucket is created
// e.g. [3, 2] + [6, 3] = [3, 2], [6, 3]
// If the range r is not adjacent to an existing bucket, and overlaps with multiple buckets, the buckets are merged
// e.g. [3, 2] + [5, 4] + [8, 3] = [3, 11]
mc::RangeBuckets& operator<<(mc::RangeBuckets& buckets, const mc::Range& r)
{
    const mc::Range sub_range{r};
    const auto [srb_tmp, sre_tmp] = begin_end(sub_range);
    auto srb = srb_tmp; // not necessary in C++20
    auto sre = sre_tmp; // not necessary in C++20

    auto r1 = std::find_if(buckets.begin(), buckets.end(), [&](const auto bucket){
        const auto [bucket_range, _] = bucket;
        const auto [brb, bre] = begin_end(bucket_range);
        return brb <= srb && srb <= bre;
    });

    if (r1 != buckets.end()) {
        r1->first = sub_range + r1->first;
        r1->second.push_back(sub_range);
        if (std::next(r1) != buckets.end()) {
            auto r2 = std::next(r1);
            auto [r2_begin, r2_end] = begin_end(r2->first);

            if (r2_begin <= sre && sre <= r2_end) {
                r1->first = r1->first + r2->first;
                std::copy(r2->second.begin(), r2->second.end(), std::back_inserter(r1->second));
                buckets.erase(r2);
            }
        }
        return buckets;
    }
    auto r2 = std::find_if(buckets.begin(), buckets.end(), [&](auto l){
        auto [l_begin, l_end] = begin_end(l.first);
        return l_begin <= sre && sre <= l_end;
    });
    if (r2 != buckets.end()) {
        r2->first = sub_range + r1->first;
        return buckets;
    }

    buckets.push_back({sub_range, mc::SubRanges{sub_range}});

    return buckets;
}


std::size_t std::hash<mc::Range>::operator() (const mc::Range& range) const
{
    static_assert(sizeof(std::size_t) == sizeof(std::uint64_t), "std::size_t must be 64 bits");
    const auto [offset, size] = range;
    return offset ^ (size << 32);
}


