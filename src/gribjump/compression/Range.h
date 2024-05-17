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

#include <cstddef>
#include <stdexcept>
#include <list>
#include <vector>
#include <iostream>
#include <tuple>
#include <cstdint>

namespace mc {
using Range = std::pair<size_t, size_t>;
}

template<>
struct std::hash<mc::Range> {
    std::size_t operator()(const mc::Range& range) const;
};


namespace mc {

// A bucket is a continous range of data that can be decoded in one go
std::pair<size_t, size_t> get_begin_end(const Range& range);
using SubRange = Range;
using SubRanges = std::vector<SubRange>;
using RangeBucket = std::pair<Range, SubRanges>;
using RangeBuckets = std::list<RangeBucket>;

} // namespace mc

std::ostream& operator<<(std::ostream& os, const mc::Range& range);
std::ostream& operator<<(std::ostream& os, const mc::RangeBucket& bucket);
mc::RangeBuckets& operator<<(mc::RangeBuckets& buckets, const mc::Range& r);
mc::Range operator+(const mc::Range& r1, const mc::Range& r2);

std::pair<size_t, size_t> begin_end(const mc::Range& range);
