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
#include <cstdint>
#include <iostream>
#include <list>
#include <stdexcept>
#include <tuple>
#include <vector>
namespace gribjump::mc {
using Block = std::pair<size_t, size_t>;
}

template <>
struct std::hash<gribjump::mc::Block> {
    std::size_t operator()(const gribjump::mc::Block& range) const;
};


namespace gribjump::mc {

// A bucket is a continous range of data that can be decoded in one go
std::pair<size_t, size_t> get_begin_end(const Block& range);
using SubBlock     = Block;
using SubBlocks    = std::vector<SubBlock>;
using BlockBucket  = std::pair<Block, SubBlocks>;
using BlockBuckets = std::vector<BlockBucket>;  // Sorted to allow binary search (std::lower_bound)

std::ostream& operator<<(std::ostream& os, const Block& range);
std::ostream& operator<<(std::ostream& os, const BlockBucket& bucket);
BlockBuckets& operator<<(BlockBuckets& buckets, const Block& r);
Block operator+(const Block& r1, const Block& r2);

std::pair<size_t, size_t> begin_end(const Block& range);

}  // namespace gribjump::mc
