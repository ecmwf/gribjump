/*
 * (C) Copyright 2026- ECMWF.
 * This software is licensed under the terms of the Apache Licence Version 2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "gribjump/api/ListResult.h"

namespace gribjump {

/// Owning, single-pass iterator. Listing currently buffers all fields before
/// returning; the iterator and yielded results can outlive the GribJump client.
class ListIterator {
public:

    explicit ListIterator(std::vector<ListResult> results) : results_(std::move(results)) {}

    ListIterator(const ListIterator&)            = delete;
    ListIterator& operator=(const ListIterator&) = delete;
    ListIterator(ListIterator&&)                 = default;
    ListIterator& operator=(ListIterator&&)      = default;

    bool hasNext() const { return index_ < results_.size(); }

    /// Transfers ownership of the next field; nullptr means exhausted.
    std::unique_ptr<ListResult> next() {
        if (!hasNext()) {
            return nullptr;
        }
        return std::make_unique<ListResult>(std::move(results_[index_++]));
    }

private:

    std::vector<ListResult> results_;
    size_t index_ = 0;
};

}  // namespace gribjump
