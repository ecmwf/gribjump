/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include <memory>
#include <vector>
#include "gribjump/ExtractionData.h"

namespace gribjump {

class IResultSource {
public:

    virtual ~IResultSource()                         = default;
    virtual bool hasNext() const                     = 0;
    virtual std::unique_ptr<ExtractionResult> next() = 0;
};

class VectorSource : public IResultSource {
public:

    explicit VectorSource(std::vector<std::unique_ptr<ExtractionResult>>&& data) : data_(std::move(data)) {}

    bool hasNext() const override { return index_ < data_.size(); }

    std::unique_ptr<ExtractionResult> next() override {
        if (!hasNext()) {
            return nullptr;
        }
        return std::move(data_[index_++]);
    }

private:

    std::vector<std::unique_ptr<ExtractionResult>> data_;
    std::size_t index_ = 0;
};

// This class is a simple iterator over a source of ExtractionResults
// e.g. a vector or queue, which is owned by this class.
class ExtractionIterator {
public:

    explicit ExtractionIterator(std::unique_ptr<IResultSource> source) : source_(std::move(source)) {}

    bool hasNext() const { return source_->hasNext(); }

    // Caller takes ownership of the returned pointer
    std::unique_ptr<ExtractionResult> next() { return source_->next(); }

    // Convenience function
    std::vector<std::unique_ptr<ExtractionResult>> dumpVector() {
        std::vector<std::unique_ptr<ExtractionResult>> results;
        while (hasNext()) {
            results.push_back(next());
        }
        return results;
    }

private:

    // Polymorphic pointer to "something" that can yield ExtractionResults
    std::unique_ptr<IResultSource> source_;
};

}  // namespace gribjump