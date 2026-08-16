/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace gribjump {

// Polymorphic source of T's (e.g. a vector or a queue), owned by the iterator.
template <typename T>
class ResultSource {
public:

    virtual ~ResultSource()           = default;
    virtual bool hasNext() const      = 0;
    virtual std::unique_ptr<T> next() = 0;
};

// A ResultSource backed by an owned vector.
template <typename T>
class VectorResultSource : public ResultSource<T> {
public:

    explicit VectorResultSource(std::vector<std::unique_ptr<T>>&& data) : data_(std::move(data)) {}

    bool hasNext() const override { return index_ < data_.size(); }

    std::unique_ptr<T> next() override {
        if (!hasNext()) {
            return nullptr;
        }
        return std::move(data_[index_++]);
    }

private:

    std::vector<std::unique_ptr<T>> data_;
    std::size_t index_ = 0;
};

// A simple iterator over a source of T's, which is owned by this class.
template <typename T>
class ResultIterator {
public:

    explicit ResultIterator(std::unique_ptr<ResultSource<T>> source) : source_(std::move(source)) {}

    bool hasNext() const { return source_->hasNext(); }

    // Caller takes ownership of the returned pointer.
    std::unique_ptr<T> next() { return source_->next(); }

    // Convenience function
    std::vector<std::unique_ptr<T>> dumpVector() {
        std::vector<std::unique_ptr<T>> results;
        while (hasNext()) {
            results.push_back(next());
        }
        return results;
    }

private:

    // Polymorphic pointer to "something" that can yield T's
    std::unique_ptr<ResultSource<T>> source_;
};

}  // namespace gribjump
