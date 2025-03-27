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


#include <cassert>
#include <fstream>
#include <iomanip>

#include "Range.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"

namespace gribjump::mc {

class DataAccessor {
public:

    virtual ~DataAccessor()                              = default;
    virtual eckit::Buffer read(const Block& range) const = 0;
    virtual eckit::Buffer read() const                   = 0;
    virtual size_t eof() const                           = 0;
};


class MemoryAccessor : public DataAccessor {
public:

    explicit MemoryAccessor(const eckit::Buffer& buffer) : buf_{buffer.data(), buffer.size()} {}

    eckit::Buffer read(const Block& range) const override {
        const auto [offset, size] = range;
        if (offset + size > buf_.size()) {
            std::stringstream ss;
            ss << "Out of range: offset: " << offset << ", size: " << size << ", buf size: " << buf_.size();
            throw eckit::OutOfRange(ss.str(), Here());
        }
        return eckit::Buffer{reinterpret_cast<const char*>(buf_.data()) + offset, size};
    }

    eckit::Buffer read() const override { return eckit::Buffer{buf_.data(), buf_.size()}; }

    size_t eof() const override { return buf_.size(); }

private:

    eckit::Buffer buf_;
};

}  // namespace gribjump::mc
