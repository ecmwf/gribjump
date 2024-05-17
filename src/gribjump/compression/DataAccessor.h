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


#include <fstream>
#include <iomanip>
#include <cassert>

#include "Range.h"

#include "eckit/io/Buffer.h"

namespace mc {

class DataAccessor {
public:
    virtual ~DataAccessor() = default;
    virtual void write(const eckit::Buffer& buffer, const size_t offset) const = 0;
    virtual eckit::Buffer read(const Range& range) const = 0;
    virtual eckit::Buffer read() const = 0;
    virtual size_t eof() const = 0;
};


class PosixAccessor : public DataAccessor {
public:
    explicit PosixAccessor(const std::string& path) : ifs_{path, std::ifstream::binary} {}
    ~PosixAccessor() {
        ifs_.close();
    }

    void write(const eckit::Buffer& buffer, const size_t offset) const override {
        throw std::runtime_error("Not implemented");
    }

    eckit::Buffer read(const Range& range) const override {
        const auto [offset, size] = range;
        eckit::Buffer buf(size);
        ifs_.seekg(offset, std::ios::beg);
        ifs_.read(reinterpret_cast<char*>(buf.data()), size);
        if (!ifs_) {
            std::cerr << "Error: only " << ifs_.gcount() << " could be read" << std::endl;
            throw std::runtime_error("Error reading file");
        }
        assert(size != 0);
        return eckit::Buffer{buf.data(), size};
    }

    eckit::Buffer read() const override {
        ifs_.seekg(0, std::ios::end);
        size_t size = ifs_.tellg();
        ifs_.seekg(0, std::ios::beg);
        eckit::Buffer buf(size);
        ifs_.read(reinterpret_cast<char*>(buf.data()), size);
        if (!ifs_) {
            std::cerr << "Error: only " << ifs_.gcount() << " could be read" << std::endl;
            throw std::runtime_error("Error reading file");
        }
        assert(size != 0);
        return eckit::Buffer{buf.data(), size};
    }

    size_t eof() const override {
        ifs_.seekg(0, std::ios::end);
        return ifs_.tellg();
    }
private:
    mutable std::ifstream ifs_;
};


class MemoryAccessor : public DataAccessor {
public:
    explicit MemoryAccessor(const eckit::Buffer& buffer) : buf_{buffer.data(), buffer.size()} {}

    void write(const eckit::Buffer& buffer, const size_t offset) const override {
        throw std::runtime_error("Not implemented");
    }

    eckit::Buffer read(const Range& range) const override {
        const auto [offset, size] = range;
        if (offset + size > buf_.size()) {
            std::cout << "offset: " << offset << ", size: " << size << ", buf size: " << buf_.size() << std::endl;
            throw std::runtime_error("Out of range");
        }
        return eckit::Buffer{reinterpret_cast<const char*>(buf_.data()) + offset, size};
    }

    eckit::Buffer read() const override {
        return eckit::Buffer{buf_.data(), buf_.size()};
    }

    size_t eof() const override {
        return buf_.size();
    }
private:
    eckit::Buffer buf_;
};

} // namespace mc
