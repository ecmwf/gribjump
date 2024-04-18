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

#include <compressor/data_reader.h>

class GribJumpDataAccessor : public DataAccessor {
public:
    GribJumpDataAccessor(const JumpHandle* jh, Range range) : jh_{jh}, range_{range} {
    }

    ~GribJumpDataAccessor() {
    }

    void write(const eckit::Buffer& buffer, const size_t offset) const override {
        throw std::runtime_error("Not implemented");
    }

    eckit::Buffer read(const Range& range) const override {
        const auto [offset, size] = range;
        eckit::Buffer buf(size);
        const auto [data_start_offset_, size_] = range_;
        jh_->seek(data_start_offset_ + offset);
        jh_->read(reinterpret_cast<char*>(buf.data()), size);
        return buf;
    }

    size_t eof() const override {
        const auto [data_start_offset_, size_] = range_;
        return data_start_offset_ + size_;
    }


private:
    const JumpHandle* jh_;
    Range range_;
};
