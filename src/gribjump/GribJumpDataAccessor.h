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

#include "eckit/io/DataHandle.h"
#include "gribjump/compression/DataAccessor.h"

namespace gribjump {

class GribJumpDataAccessor2 : public mc::DataAccessor { // TODO: Remove the old GribJumpDataAccessor. This one uses datahandle.

public:
    GribJumpDataAccessor2(eckit::DataHandle& dh, const mc::Range range) : dh_{dh}, data_section_range_{range} {}

    void write(const eckit::Buffer& buffer, const size_t offset) const override {
        throw std::runtime_error("Not implemented");
    }

    eckit::Buffer read(const mc::Range& range) const override {
        eckit::Offset offset = range.first;
        eckit::Length size = range.second;
        
        eckit::Buffer buf(size);
        const eckit::Offset data_section_offset = data_section_range_.first;
        const eckit::Length data_section_size = data_section_range_.second;
        if (offset + size > data_section_size)
            throw std::runtime_error("Read access outside data section: " + std::to_string(offset) + " + " + std::to_string(size) + " > " + std::to_string(data_section_size));
        if (dh_.seek(data_section_offset + offset) != (eckit::Offset) (data_section_offset + offset))
            throw std::runtime_error("Failed to seek to offset in grib file");
        if (dh_.read(reinterpret_cast<char*>(buf.data()), size) != size)
            throw std::runtime_error("Failed to read from grib file");
        return buf;
    }

    eckit::Buffer read() const override {
        return read({0, data_section_range_.second});
    }

    size_t eof() const override {
        return data_section_range_.second;
    }

private:
    eckit::DataHandle& dh_;
    mc::Range data_section_range_;
};

} // namespace gribjump