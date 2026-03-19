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

#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "gribjump/compression/DataAccessor.h"

namespace gribjump {

class GribJumpDataAccessor : public mc::DataAccessor {

public:

    GribJumpDataAccessor(eckit::DataHandle& dh, const mc::Block range) : dh_{dh}, data_section_range_{range} {}

    eckit::Buffer read(const mc::Block& range) const override {
        eckit::Offset offset = range.first;
        eckit::Length size   = range.second;

        eckit::Buffer buf(size);
        const eckit::Offset data_section_offset = data_section_range_.first;
        const eckit::Length data_section_size   = data_section_range_.second;
        if (offset + size > data_section_size)
            throw eckit::OutOfRange("Read access outside data section", Here());
        if (dh_.seek(data_section_offset + offset) != (eckit::Offset)(data_section_offset + offset))
            throw eckit::Exception("Failed to seek to offset in datahandle", Here());
        if (dh_.read(reinterpret_cast<char*>(buf.data()), size) != size)
            throw eckit::Exception("Failed to read from datahandle", Here());
        return buf;
    }

    eckit::Buffer read() const override { return read({0, data_section_range_.second}); }

    size_t eof() const override { return data_section_range_.second; }

private:

    eckit::DataHandle& dh_;
    mc::Block data_section_range_;
};

}  // namespace gribjump