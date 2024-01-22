/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include "eckit/filesystem/PathName.h"

#include "gribjump/GribInfo.h"

#include "compression/NumericCompressor.h"

namespace eckit {
class DataHandle;
}

namespace gribjump {

class JumpInfo;

class JumpHandle : public eckit::NonCopyable {
public:
    explicit JumpHandle(const eckit::PathName&);

    /// Takes ownership of a handle pointer
    explicit JumpHandle(eckit::DataHandle*);

    ~JumpHandle();

    std::vector<JumpInfo*> extractInfoFromFile();
    JumpInfo* extractInfo();

    eckit::Offset position();
    eckit::Length size();
    eckit::Offset seek(const eckit::Offset&) const;

private:

    mutable eckit::DataHandle *handle_;
    bool ownsHandle_;
    mutable bool opened_;
    eckit::PathName path_;

    virtual long read(void*, long) const;
    virtual void print(std::ostream& s) const;

    void open() const;
    void close() const;

    friend class JumpInfo;
    friend class GribJumpDataAccessor;
};

void write_jumpinfos_to_file(const std::vector<JumpInfo*> infos, const eckit::PathName& path);

//----------------------------------------------------------------------------------------------------------------------

class GribJumpDataAccessor : public mc::DataAccessor {
public:
    GribJumpDataAccessor(const JumpHandle* jh, mc::Range range) : jh_{jh}, data_section_range_{range} {}

    void write(const eckit::Buffer& buffer, const size_t offset) const override {
        throw std::runtime_error("Not implemented");
    }

    eckit::Buffer read(const mc::Range& range) const override {
        const auto [offset, size] = range;
        eckit::Buffer buf(size);
        const auto [data_section_offset, data_section_size] = data_section_range_;
        if (offset + size > data_section_size)
            throw std::runtime_error("Read access outside data section: " + std::to_string(offset) + " + " + std::to_string(size) + " > " + std::to_string(data_section_size));
        if (jh_->seek(data_section_offset + offset) != (eckit::Offset) (data_section_offset + offset))
            throw std::runtime_error("Failed to seek to offset in grib file");
        if (jh_->read(reinterpret_cast<char*>(buf.data()), size) != size)
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
    const JumpHandle* jh_;
    mc::Range data_section_range_;
};


} // namespace gribjump
