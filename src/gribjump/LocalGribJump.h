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
/// @author Tiago Quintino

#pragma once

#include "gribjump/GribJumpBase.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribInfoCache.h"

namespace gribjump {

class LocalGribJump : public GribJumpBase {

public:

    explicit LocalGribJump(const Config& config);
    ~LocalGribJump();

    /// @brief Scans the full grib file, looking for GRIB messages and populates cache
    /// @param path full path to grib file
    size_t scan(const eckit::PathName& path) override;

    size_t scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) override;

    std::vector<ExtractionResult*> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) override;

    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>) override;
    
    ExtractionResult directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfoHandle info) const;
    ExtractionResult* directJump(eckit::PathName path, const eckit::Offset offset, const std::vector<Range> ranges, JumpInfoHandle info) const;
    
    std::vector<ExtractionResult*> directJumpSharedHandle(const eckit::PathName path, const std::vector<eckit::Offset> offsets, const std::vector<std::vector<Range>> ranges, std::vector<JumpInfoHandle> infos) const;

    JumpInfoHandle extractInfo(const fdb5::FieldLocation& loc);
    JumpInfoHandle extractInfo(const eckit::PathName& path, const eckit::Offset& offset);
    
    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;
};

} // namespace gribjump
