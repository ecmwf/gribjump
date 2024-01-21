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

    size_t scan(const metkit::mars::MarsRequest) override;
    size_t scan(std::vector<ExtractionRequest>) override;

    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>) override;
    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) override;
    
    std::vector<std::vector<ExtractionResult>> extractMultithread(std::vector<ExtractionRequest> polyRequest);
    
    ExtractionResult directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfoHandle info) const;

    JumpInfoHandle extractInfo(const fdb5::FieldLocation& loc);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;

};

} // namespace gribjump
