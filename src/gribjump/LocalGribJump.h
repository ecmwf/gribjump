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

#include "gribjump/GribJumpBase.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribInfoCache.h"

namespace gribjump {
class LocalGribJump : public GribJumpBase {
public: 
    LocalGribJump();
    ~LocalGribJump();
    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>) override;
    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) override;
    
    ExtractionResult directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfo info) const;

    // JumpInfo extractInfo(eckit::DataHandle* handle) const;
    JumpInfo extractInfo(const fdb5::FieldLocation& loc);

    bool isCached(std::string) const;

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;


private:
    GribInfoCache cache_;
    // std::map<std::string, std::string> cachePaths_; // Maps fieldlocation to cache path
    // std::map<eckit::PathName, GribInfoCache> caches_;
    
    bool cacheEnabled_ = false;

};
} // namespace gribjump
