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

namespace gribjump {
class LocalGribJump : public GribJumpBase {
public:
    LocalGribJump();
    ~LocalGribJump();
    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>) override;
    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) override;

    ExtractionResult directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfo info) const;

    JumpInfo extractInfo(eckit::DataHandle* handle) const;

    bool isCached(std::string) const override {return false;} // not imp

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;


private:
    // std::map<Key, std::tuple<FieldLocation*, JumpInfo> > cache_; // not imp
};
} // namespace gribjump
