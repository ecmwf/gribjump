/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#pragma once

#include "gribjump/GribInfo.h"
#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"
#include "gribjump/ExtractionData.h"

namespace gribjump {

using Range = std::tuple<size_t, size_t>;

// Gribjump API
class GribJump : public eckit::NonCopyable {
public: 
    GribJump();
    ~GribJump();
    std::vector<std::vector<ExtractionResult>> extractRemote(std::vector<ExtractionRequest>);
    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>);
    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges);
    
    ExtractionResult directJump(eckit::DataHandle* handle, std::vector<Range> allRanges, JumpInfo info) const;

    JumpInfo extractInfo(eckit::DataHandle* handle) const;

    bool isCached(std::string) const {return false;} // todo implement caching

private:
    // std::map<Key, std::tuple<FieldLocation*, JumpInfo> > cache_; // not imp
};

} // namespace GribJump