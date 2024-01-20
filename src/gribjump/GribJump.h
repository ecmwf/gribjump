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

#include <cstddef>
#include <string>
#include <vector>
#include <memory>
#include <unordered_set>

#include "eckit/io/DataHandle.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/Config.h"
#include "gribjump/GribJumpBase.h"

namespace gribjump {

class GribJumpBase;

typedef std::pair<size_t, size_t> Range;

/// GribJump high-level API

class GribJump {
public:
    
    GribJump();
    
    ~GribJump();

    size_t scan(const metkit::mars::MarsRequest request);
    size_t scan(std::vector<ExtractionRequest> requests);

    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest> requests);

    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request);

    void stats();

private:
    std::unique_ptr<GribJumpBase> impl_;
    Config config_;

};

} // namespace gribjump
