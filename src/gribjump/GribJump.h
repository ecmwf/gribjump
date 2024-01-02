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

#include <unordered_set>
#include <memory>

#include "eckit/io/DataHandle.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJumpBase.h"
#include "metkit/mars/MarsRequest.h"

namespace gribjump {

using Interval = std::tuple<size_t, size_t>;

// Gribjump API

class GribJump {
public:
    GribJump();
    ~GribJump() {}

    std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest> requests);

    std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Interval> ranges);

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request);

private:
    std::unique_ptr<GribJumpBase> internal_;

};
} // namespace gribjump
