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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/GribJumpBase.h"
#include "gribjump/api/ExtractionIterator.h"

namespace gribjump {

class GribJumpBase;

typedef std::pair<size_t, size_t> Range;

/// GribJump high-level API

class GribJump {
public:

    GribJump();

    ~GribJump();

    size_t scan(const std::vector<eckit::PathName>& paths, const LogContext& ctx = LogContext());
    size_t scan(std::vector<metkit::mars::MarsRequest> requests, bool byfiles = false,
                const LogContext& ctx = LogContext());


    // Extract from a vector of requests
    ExtractionIterator extract(std::vector<ExtractionRequest>& requests, const LogContext& ctx = LogContext());

    // Extract from all fields matching a mars request (which will be expanded into a vector of ExtractionRequests)
    ExtractionIterator extract(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges,
                               const std::string& gridHash, const LogContext& ctx = LogContext());

    // Extract from a specific file, with grib messages starting at the given offsets
    ExtractionIterator extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets,
                               const std::vector<std::vector<Range>>& ranges, const LogContext& ctx = LogContext());

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level = 3,
                                                                const LogContext& ctx = LogContext());

    void stats();

private:

    std::unique_ptr<GribJumpBase> impl_;
};

}  // namespace gribjump
