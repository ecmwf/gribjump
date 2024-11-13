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
#include <string>
#include <vector>
#include <memory>
#include <unordered_set>

#include "eckit/io/DataHandle.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionItem.h"
#include "gribjump/ExtractionData.h"
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

    size_t scan(const std::vector<eckit::PathName>& paths, const LogContext& ctx=LogContext("none"));
    size_t scan(std::vector<metkit::mars::MarsRequest> requests, bool byfiles = false, const LogContext& ctx=LogContext("none"));

    std::vector<std::vector<std::unique_ptr<ExtractionResult>>> extract(std::vector<ExtractionRequest>& requests, const LogContext& ctx=LogContext("none"));
    std::vector<std::unique_ptr<ExtractionItem>> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges, const LogContext& ctx=LogContext("none"));

    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level=3, const LogContext& ctx=LogContext("none"));

    void stats();

private:
    std::unique_ptr<GribJumpBase> impl_;
};

} // namespace gribjump
