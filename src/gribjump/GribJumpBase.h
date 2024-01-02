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

#include "gribjump/ExtractionData.h"
#include "eckit/memory/NonCopyable.h"
#include <unordered_set>

namespace gribjump
{

class GribJumpBase : public eckit::NonCopyable {
public:
    GribJumpBase() {}
    virtual ~GribJumpBase() {}
    virtual std::vector<std::vector<ExtractionResult>> extract(std::vector<ExtractionRequest>) = 0;
    virtual std::vector<ExtractionResult> extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges) = 0;
    virtual std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) = 0;
};

} // namespace gribjump
