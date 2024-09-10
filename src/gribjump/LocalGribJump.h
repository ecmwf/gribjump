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
#include "gribjump/ExtractionItem.h"
#include "gribjump/info/InfoAggregator.h"

namespace gribjump {

typedef metkit::mars::MarsRequest MarsRequest;

class LocalGribJump : public GribJumpBase {

public:

    explicit LocalGribJump(const Config& config);
    ~LocalGribJump();

    /// @brief Scans the full grib file, looking for GRIB messages and populates cache
    /// @param path full path to grib file
    size_t scan(const eckit::PathName& path) override;

    size_t scan(const std::vector<MarsRequest> requests, bool byfiles) override;

    // new API!
    ResultsMap extract(const std::vector<MarsRequest>& requests, const std::vector<std::vector<Range>>& ranges, bool flatten);

    // old API
    std::vector<std::unique_ptr<ExtractionItem>> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) override;
    std::vector<std::vector<ExtractionResult*>> extract(std::vector<ExtractionRequest>) override;
    
    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request) override;

private:
};

} // namespace gribjump
