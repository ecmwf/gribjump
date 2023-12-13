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

#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/GribJumpFactory.h"


namespace gribjump {

GribJump::GribJump(){
    std::string name = "localgribjump";
    if (getenv("GRIBJUMP_REMOTE") != NULL) {
        name = "remotegribjump";
    }
    internal_ = std::unique_ptr<GribJumpBase>(GribJumpFactory::build(name));
}

std::vector<std::vector<ExtractionResult>> GribJump::extract(std::vector<ExtractionRequest> requests) {
    return internal_->extract(requests);
}

std::vector<ExtractionResult> GribJump::extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges){
    return internal_->extract(request, ranges);
}

std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const std::string& request) {
    return internal_->axes(request);
}

} // namespace gribjump
