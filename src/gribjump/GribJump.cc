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
    if(getenv("GRIBJUMP_CONFIG_FILE") != nullptr){
        config_ = Config(getenv("GRIBJUMP_CONFIG_FILE"));
    } 
    else {
        eckit::Log::debug<LibGribJump>() << "GRIBJUMP_CONFIG_FILE not set, using default config" << std::endl;
    }
    internal_ = std::unique_ptr<GribJumpBase>(GribJumpFactory::build(config_));
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

} // namespace GribJump