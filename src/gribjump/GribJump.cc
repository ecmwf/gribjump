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

#include "eckit/log/Timer.h"

#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/GribJumpBase.h"

namespace gribjump {

GribJump::GribJump() {
    impl_ = std::unique_ptr<GribJumpBase>(GribJumpFactory::build(LibGribJump::instance().config()));
}

GribJump::~GribJump() {
}

size_t GribJump::scan(const eckit::PathName& path) {
    eckit::Timer timer("Gribjump::scan API", eckit::Log::debug<LibGribJump>());
    size_t ret = impl_->scan(path);
    timer.report();
    return ret;
}

size_t GribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles) {
    eckit::Timer timer("Gribjump::scan API", eckit::Log::debug<LibGribJump>());
    size_t ret = impl_->scan(requests, byfiles);
    timer.report();
    return ret;
}

std::vector<std::vector<ExtractionResult>> GribJump::extract(std::vector<ExtractionRequest> requests) {
    eckit::Timer timer("Gribjump::extract API", eckit::Log::debug<LibGribJump>());
    auto out = impl_->extract(requests);
    timer.report();
    return out;
}

std::vector<ExtractionResult> GribJump::extract(const metkit::mars::MarsRequest request, const std::vector<Range> ranges){
    // eckit::Timer timer("Gribjump::extract API",eckit::Log::debug<LibGribJump>());
    auto out = impl_->extract(request, ranges);
    // timer.report();
    return out;
}

std::vector<ExtractionResult> GribJump::extract(const std::vector<eckit::URI> uris, const std::vector<Range> ranges){
    eckit::Timer timer("Gribjump::extract API", eckit::Log::debug<LibGribJump>());
    auto out = impl_->extract(uris, ranges);
    timer.report();
    return out;
}

std::vector<ExtractionResult*> GribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets, const std::vector<std::vector<Range>>& ranges) {
    eckit::Timer timer("Gribjump::extract API", eckit::Log::debug<LibGribJump>());
    auto out = impl_->extract(path, offsets, ranges);
    timer.report();
    return out;
}


std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const std::string& request) {
    eckit::Timer timer("GribJump::axes API",eckit::Log::debug<LibGribJump>());
    auto out = impl_->axes(request);
    timer.report();
    return out;
}

void GribJump::stats() {
    impl_->stats();
}

} // namespace gribjump
