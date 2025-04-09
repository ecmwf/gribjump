/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
/// @author Christopher Bradley

#include <fstream>

#include "eckit/filesystem/PathName.h"
#include "eckit/utils/StringTools.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/tools/ToolUtils.h"
#include "metkit/mars/MarsExpension.h"

namespace gribjump {

std::vector<std::vector<Range>> parseRangesFile(eckit::PathName fname) {

    // plain text file with the following format:
    //      10-20, 30-40
    //      10-20, 60-70, 80-90
    //      ...etc
    // Each line contains a list of ranges, separated by commas.
    // One line per request.

    std::vector<std::vector<Range>> allRanges;

    std::ifstream in(fname);
    if (in.bad()) {
        throw eckit::ReadError(fname);
    }

    std::string line;
    while (std::getline(in, line)) {
        std::vector<Range> ranges;
        std::vector<std::string> rangeStrings = eckit::StringTools::split(",", line);
        for (const auto& rangeString : rangeStrings) {
            std::vector<std::string> range = eckit::StringTools::split("-", rangeString);
            ASSERT(range.size() == 2);
            ranges.push_back(std::make_pair(std::stoi(range[0]), std::stoi(range[1])));
        }
        allRanges.push_back(ranges);
    }

    return allRanges;
}

class CollectFlattenedRequests : public metkit::mars::FlattenCallback {
public:

    CollectFlattenedRequests(std::vector<metkit::mars::MarsRequest>& flattenedRequests) :
        flattenedRequests_(flattenedRequests) {}

    virtual void operator()(const metkit::mars::MarsRequest& req) { flattenedRequests_.push_back(req); }

    std::vector<metkit::mars::MarsRequest>& flattenedRequests_;
};

std::vector<metkit::mars::MarsRequest> flattenRequest(const metkit::mars::MarsRequest& request) {

    metkit::mars::MarsExpension expansion(false);
    metkit::mars::DummyContext ctx;
    std::vector<metkit::mars::MarsRequest> flattenedRequests;

    CollectFlattenedRequests cb(flattenedRequests);
    expansion.flatten(ctx, request, cb);

    if (LibGribJump::instance().debug()) {
        LOG_DEBUG_LIB(LibGribJump) << "Base request: " << request << std::endl;
        for (const auto& req : flattenedRequests) {
            LOG_DEBUG_LIB(LibGribJump) << "  Flattened request: " << req << std::endl;
        }
    }

    return flattenedRequests;
}

}  // namespace gribjump
