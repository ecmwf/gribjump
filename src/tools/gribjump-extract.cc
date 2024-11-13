/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
/// @author Christopher Bradley

#include <fstream>

#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"

#include "gribjump/GribJump.h"
#include "gribjump/tools/ToolUtils.h"
#include "gribjump/tools/GribJumpTool.h"

namespace gribjump::tool {

class GribJumpExtract: public GribJumpTool{

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }

public:
    GribJumpExtract(int argc, char **argv): GribJumpTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("print", "Prints the results"));
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
    }

private:
        std::vector<eckit::option::Option*> options_;
};

void GribJumpExtract::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <request_file> <ranges_file>" << std::endl
                       << "       " << tool << " --raw <request_file> <ranges_file>" << std::endl;
                       ;
    GribJumpTool::usage(tool);
}

void GribJumpExtract::execute(const eckit::option::CmdArgs &args) {
    // Testing tool for extract / directJump functionality
    using MarsRequests = metkit::mars::MarsRequest;
    const bool raw = args.getBool("raw", false);
    const bool printout = args.getBool("print", true);

    // Build request(s) from input
    std::ifstream in(args(0).c_str());
    if (in.bad()) {
        throw eckit::ReadError(args(0));
    }

    std::vector<metkit::mars::MarsRequest> requests;
    metkit::mars::MarsParser parser(in);
    auto parsedRequests = parser.parse();
    if (raw) {
        for (auto r : parsedRequests)
            requests.push_back(r);
    } else {
        metkit::mars::MarsExpension expand(/* inherit */ false);
        requests = expand.expand(parsedRequests);
    }

    // handle ranges
    auto allRanges = parseRangesFile(args(1));

    // Require number of requests == number of ranges
    // Special case: if only one range, apply to all requests
    if (allRanges.size() == 1) {
        for (size_t i = 1; i < requests.size(); i++) {
            allRanges.push_back(allRanges[0]);
        }
    }
    ASSERT(requests.size() == allRanges.size());

    std::vector<ExtractionRequest> polyRequest;
    for (size_t i = 0; i < requests.size(); i++) {
        // Flatten and remove verb
        std::vector<MarsRequests> flattenedRequests = flattenRequest(requests[i]);
        for (auto& req : flattenedRequests) {
            std::string s = req.asString();
            // remove "retrieve," from the beginning, if it exists
            if (s.find("retrieve,") == 0) {
                s = s.substr(9);
            }
            ExtractionRequest exrequest(s, allRanges[i]);
            polyRequest.push_back(exrequest);
        }
    }

    // Grid hash
    // extract tool currently has no way to specify grid hash, so we explicitly ignore it.
    eckit::testing::SetEnv ignoreGrid{"GRIBJUMP_IGNORE_GRID", "1"};

    // Extract values
    GribJump gj;
    std::vector<std::vector<std::unique_ptr<ExtractionResult>>> output = gj.extract(polyRequest);

    // Print extracted values
    if (!printout) return;
    
    eckit::Log::info() << "Extracted values:" << std::endl;
    for (size_t i = 0; i < output.size(); i++) { // each request
        eckit::Log::info() << "Request " << i << ": " << requests[i] << std::endl;
        eckit::Log::info() << "  Number of fields: " << output[i].size() << std::endl;
        for (size_t j = 0; j < output[i].size(); j++) { // each field
            eckit::Log::info() << "  Field " << j << std::endl;
            auto values = output[i][j]->values(); 
            auto mask = output[i][j]->mask();

            for (size_t k = 0; k < values.size(); k++) { // each range
                eckit::Log::info() << "    Range " << k;
                eckit::Log::info() << " (" << std::get<0>(allRanges[i][k]) << "-" << std::get<1>(allRanges[i][k]) << "): ";
                for (size_t l = 0; l < values[k].size(); l++) { // each value
                    eckit::Log::info() << values[k][l] << ", ";
                }
                eckit::Log::info() << std::endl;
            }
        }
    }
}

} // namespace gribjump::tool

int main(int argc, char **argv) {
    gribjump::tool::GribJumpExtract app(argc, argv);
    return app.start();
}

