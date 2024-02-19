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
#include <memory>

#include "eckit/io/FileHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/utils/StringTools.h"
#include "eckit/runtime/Tool.h"
#include "eckit/option/SimpleOption.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"

#include "gribjump/JumpHandle.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJump.h"
#include "gribjump/tools/ToolUtils.h"


// TODO(Chris) Make a gribjump::Tool class that inherits from eckit::Tool

namespace gribjump {

static void usage(const std::string &tool) {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <request_file> <ranges_file>" << std::endl
                       << "       " << tool << " --raw <request_file> <ranges_file>" << std::endl;

    
}
class GJExtractTool : public eckit::Tool{

    virtual int numberOfPositionalArguments() const { return 2; }

    virtual void run(const eckit::option::CmdArgs &args);

    virtual void run() override {
        eckit::option::CmdArgs args(usage, options_, numberOfPositionalArguments());
        run(args);
    }

public:
    GJExtractTool(int argc, char **argv): eckit::Tool(argc, argv, "GRIBJUMP_HOME") {
        options_.push_back(new eckit::option::SimpleOption<bool>("print", "Prints the results"));
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
    }

private:
        std::vector<eckit::option::Option*> options_;
};

void GJExtractTool::run(const eckit::option::CmdArgs &args) {
    // Testing tool for extract / directJump functionality

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
        ExtractionRequest exrequest(requests[i], allRanges[i]);
        polyRequest.push_back(exrequest);
    }

    // Extract values
    GribJump gj;
    std::vector<std::vector<ExtractionResult>> output = gj.extract(polyRequest);

    // Print extracted values
    if (!printout) return;
    
    eckit::Log::info() << "Extracted values:" << std::endl;
    for (size_t i = 0; i < output.size(); i++) { // each request
        eckit::Log::info() << "Request " << i << ": " << requests[i] << std::endl;
        eckit::Log::info() << "  Number of fields: " << output[i].size() << std::endl;
        for (size_t j = 0; j < output[i].size(); j++) { // each field
            eckit::Log::info() << "  Field " << j << std::endl;
            auto values = output[i][j].values(); // NOTE: Copies data?
            auto mask = output[i][j].mask(); // NOTE: Copies data?

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

} // namespace gribjump

int main(int argc, char **argv) {
    gribjump::GJExtractTool app(argc, argv);
    return app.start();
}

