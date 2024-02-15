/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>
#include <memory>

#include "eckit/io/FileHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/utils/StringTools.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"

#include "fdb5/api/FDB.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"

#include "gribjump/JumpHandle.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJump.h"
#include "gribjump/tools/ToolUtils.h"


// TODO(Chris) This probably doesnt need to be an FDBTool

namespace gribjump
{
    

class GJExtractTool : public fdb5::FDBTool {
// class GJExtractTool : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }
  public:
    GJExtractTool(int argc, char **argv): fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("print", "Prints the results"));
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
        options_.push_back(
                    new eckit::option::SimpleOption<bool>("statistics",
                                                          "Report timing statistics"));
    }
};

void GJExtractTool::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <mars request file> <ranges file>" << std::endl
                       << "       " << tool << " --raw <mars request file> <ranges file>" << std::endl;

    fdb5::FDBTool::usage(tool);
}

void GJExtractTool::execute(const eckit::option::CmdArgs &args) {
    // Testing tool for extract / directJump functionality

    bool raw = args.getBool("raw", false);
    bool printout = args.getBool("print", true);

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

