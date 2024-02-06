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
#include <memory>

#include "eckit/io/FileHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/utils/StringTools.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"
#include "metkit/codes/GribHandle.h"

#include "fdb5/api/FDB.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJump.h"
#include "gribjump/FDBService.h"
#include "tools/ToolUtils.h"

/// @author Christopher Bradley

// Tool to perform an extraction request on a given request, and compare
// the results with the equivalent request using eccodes.
// Note: This tool requires a locally configured FDB config.

namespace gribjump
{
    
class GJCompareEccodes : public fdb5::FDBTool {
    
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }
  
  public:
    GJCompareEccodes(int argc, char **argv): fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
    }

};

void GJCompareEccodes::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <mars request file> <ranges file>" << std::endl
                       << "       " << tool << " --raw <mars request file> <ranges file>" << std::endl;
    fdb5::FDBTool::usage(tool);
}


void GJCompareEccodes::execute(const eckit::option::CmdArgs &args) {
    std::cout << "GJCompareEccodes::execute" << std::endl;
    bool raw = args.getBool("raw", false);

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
        bool inherit = false;
        metkit::mars::MarsExpension expand(inherit);
        requests = expand.expand(parsedRequests);
    }

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

    GribJump gj;
    std::vector<std::vector<ExtractionResult>> results = gj.extract(polyRequest);

    // Now we have the gribjump results, we need to compare with eccodes
    // For each request we need to:
    // 1. find the message using path + offset from fdb.
    // 2. Extract the data (using eccodes)

    size_t countAllValues = 0;

    for (int i = 0; i < requests.size(); i++) {
        std::map< eckit::PathName, eckit::OffsetList > map = FDBService::instance().filesOffsets({requests[i]});
        ASSERT(map.size() == results[i].size());
        size_t filecount = 0;
        for (const auto& entry : map) {
            const eckit::PathName path = entry.first;
            const eckit::OffsetList offsets = entry.second;

            std::unique_ptr<eckit::DataHandle> dh(path.fileHandle());
            dh->openForRead();

            for (int j = 0; j < offsets.size(); j++) {

                const eckit::Offset offset = offsets[j];
                const metkit::grib::GribHandle handle(*dh, offset);

                size_t count;
                std::unique_ptr<double[]> data(handle.getDataValues(count));

                std::vector<std::vector<double>> ecvalues;
                for (const auto& range : allRanges[i]) {
                    std::vector<double> rangeValues;
                    for (size_t k = range.first; k < range.second; k++) {
                        rangeValues.push_back(data[k]);
                    }
                    ecvalues.push_back(rangeValues);
                }

                // compare the values
                const auto& gjvalues = results[i][j+filecount].values();
                ASSERT(ecvalues.size() == gjvalues.size());

                for (int k = 0; k < ecvalues.size(); k++) {
                    ASSERT(ecvalues[k].size() == gjvalues[k].size());
                    for (int l = 0; l < ecvalues[k].size(); l++) {
                        ASSERT(ecvalues[k][l] == gjvalues[k][l]);
                        countAllValues++;
                    }
                }
            }
            filecount++;

        }
    }

    eckit::Log::info() << "Compared " << countAllValues << " values. All match." << std::endl;
    
}
} // namespace gribjump

int main(int argc, char **argv) {
    gribjump::GJCompareEccodes app(argc, argv);
    return app.start();
}

