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

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"

#include "fdb5/api/FDB.h"

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"

/// @author Tiago Quintino
/// @author Christopher Bradley

/// Tool to execute the scanning of the FDB and building the GribJump info
/// either locally or remotely on the GribJump server

namespace gribjump::tool {

class Scan: public GribJumpTool {
    
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 1; }
  
  public:
    Scan(int argc, char **argv): GribJumpTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
        options_.push_back(new eckit::option::SimpleOption<bool>("files", "Scan entire files matching the request (default: true)"));
    }

};

void Scan::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <mars request file>" << std::endl
                       << "       " << tool << " --raw <mars request file>" << std::endl
                       << "       " << tool << " --files <mars request file>" << std::endl
                       ;

    GribJumpTool::usage(tool);
}

void Scan::execute(const eckit::option::CmdArgs &args) {

    bool raw = args.getBool("raw", false);
    bool files = args.getBool("files", false);

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

    GribJump gj;

    gj.scan(requests, files);
}

} // gribjump::tool

int main(int argc, char **argv) {
    gribjump::tool::Scan app(argc, argv);
    return app.start();
}

