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
#include "fdb5/api/helpers/FDBToolRequest.h"

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
    virtual int numberOfPositionalArguments() const { return -1; }
  
  public:
    Scan(int argc, char **argv): GribJumpTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("file", "Reads the mars requests from a file, rather than from the command line"));
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
        options_.push_back(new eckit::option::SimpleOption<bool>("byfiles", "Scan entire files matching the request (default: true)"));
    }

};

void Scan::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " class=od,stream=oper,expver=xxxx" << std::endl
                       << "       " << tool << " --file=<mars request file>" << std::endl
                       ;

    GribJumpTool::usage(tool);
}

void Scan::execute(const eckit::option::CmdArgs &args) {

    bool raw = args.getBool("raw", false);
    bool byfiles = args.getBool("byfiles", false);
    std::string file = args.getString("file", "");

    std::vector<metkit::mars::MarsRequest> requests;
    
    if (args.count() > 0 && !file.empty()) {
        throw eckit::UserError("Invalid arguments: Cannot specify both a file (--file) and a request (positional arguments)");
    }
    
    if (!file.empty()){
        // Build request(s) from <file>
        std::ifstream in(file);
        if (in.bad()) {
            throw eckit::ReadError(args(0));
        }

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
    } else {
        // Build request(s) from positional arguments
        std::vector<std::string> requests_in;
        for (size_t i = 0; i < args.count(); ++i) {
            requests_in.emplace_back(args(i));
        }

        for (const std::string& request_string : requests_in) {
            auto parsed = fdb5::FDBToolRequest::requestsFromString(request_string, {}, raw, "retrieve");
            for (auto r : parsed) {
                requests.push_back(r.request());
            }
        }
    }
    GribJump gj;
    size_t nfields = gj.scan(requests, byfiles);
    eckit::Log::info() << "Scanned " << nfields << " field(s)" << std::endl;
}

} // gribjump::tool

int main(int argc, char **argv) {
    gribjump::tool::Scan app(argc, argv);
    return app.start();
}

