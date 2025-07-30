/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <chrono>
#include <fstream>
#include <sstream>
#include <thread>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/log/JSON.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/testing/Test.h"

#include "eckit/value/Value.h"
#include "gribjump/GribJump.h"
#include "gribjump/Metrics.h"
#include "gribjump/gribjump_config.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "../path_tools.cc"


using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
static std::string gridHash        = "33c7d6025995e1b4913811e77d38ec50";  // base file: extract_ranges.grib
static eckit::PathName metricsFile = "test_metrics";


CASE("Remote protocol: Forwarding with paths") {
    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
    };


    std::vector<std::vector<Interval>> allIntervals = {
        {std::make_pair(0, 5), std::make_pair(20, 30)},
        {std::make_pair(0, 5), std::make_pair(20, 30)},
    };

    std::cout << "STARTED HERE?" << std::endl;

    fdb5::FDB fdb;
    std::vector<std::string> filenames = {};
    for (size_t i = 0; i < requests.size(); i++) {
        std::string mars_str = "retrieve," + requests[i];
        std::string path_str = get_path_name_from_mars_req(mars_str, fdb);
        filenames.push_back(path_str);
    }

    std::string scheme = "file";

    std::vector<size_t> offsets = {0, 226};

    // std::string host = "localhost";
    std::string host = "dummy_host";

    // int port = 57777;
    int port = 1234;

    std::cout << "ARE HERE?" << std::endl;

    std::vector<PathExtractionRequest> exRequests;
    for (size_t i = 0; i < requests.size(); i++) {
        exRequests.push_back(
            PathExtractionRequest(filenames[i], scheme, offsets[i], host, port, allIntervals[i], gridHash));
    }

    std::cout << "AND NOW HERE?" << std::endl;

    GribJump gribjump;

    // Create context from json
    std::stringstream ss;
    eckit::JSON j{ss};
    j.startObject();
    j << "origin" << "test_extract_from_path";
    j << "description" << "test test test";
    j.endObject();

    LogContext ctx(ss.str());

    std::cout << "CREATED CONTEXT?" << std::endl;

    std::vector<std::unique_ptr<ExtractionResult>> output = gribjump.extract(exRequests, ctx).dumpVector();

    std::cout << "EXTRACTED?" << std::endl;

    EXPECT_EQUAL(output.size(), 2);
    for (size_t i = 0; i < output.size(); i++) {
        EXPECT_EQUAL(output[i]->nvalues(0), 5);
        EXPECT_EQUAL(output[i]->nvalues(1), 10);
    }
}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
