/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cmath>
#include <fstream>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"

#include "gribjump/GribJump.h"
#include "gribjump/FDBPlugin.h"
#include "gribjump/info/InfoCache.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/InfoExtractor.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"
using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------
static std::string gridHash = "33c7d6025995e1b4913811e77d38ec50"; // base file: extract_ranges.grib

//-----------------------------------------------------------------------------
// Note: the environment for this test is configured by an external script. See tests/remote/test_server.sh.in

// Fairly limited set of tests designed to add some coverage to the client-server comms.
// Just tests we have a successful round-trip.
CASE( "Remote protocol: extract" ) {
    
    // --- Extract
    std::vector<metkit::mars::MarsRequest> requests = {
        fdb5::FDBToolRequest::requestsFromString("class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc")[0].request(),
        fdb5::FDBToolRequest::requestsFromString("class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc")[0].request(),
    };
    
    std::vector<std::vector<Interval>> allIntervals = {
        {std::make_pair(0, 5),  std::make_pair(20, 30)},
        {std::make_pair(0, 5),  std::make_pair(20, 30)},
    };

    std::vector<ExtractionRequest> exRequests;
    for (size_t i = 0; i < requests.size(); i++) {
        exRequests.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
    }

    GribJump gribjump;
    std::vector<std::vector<ExtractionResult*>> output = gribjump.extract(exRequests);

    EXPECT_EQUAL(output.size(), 2);
    for (size_t i = 0; i < output.size(); i++) {
        EXPECT_EQUAL(output[i].size(), 1);
        for (size_t j = 0; j < output[i].size(); j++) {
            EXPECT_EQUAL(output[i][j]->nvalues(0), 5);
            EXPECT_EQUAL(output[i][j]->nvalues(1), 10);
        }
    }
}
CASE( "Remote protocol: axes" ) {

    GribJump gribjump;
    std::map<std::string, std::unordered_set<std::string>> axes = gribjump.axes("class=rd,expver=xxxx");

    EXPECT(axes.find("step") != axes.end());
    EXPECT_EQUAL(axes["step"].size(), 3);

}

}  // namespace test
}  // namespace gribjump

//-----------------------------------------------------------------------------

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
