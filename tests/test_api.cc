/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


#include <cmath>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/filesystem/LocalPathName.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "gribjump/GribJump.h"
#include "gribjump/tools/EccodesExtract.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"


using namespace eckit::testing;

namespace gribjump {
namespace test {

//----------------------------------------------------------------------------------------------------------------------
// Build expected values for test ranges
constexpr double MISSING = std::numeric_limits<double>::quiet_NaN();

void compareValues(const std::vector<std::vector<std::vector<std::vector<double>>>>& expectedValues, const std::vector<std::vector<ExtractionResult*>>& output) {
    EXPECT(expectedValues.size() == output.size());
    for (size_t i = 0; i < expectedValues.size(); i++) { // each mars request
        EXPECT_EQUAL(expectedValues[i].size(), output[i].size());
        for (size_t j = 0; j < expectedValues[i].size(); j++) { // each field
            auto values = output[i][j]->values();
            auto mask = output[i][j]->mask();
            EXPECT_EQUAL(expectedValues[i][j].size(), values.size());
            for (size_t k = 0; k < expectedValues[i][j].size(); k++) { // each range
                EXPECT_EQUAL(expectedValues[i][j][k].size(), values[k].size());
                for (size_t l = 0; l < expectedValues[i][j][k].size(); l++) { // each value

                    if (expectedValues[i][j][k][l] == 9999) {
                        EXPECT(std::isnan(values[k][l]));
                        EXPECT(!mask[k][l/64][l%64]);
                        continue;
                    }
                
                    EXPECT_EQUAL(values[k][l], expectedValues[i][j][k][l]);
                    EXPECT(mask[k][l/64][l%64]);

                }
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

CASE( "test_gribjump_api_extract" ) {
    
    // --------------------------------------------------------------------------------------------
    
    // Prep: Write test data to FDB

    std::string s = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(s.c_str());
    tmpdir.mkdir();

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        schema: schema
        spaces:
        - roots:
          - path: ")XX" + tmpdir + R"XX("
    )XX");

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "extract_ranges.grib";
    std::string gridHash = "33c7d6025995e1b4913811e77d38ec50";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    // --------------------------------------------------------------------------------------------

    // Test 1: Extract 3 fields. Each field has a different set of ranges

    std::vector<metkit::mars::MarsRequest> requests;
    {
        std::istringstream s(
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc\n"
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc\n"
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc\n"
            );
        metkit::mars::MarsParser parser(s);
        auto parsedRequests = parser.parse();
        metkit::mars::MarsExpension expand(/* inherit */ false);
        requests = expand.expand(parsedRequests);
    }


    std::vector<std::vector<Interval>> allIntervals = {
        {
            // two distinct ranges
            std::make_pair(0, 5),  std::make_pair(20, 30)
        },
        {
            // full message
            std::make_pair(0, 100), 
        },
        {
            // two sets of adjacent ranges
            std::make_pair(0, 1), std::make_pair(1, 2),
            std::make_pair(3, 4), std::make_pair(4, 5),
        }
    };

    using PolyRequest = std::vector<ExtractionRequest>;
    PolyRequest polyRequest1;
    for (size_t i = 0; i < requests.size(); i++) {
        polyRequest1.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
    }

    // Extract values
    GribJump gj;
    std::vector<std::vector<ExtractionResult*>> output1 = gj.extract(polyRequest1);

    // Eccodes expected values
    std::vector<std::vector<std::vector<std::vector<double>>>> expectedValues;
    for (auto req : polyRequest1) {
        expectedValues.push_back(eccodesExtract(req.request(), req.ranges()));
    }
    compareValues(expectedValues, output1);

    // --------------------------------------------------------------------------------------------

    // Test 2: Extract same fields as Test 1, but in a single step=2/1/3 request. One set of ranges for all fields.
    
    {
        std::istringstream s(
            "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/3,stream=oper,time=1200,type=fc\n"
        );
        metkit::mars::MarsParser parser(s);
        auto parsedRequests = parser.parse();
        metkit::mars::MarsExpension expand(/* inherit */ false);
        requests = expand.expand(parsedRequests);
    }
    
    std::vector<Interval> ranges = allIntervals[0];
    PolyRequest polyRequest2;
    polyRequest2.push_back(ExtractionRequest(requests[0], ranges, gridHash));

    std::vector<std::vector<ExtractionResult*>> output2 = gj.extract(polyRequest2);
    EXPECT(output2.size() == 1);
    EXPECT(output2[0].size() == 3);

    expectedValues.clear();
    expectedValues.push_back(eccodesExtract(requests[0], ranges));
    compareValues(expectedValues, output2);

    // --------------------------------------------------------------------------------------------

    // Test 2.b: Extract but with an md5 hash
    EXPECT_THROWS_AS(gj.extract({ExtractionRequest(requests[0], ranges)}), eckit::SeriousBug); // missing hash
    EXPECT_THROWS_AS(gj.extract({ExtractionRequest(requests[0], ranges, "wronghash")}), eckit::SeriousBug); // incorrect hash
    
    // correct hash
    std::vector<std::vector<ExtractionResult*>> output2c = gj.extract({ExtractionRequest(requests[0], ranges, gridHash)});
    EXPECT_EQUAL(output2c[0][0]->total_values(), 15);

    // --------------------------------------------------------------------------------------------

    // Test 3: Extract function using path and offsets, which skips engine and related tasks/checks.
    
    std::vector<eckit::URI> uris;
    fdb5::FDBToolRequest fdbreq(requests[0]);
    auto listIter = fdb.list(fdbreq, false);
    fdb5::ListElement elem;
    while (listIter.next(elem)) {
        const fdb5::FieldLocation& loc = elem.location();
        uris.push_back(loc.fullUri());
    }

    // Get the paths from the URI above. Offset from URI's fragment
    std::vector<eckit::PathName> paths;
    std::vector<eckit::Offset> offsets;
    for (const auto& uri : uris) {
        paths.push_back(uri.path());
        std::string fragment =  uri.fragment();
        offsets.push_back(std::stoll(fragment));
    }

    for (size_t i = 1; i < paths.size(); i++) {
        EXPECT(paths[i] == paths[0]);
    }

    std::vector<std::vector<Range>> rangesRepeat;
    for (size_t i = 0; i < paths.size(); i++) {
        rangesRepeat.push_back(ranges);
    }
    
    std::vector<std::unique_ptr<ExtractionItem>> outputItems3 = gj.extract(paths[0], offsets, rangesRepeat);
    EXPECT(outputItems3.size() == 3);

    /// @todo temp: convert to extractionResult
    std::vector<ExtractionResult*> output3;
    for (size_t i = 0; i < outputItems3.size(); i++) {
        output3.push_back(new ExtractionResult(outputItems3[i]->values(), outputItems3[i]->mask()));
    }

    // Expect output to be the same as output2[0]
    compareValues(expectedValues, {output3});

    std::cout << "test_gribjump_api_extract got to the end" << std::endl;
}

// --------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump

int main(int argc, char **argv)
{
    // print the current directoy
    return run_tests ( argc, argv );
}
