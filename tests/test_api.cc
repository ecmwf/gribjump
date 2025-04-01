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

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "gribjump/GribJump.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/tools/EccodesExtract.h"

#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"


using namespace eckit::testing;

namespace gribjump {
namespace test {


//----------------------------------------------------------------------------------------------------------------------
// Build expected values for test ranges
constexpr double MISSING = std::numeric_limits<double>::quiet_NaN();
///@todo: continue refactoring these functions to be simpler.
void compareValues_old(const std::vector<std::vector<std::vector<std::vector<double>>>>& expectedValues,
                       const std::vector<std::vector<std::unique_ptr<ExtractionResult>>>& output, size_t nvalues) {
    EXPECT_EQUAL(expectedValues.size(), output.size());
    size_t count = 0;
    for (size_t i = 0; i < expectedValues.size(); i++) {  // each mars request
        EXPECT_EQUAL(expectedValues[i].size(), output[i].size());
        for (size_t j = 0; j < expectedValues[i].size(); j++) {  // each field
            auto values = output[i][j]->values();
            auto mask   = output[i][j]->mask();
            EXPECT_EQUAL(expectedValues[i][j].size(), values.size());
            for (size_t k = 0; k < expectedValues[i][j].size(); k++) {  // each range
                EXPECT_EQUAL(expectedValues[i][j][k].size(), values[k].size());
                for (size_t l = 0; l < expectedValues[i][j][k].size(); l++) {  // each value
                    count++;
                    if (expectedValues[i][j][k][l] == 9999) {
                        EXPECT(std::isnan(values[k][l]));
                        EXPECT(!mask[k][l / 64][l % 64]);
                        continue;
                    }

                    EXPECT_EQUAL(values[k][l], expectedValues[i][j][k][l]);
                    EXPECT(mask[k][l / 64][l % 64]);
                }
            }
        }
    }

    // Ensure we did in fact check all the values
    EXPECT_EQUAL(count, nvalues);
}

void compareValues(const std::vector<std::vector<std::vector<std::vector<double>>>>& expectedValues,
                   const std::vector<std::unique_ptr<ExtractionResult>>& output, size_t nvalues) {
    EXPECT_EQUAL(expectedValues.size(), output.size());
    size_t count = 0;
    for (size_t i = 0; i < expectedValues.size(); i++) {  // each mars request
        EXPECT_EQUAL(expectedValues[i].size(), 1);        // 1.
        auto values = output[i]->values();
        auto mask   = output[i]->mask();
        EXPECT_EQUAL(expectedValues[i][0].size(), values.size());
        for (size_t k = 0; k < expectedValues[i][0].size(); k++) {  // each range
            EXPECT_EQUAL(expectedValues[i][0][k].size(), values[k].size());
            for (size_t l = 0; l < expectedValues[i][0][k].size(); l++) {  // each value
                count++;
                if (expectedValues[i][0][k][l] == 9999) {
                    EXPECT(std::isnan(values[k][l]));
                    EXPECT(!mask[k][l / 64][l % 64]);
                    continue;
                }

                EXPECT_EQUAL(values[k][l], expectedValues[i][0][k][l]);
                EXPECT(mask[k][l / 64][l % 64]);
            }
        }
    }

    // Ensure we did in fact check all the values
    EXPECT_EQUAL(count, nvalues);
}
//----------------------------------------------------------------------------------------------------------------------

CASE("test_gribjump_api_extract") {

    // --------------------------------------------------------------------------------------------

    // Prep: Write test data to FDB

    std::string cwd = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(cwd.c_str());
    tmpdir.mkdir();

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        schema: schema
        spaces:
        - roots:
          - path: ")XX" + tmpdir +
                                 R"XX("
    )XX");

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "extract_ranges.grib";
    std::string gridHash = "33c7d6025995e1b4913811e77d38ec50";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    // --------------------------------------------------------------------------------------------

    // Test 1: Extract 3 fields. Each field has a different set of ranges

    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc"};

    std::vector<std::vector<Interval>> allIntervals = {
        {
            // two distinct ranges
            std::make_pair(0, 5), std::make_pair(20, 30)  // 15 values
        },
        {
            // full message
            std::make_pair(0, 100),  // 100 values
        },
        {
            // two sets of adjacent ranges
            std::make_pair(0, 1), std::make_pair(1, 2), std::make_pair(3, 4), std::make_pair(4, 5),  // 4 values
        }};

    using PolyRequest = std::vector<ExtractionRequest>;
    PolyRequest polyRequest1;
    for (size_t i = 0; i < requests.size(); i++) {
        polyRequest1.push_back(ExtractionRequest(requests[i], allIntervals[i], gridHash));
    }

    // Extract values
    GribJump gj;
    std::vector<std::unique_ptr<ExtractionResult>> output1 = gj.extract(polyRequest1).dumpVector();
    EXPECT_EQUAL(output1.size(), 3);

    // Eccodes expected values
    std::vector<std::vector<std::vector<std::vector<double>>>> expectedValues;
    for (auto req : polyRequest1) {
        metkit::mars::MarsRequest marsreq = fdb5::FDBToolRequest::requestsFromString(req.requestString())[0].request();
        expectedValues.push_back(eccodesExtract(marsreq, req.ranges()));
    }
    compareValues(expectedValues, output1, 15 + 100 + 4);

    // --------------------------------------------------------------------------------------------

    // Test 2: Extract same fields as Test 1, but in a single step=2/1/3 request. One set of ranges for all fields.
    std::istringstream ss(
        "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/"
        "3,stream=oper,time=1200,type=fc\n");
    metkit::mars::MarsParser parser(ss);
    auto parsedRequests = parser.parse();
    metkit::mars::MarsExpension expand(/* inherit */ false);
    metkit::mars::MarsRequest req = expand.expand(parsedRequests)[0];

    std::vector<Interval> ranges = {std::make_pair(0, 5), std::make_pair(20, 30)};  // 15 values
    std::vector<std::unique_ptr<ExtractionResult>> output2 = gj.extract(req, ranges, gridHash).dumpVector();
    EXPECT_EQUAL(output2.size(), 3);

    // Eccodes expected values
    std::vector<std::vector<std::vector<std::vector<double>>>> expectedValues2;
    for (auto req : polyRequest1) {
        metkit::mars::MarsRequest marsreq = fdb5::FDBToolRequest::requestsFromString(req.requestString())[0].request();
        expectedValues2.push_back(eccodesExtract(marsreq, ranges));
    }
    compareValues(expectedValues2, output2, 15 * 3);

    // --------------------------------------------------------------------------------------------
    ranges = {std::make_pair(0, 5), std::make_pair(20, 30)};  // 15 values

    // Test 1.b: Extract but with an md5 hash
    std::vector<ExtractionRequest> vec = {ExtractionRequest(requests[0], ranges)};
    EXPECT_THROWS_AS(gj.extract(vec), eckit::SeriousBug);  // missing hash
    vec = {ExtractionRequest(requests[0], ranges, "wronghash")};
    EXPECT_THROWS_AS(gj.extract(vec), eckit::SeriousBug);  // incorrect hash

    // correct hash
    vec                                                     = {ExtractionRequest(requests[0], ranges, gridHash)};
    std::vector<std::unique_ptr<ExtractionResult>> output2c = gj.extract(vec).dumpVector();
    EXPECT_EQUAL(output2c.size(), 1);
    EXPECT_EQUAL(output2c[0]->total_values(), 15);

    // // --------------------------------------------------------------------------------------------

    // Test 3: Extract function using path and offsets, which skips engine and related tasks/checks.

    // std::vector<eckit::URI> uris;
    fdb5::FDBToolRequest fdbreq = fdb5::FDBToolRequest::requestsFromString(
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/3,stream=oper,time=1200,type=fc")
        [0];
    auto listIter = fdb.list(fdbreq, false);
    fdb5::ListElement elem;

    // Get the paths from the URI above. Offset from URI's fragment
    std::set<eckit::PathName> paths;
    std::vector<eckit::Offset> offsets;

    std::vector<std::vector<Range>> rangesRepeat;
    while (listIter.next(elem)) {
        const fdb5::FieldLocation& loc = elem.location();
        eckit::URI uri                 = loc.fullUri();
        paths.insert(uri.path());
        std::string fragment = uri.fragment();
        offsets.push_back(std::stoll(fragment));
        rangesRepeat.push_back(ranges);  // Extract same range from all fields
    }
    EXPECT_EQUAL(paths.size(), 1);    // Only one file
    EXPECT_EQUAL(offsets.size(), 3);  // 3 fields

    std::vector<std::unique_ptr<ExtractionResult>> output3 =
        gj.extract(*paths.begin(), offsets, rangesRepeat).dumpVector();
    EXPECT_EQUAL(output3.size(), 3);

    // Expect output to be the same as output2[0]
    expectedValues.clear();
    expectedValues.push_back(eccodesExtract(fdbreq.request(), ranges));

    std::vector<std::vector<std::unique_ptr<ExtractionResult>>> output3v;
    output3v.push_back(std::move(output3));           // i.e. == {output3}
    compareValues_old(expectedValues, output3v, 45);  // 15 * 3 fields

    std::cout << "test_gribjump_api_extract got to the end" << std::endl;
}

// --------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump

int main(int argc, char** argv) {
    // print the current directoy
    return run_tests(argc, argv);
}
