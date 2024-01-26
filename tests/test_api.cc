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

#include "gribjump/GribJump.h"
#include "gribjump/FDBService.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"


using namespace eckit::testing;

namespace gribjump {
namespace test {

//----------------------------------------------------------------------------------------------------------------------
// Build expected values for test ranges
constexpr double MISSING = std::numeric_limits<double>::quiet_NaN();

const std::vector<double> testdata = {
    0, MISSING, MISSING, 3, 4, 5, MISSING, MISSING, MISSING, MISSING, 10, 11, 12, 13, 14,
    MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, 21, 22, 23, 24, 25, 26, 27, 
    MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, 36, 37, 38, 39, 40,
    41, 42, 43, 44, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING,
    MISSING, MISSING, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, MISSING, MISSING, MISSING,
    MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, MISSING, 78, 79, 
    80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, MISSING, MISSING, MISSING, MISSING, MISSING,
    MISSING, MISSING, MISSING, MISSING 
    };


CASE( "test_gribjump_api_extract" ) {

    // current directory, base is cwd
    std::string s = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(s.c_str());
    tmpdir.mkdir();

    // eckit::PathName tmpdir(s.c_str() + std::string("/tmp"));
    // tmpdir.mkdir();
    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        schema: schema
        spaces:
        - roots:
          - path: ")XX" + tmpdir + R"XX("
    )XX");
    std::cout << config_str << std::endl;

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "extract_ranges.grib";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    // Build test requests
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

    // using Interval = std::pair<size_t, size_t>;
    std::vector<std::vector<Interval>> allIntervals = {
        {
            // two distinct ranges
            std::make_pair(0, 5), 
            std::make_pair(20, 30)
        },
        {
            // full message
            std::make_pair(0, 100), 
        },
        {
            // two sets of adjacent ranges
            std::make_pair(0, 1),
            std::make_pair(1, 2),
            std::make_pair(3, 4),
            std::make_pair(4, 5),
        }
    };

    using PolyRequest = std::vector<ExtractionRequest>;
    PolyRequest polyRequest;
    for (size_t i = 0; i < requests.size(); i++) {
        polyRequest.push_back(ExtractionRequest(requests[i], allIntervals[i]));
    }

    // Extract values
    GribJump gj;
    std::vector<std::vector<ExtractionResult>> output = gj.extract(polyRequest);
    std::vector<std::vector<std::vector<std::vector<double>>>> expectedValues;
    for (size_t i = 0; i < requests.size(); i++) {
        std::vector<std::vector<std::vector<double>>> vi;
        // each request is 1 field in this example
        size_t Nfields = 1;
        for (size_t j = 0; j < Nfields; j++) {
            std::vector<std::vector<double>> vij;
            for (const auto& range : allIntervals[i]) {
                std::vector<double> vijk;
                for (size_t l = std::get<0>(range); l < std::get<1>(range); l++) {
                    vijk.push_back(testdata[l]);
                }
                vij.push_back(vijk);
            }
            vi.push_back(vij);
        }
        expectedValues.push_back(vi);
    }

    // Compare values to expected values
    for (size_t i = 0; i < expectedValues.size(); i++) { // each request
        std::cout << "request " << i << std::endl;
        for (size_t j = 0; j < expectedValues[i].size(); j++) { // each field
            std::cout << "field " << j << std::endl;
            auto values = output[i][j].values();
            auto mask = output[i][j].mask();
            for (size_t k = 0; k < expectedValues[i][j].size(); k++) { // each range
                std::cout << "range " << k << std::endl;
                for (size_t l = 0; l < expectedValues[i][j][k].size(); l++) { // each value
                    std::cout << "v=" << values[k][l];
                    std::cout << ", expected=" << expectedValues[i][j][k][l] << std::endl;
                    if (std::isnan(expectedValues[i][j][k][l])) {
                        EXPECT(std::isnan(values[k][l]));
                        EXPECT(!mask[k][l/64][l%64]);
                        continue;
                    }
                    EXPECT(values[k][l] == expectedValues[i][j][k][l]);
                    EXPECT(mask[k][l/64][l%64]);
                }
            }
        }
    }

    // // test 2: Same fields but in a single step=2/1/3 request. One set of ranges for all fields.
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
    polyRequest.clear();
    polyRequest.push_back(ExtractionRequest(requests[0], ranges));

    std::vector<std::vector<ExtractionResult>> output2 = gj.extract(polyRequest);
    EXPECT(output2.size() == 1);
    EXPECT(output2[0].size() == 3);

    expectedValues.clear();
    std::vector<std::vector<std::vector<double>>> vi;
    //  request is 3 fields in this example
    size_t Nfields = 3;
    for (size_t j = 0; j < Nfields; j++) {
        std::vector<std::vector<double>> vij;
        for (const auto& range : ranges) {
            std::vector<double> vijk;
            for (size_t l = std::get<0>(range); l < std::get<1>(range); l++) {
                vijk.push_back(testdata[l]);
            }
            vij.push_back(vijk);
        }
        vi.push_back(vij);
    }
    expectedValues.push_back(vi);

    for (size_t i = 0; i < expectedValues.size(); i++) { // each request
        std::cout << "request " << i << std::endl;
        for (size_t j = 0; j < expectedValues[i].size(); j++) { // each field
            std::cout << "field " << j << std::endl;
            auto values = output2[i][j].values();
            auto mask = output2[i][j].mask();
            for (size_t k = 0; k < expectedValues[i][j].size(); k++) { // each range
                std::cout << "range " << k << std::endl;
                for (size_t l = 0; l < expectedValues[i][j][k].size(); l++) { // each value
                    std::cout << "v=" << values[k][l];
                    std::cout << ", expected=" << expectedValues[i][j][k][l] << std::endl;
                    if (std::isnan(expectedValues[i][j][k][l])) {
                        EXPECT(std::isnan(values[k][l]));
                        EXPECT(!mask[k][l/64][l%64]);
                        continue;
                    }
                    EXPECT(values[k][l] == expectedValues[i][j][k][l]);
                    EXPECT(mask[k][l/64][l%64]);
                }
            }
        }
    }

    // test 3: use extract with uri
    // Use fdb list to get URIs
    std::vector<eckit::URI> uris;
    fdb5::FDBToolRequest fdbreq(requests[0]);
    auto listIter = fdb.list(fdbreq, false);
    fdb5::ListElement elem;
    while (listIter.next(elem)) {
        const fdb5::FieldLocation& loc = elem.location();
        uris.push_back(loc.fullUri());
        std::cout << "location: " << loc.fullUri() << std::endl;
    }
    std::vector<ExtractionResult> output3 = gj.extract(uris, ranges);
    EXPECT(output3.size() == 3);

    // Expect output3 to be the same as output2[0]
    for (size_t i = 0; i < output3.size(); i++) { // each field
        auto values = output3[i].values();
        auto mask = output3[i].mask();

        auto values2 = output2[0][i].values();
        auto mask2 = output2[0][i].mask();

        for (size_t k = 0; k < values.size(); k++) { // each range
            for (size_t l = 0; l < values[k].size(); l++) { // each value
                EXPECT(mask[k][l/64][l%64] == mask2[k][l/64][l%64]);
                if (std::isnan(values[k][l])) {
                    EXPECT(std::isnan(values2[k][l]));
                    continue;
                }
                EXPECT(values[k][l] == values2[k][l]);
            }
        }
    }

    // test 4: use extract with path and offsets
    // Note, this returns pointers to ExtractionResult, not ExtractionResult

    // Get the paths from the URI above. Offset from URI's fragment
    std::vector<eckit::PathName> paths;
    std::vector<eckit::Offset> offsets;
    for (const auto& uri : uris) {
        paths.push_back(uri.path());
        std::string fragment =  uri.fragment();
        offsets.push_back(std::stoll(fragment));
    }

    // Make sure all paths are the same
    for (size_t i = 1; i < paths.size(); i++) {
        EXPECT(paths[i] == paths[0]);
    }

    std::vector<std::vector<Range>> rangesRepeat;
    for (size_t i = 0; i < paths.size(); i++) {
        rangesRepeat.push_back(ranges);
    }
    

    std::vector<ExtractionResult*> output4 = gj.extract(paths[0], offsets, rangesRepeat);
    EXPECT(output4.size() == 3);

    // Expect output4 to be the same as output2[0]

    std::cout << "output 4 expected" << std::endl;

    for (size_t i = 0; i < output4.size(); i++) { // each field
        auto values = output4[i]->values();
        auto mask = output4[i]->mask();

        auto values2 = output2[0][i].values();
        auto mask2 = output2[0][i].mask();

        for (size_t k = 0; k < values.size(); k++) { // each range
            for (size_t l = 0; l < values[k].size(); l++) { // each value
                EXPECT(mask[k][l/64][l%64] == mask2[k][l/64][l%64]);
                std::cout << "v=" << values[k][l];
                std::cout << ", expected=" << values2[k][l] << std::endl;
                if (std::isnan(values[k][l])) {
                    EXPECT(std::isnan(values2[k][l]));
                    continue;
                }
                EXPECT(values[k][l] == values2[k][l]);
            }
        }
    }

    std::cout << "test_gribjump_api_extract got to the end" << std::endl;

}
//----------------------------------------------------------------------------------------------------------------------


}  // namespace test
}  // namespace gribjump

int main(int argc, char **argv)
{
    // print the current directoy
    return run_tests ( argc, argv );
}
