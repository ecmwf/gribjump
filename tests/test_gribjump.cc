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
#include <fstream>

#include "eckit/testing/Test.h"

#include "gribjump/GribInfo.h"
#include "gribjump/JumpHandle.h"

using namespace eckit::testing;

#include "data.cc"

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------


using Range = std::pair<size_t, size_t>;

void doTest(int i, JumpInfo gribInfo, JumpHandle &dataSource){
    EXPECT(gribInfo.ready());
    size_t numberOfDataPoints = gribInfo.getNumberOfDataPoints();
    double epsilon = simplePackedData[i].epsilon;

    std::cout << "Testing " << simplePackedData[i].gribFileName << std::endl;
    EXPECT(numberOfDataPoints == simplePackedData[i].expectedData.size());
    // Range of ranges, all at once. Note ranges must not overlap.

    std::vector<std::vector<Interval> > rangesVector;
    // Single ranges
    rangesVector.push_back(std::vector<Interval>{
        Range(12, 34),   // start and end in same word
    });
    rangesVector.push_back(std::vector<Interval>{
        Range(0, 63),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        Range(0, 64),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        Range(0, 65),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        Range(32, 100),  // start and end in adjacent words
    });
    rangesVector.push_back(std::vector<Interval>{
        Range(123, 456), // start and end in non-adjacent words
    });

    // Ranges of ranges:
    // densely backed ranges
    rangesVector.push_back(std::vector<Interval>{
        Range(0, 3), Range(30, 60), Range(60, 90), Range(90, 120)
    });
    // // slightly seperated ranges, also starting from non-zero. also big ranges
    rangesVector.push_back(std::vector<Interval>{
        Range(1, 30), Range(31, 60), Range(61, 90), Range(91, 120),
        Range(200, 400), Range(401, 402), Range(403, 600),
    });
    // not starting on first word. Hitting last index.
    rangesVector.push_back(std::vector<Interval>{
        Range(200, 220), Range(220, 240), Range(240, 260), Range(600, 684)
    });
    // ranges with entire words between them
    rangesVector.push_back(std::vector<Interval>{
        Range(5, 10), Range(130, 135), Range(400, 410)
    });

    // loop through the ranges
    for (const auto& ranges : rangesVector) {

        // get the expected result
        std::vector<std::vector<double>> expected;
        for (const auto& range : ranges) {
            std::vector<double> expectedRange;
            for (size_t index = std::get<0>(range); index < std::get<1>(range); index++) {
                expectedRange.push_back(simplePackedData[i].expectedData[index]);
            }
            expected.push_back(expectedRange);
        }

        std::vector<std::vector<double>> actual;
        std::vector<std::vector<std::bitset<64>>> mask;
        std::unique_ptr<ExtractionResult> output(gribInfo.extractRanges(dataSource, ranges));
        actual = output->values();
        mask = output->mask();

        EXPECT(actual.size() == expected.size());
        for (size_t ri = 0; ri < expected.size(); ri++) {
            EXPECT(actual[ri].size() == expected[ri].size());
            for (size_t index = 0; index < expected[ri].size(); index++) {
                if (expected[ri][index] == 9999) {
                    EXPECT(std::isnan(actual[ri][index]));
                    EXPECT(!mask[ri][index/64][index%64]);
                    continue;
                }
                double delta = std::abs(actual[ri][index] - expected[ri][index]);
                EXPECT(delta < epsilon);
                EXPECT(mask[ri][index/64][index%64]);
            }
        }
    }
    std::cout << std::endl;

}
CASE( "test_gribjump_extract" ) {
    setGribJumpData(); // Set the data used by the test cases

    // loop through the test cases, ensure metadata is extracted correctly
    for (int i=0; i < simplePackedData.size(); i++) {
        JumpHandle dataSource(simplePackedData[i].gribFileName);
        std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
        JumpInfo gribInfo = *infos.back();

        std::ostringstream out;
        gribInfo.print(out);
        std::string s = out.str();
        s.erase(s.size()-1); // remove newline

        EXPECT(s == simplePackedData[i].expectedString);
    }

}

CASE( "test_gribjump_query" ) {
    setGribJumpData(); // Set the data used by the test cases

    // loop through the test cases
    for (int i = 0; i < simplePackedData.size(); i++) {
        JumpHandle dataSource(simplePackedData[i].gribFileName);
        std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
        JumpInfo gribInfo = *infos.back();
        
        doTest(i, gribInfo, dataSource);
        
        for(auto info : infos) {
            delete info;
        }
    }
}

CASE( "test_gribjump_query_multimsg" ) {
    setGribJumpData(); // Set the data used by the test cases

    // concatenate each test file into one big file, and check that reading from that file
    // gives the same result as reading from the individual files

    std::ofstream f("combine.grib", std::ios::binary);
    for (int i = 0; i < simplePackedData.size(); i++) {
        std::ifstream testFileStream(simplePackedData[i].gribFileName, std::ios::binary);
        f << testFileStream.rdbuf();
        // add some junk between some of the files (as long as its not the string GRIB)
        if (i == 1 || i == 3)  f << "junky junk";
    }
    f.close();

    // Extract
    eckit::PathName fname("combine.grib");
    JumpHandle dataSource(fname);
    eckit::PathName binName = "temp";
    
    std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
    write_jumpinfos_to_file(infos, binName);

    // loop through the test cases
    for (int i = 0; i < simplePackedData.size(); i++) {
        JumpInfo gribInfo = JumpInfo::fromFile(binName, i);
        doTest(i, gribInfo, dataSource);
    }

    for(auto info : infos) {
        delete info;
    }

    // remove the temporary file
    binName.unlink();
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
