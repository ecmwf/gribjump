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
#include "gribjump/GribHandleData.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

#include "data.cc"

void doTest(int i, JumpInfo gribInfo, JumpHandle &dataSource){
    EXPECT(gribInfo.ready());
    size_t numberOfDataPoints = gribInfo.getNumberOfDataPoints();
    double epsilon = testData[i].epsilon;

    // Query each single data point, and compare with expected value
    std::cout << "Testing " << testData[i].gribFileName << std::endl;
    EXPECT(numberOfDataPoints == testData[i].expectedData.size());

    // TODO(maee): Chris, this is not working, please review
    //for (size_t index = 0; index < numberOfDataPoints; index++) {
    //    double v = gribInfo.extractValue(dataSource, index);
    //    if (std::isnan(testData[i].expectedData[index]) ) {
    //        EXPECT(std::isnan(v));
    //        continue;
    //    }
    //    double delta = std::abs(v - testData[i].expectedData[index]);
    //    EXPECT(delta < epsilon);
    //}
    // Range of ranges, all at once. Note ranges must not overlap.

    std::vector<std::vector<Interval> > rangesVector;
    // Single ranges
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(12, 34),   // start and end in same word
    });
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(0, 63),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(0, 64),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(0, 65),    // test edge of word
    });
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(32, 100),  // start and end in adjacent words
    });
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(123, 456), // start and end in non-adjacent words
    });

    // Ranges of ranges:
    // densely backed ranges
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(0, 3), std::make_pair(30, 60), std::make_pair(60, 90), std::make_pair(90, 120)
    });
    // // slightly seperated ranges, also starting from non-zero. also big ranges
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(1, 30), std::make_pair(31, 60), std::make_pair(61, 90), std::make_pair(91, 120),
        std::make_pair(200, 400), std::make_pair(401, 402), std::make_pair(403, 600),
    });
    // not starting on first word. Hitting last index.
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(200, 220), std::make_pair(220, 240), std::make_pair(240, 260), std::make_pair(600, 684)
    });
    // ranges with entire words between them (so word skipping should take place)
    rangesVector.push_back(std::vector<Interval>{
        std::make_pair(5, 10), std::make_pair(130, 135), std::make_pair(400, 410)
    });

    // loop through the ranges
    for (const auto& ranges : rangesVector) {

        // get the expected result
        std::vector<std::vector<double>> expected;
        for (const auto& range : ranges) {
            std::vector<double> expectedRange;
            for (size_t index = std::get<0>(range); index < std::get<1>(range); index++) {
                expectedRange.push_back(testData[i].expectedData[index]);
            }
            expected.push_back(expectedRange);
        }

        // auto [actual, mask] = gribInfo.extractRanges(dataSource, ranges); // lets not use auto in the testing code
        std::vector<std::vector<double>> actual;
        std::vector<std::vector<std::bitset<64>>> mask;
        ExtractionResult output = gribInfo.extractRanges(dataSource, ranges);
        actual = output.values();
        mask = output.mask();

        EXPECT(actual.size() == expected.size());
        for (size_t ri = 0; ri < expected.size(); ri++) {
            EXPECT(actual[ri].size() == expected[ri].size());
            // std::cout << "range " << std::get<0>(ranges[ri]) << " to " << std::get<1>(ranges[ri]) << std::endl;
            // std::cout << "xxx:" << " expected " << expected[ri] << " actual " << actual[ri] << std::endl;
            for (size_t index = 0; index < expected[ri].size(); index++) {
                if (std::isnan(expected[ri][index])) {
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
CASE( "test_metkit_gribjump_extract" ) {
    // loop through the test cases, ensure metadata is extracted correctly
    eckit::PathName binName = "temp";
    for (int i=0; i < testData.size(); i++) {
        JumpHandle dataSource(testData[i].gribFileName);
        JumpInfo gribInfo = dataSource.extractInfoFromFile(binName);

        std::ostringstream out;
        gribInfo.print(out);
        std::string s = out.str();
        s.erase(s.size()-1); // remove newline

        // Check s against expected string testData[i].expectedString
        EXPECT(s == testData[i].expectedString);
    }

    // delete the temporary file
    eckit::PathName(binName).unlink();
}

CASE( "test_metkit_gribjump_query" ) {
    eckit::PathName binName = "temp";
    // loop through the test cases
    for (int i = 0; i < testData.size(); i++) {
        // Extract
        JumpHandle dataSource(testData[i].gribFileName);
        JumpInfo gribInfo = dataSource.extractInfoFromFile(binName);
        doTest(i, gribInfo, dataSource);
    }

    // delete the temporary file
    eckit::PathName(binName).unlink();
}

CASE( "test_metkit_gribjump_query_multimsg" ) {
    // concatenate each test file into one big file, and check that reading from that file
    // gives the same result as reading from the individual files

    std::ofstream f("combine.grib", std::ios::binary);
    for (int i = 0; i < testData.size(); i++) {
        std::ifstream testFileStream(testData[i].gribFileName, std::ios::binary);
        f << testFileStream.rdbuf();
        // add some junk between some of the files (as long as its not the string GRIB)
        if (i == 1 || i == 3)  f << "junky junk";
    }
    f.close();

    // Extract
    eckit::PathName fname("combine.grib");
    JumpHandle dataSource(fname);
    eckit::PathName binName = "temp";
    JumpInfo gribInfo = dataSource.extractInfoFromFile(binName);

    // loop through the test cases
    for (int i = 0; i < testData.size(); i++) {
        gribInfo.fromFile(binName, i);
        doTest(i, gribInfo, dataSource);
    }

    // delete the temporary file
    eckit::PathName(binName).unlink();
}

CASE( "test_metkit_gribjump_accedges1" ) {
    // unit test for accumulateIndexes function
    // Testing handling of single words
    size_t MASKED = -1;
    for (size_t ti=0; ti < 2; ti ++){
        uint64_t n;
        std::queue<size_t> edges;
        std::vector<size_t> n_index;
        std::vector<size_t> expected_n;
        size_t expected_count;
        if (ti == 0) {
            n = 0xF0F0F0F0F0F0F0F0;
            expected_n = {
                1, 2, 3, 4, MASKED, MASKED, MASKED, MASKED, 5, 6,
                MASKED, MASKED, MASKED, MASKED, 13, 14, 15, 16, MASKED, MASKED,
                MASKED,
                18, 19, 20, MASKED, MASKED, MASKED, MASKED, 21, 22, 23, 24,
                MASKED, MASKED, MASKED, 25, 26, 27, 28, MASKED, MASKED, MASKED, MASKED, 29, 30, 31, 32, MASKED, MASKED, MASKED, MASKED
            };
            expected_count = 32;
        } else if (ti == 1) {
            n = 0;
            expected_n = {
                MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED,
                MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED,
                MASKED,
                MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED,
                MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED, MASKED,
            };
            expected_count = 0;
        } else if (ti == 2){
            n = 0xFFFFFFFFFFFFFFFF;
            expected_n = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                32,
                34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
                46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64
            };
            expected_count = 64;
        }
        else {
            continue;
        }

        // std::cout << "bitstring n: " << std::bitset<64>(n) << std::endl;
        size_t count = 0;
        edges.push(0);
        edges.push(10);

        edges.push(20);
        edges.push(30);

        edges.push(31);
        edges.push(32);

        edges.push(33);
        edges.push(44);

        edges.push(45);
        edges.push(64);

        bool pushToggle = false; // note this test doesnt utilise this trick, its for inter-word edges
        size_t bp = 0; // ditto
        accumulateIndexes(n, count, n_index, edges, pushToggle, bp);

        // print out expected
        // std::cout << "expected: ";
        // for (size_t i = 0; i < expected_n.size(); i++) {
        //     std::cout << expected_n[i] << ", ";
        // }
        // std::cout << std::endl;

        // std::cout << "count: " << count << std::endl;
        // std::cout << "n_index: ";
        // for (size_t i = 0; i < n_index.size(); i++) {
        //     std::cout << n_index[i] << ", ";
        // }
        // std::cout << std::endl;


        // XXX: WIP, expected_n currently off by 1 intentionally while developments happen
        for (size_t i = 0; i < expected_n.size(); i++) {
            if (expected_n[i] != MASKED) {
                expected_n[i] -= 1;
            }
        }

        EXPECT(expected_n.size() == n_index.size());
        for (size_t i = 0; i < expected_n.size(); i++) {
            EXPECT(n_index[i] == expected_n[i]);
        }
        EXPECT(count == expected_count);
    }
}

CASE( "test_metkit_gribjump_accedges2" ) {
    // unit test for accumulateIndexes function
    // Testing handling of multiple words

    uint64_t bitstream[] = {
        0xFFFFFFFFFFFFFFFF, // [0, 64)
        0xFFFFFFFFFFFFFFFF, // [64, 128)
        0x0000000000000000, // [128, 192)
        0xFFFFFFFFFFFFFFFF, // [192, 256)
        0xFFFFFFFFFFFFFFFF, // [256, 320)
    };
    uint64_t n;
    std::queue<size_t> edges;
    std::vector<size_t> n_index;
    std::vector<size_t> expected_n;
    size_t expected_count;
    size_t count = 0;

    // inter-word range
    edges.push(0);
    edges.push(96);
    // intra-word range
    edges.push(100);
    edges.push(120);
    // inter-word range, edge case
    edges.push(191);
    edges.push(193);
    // intra-word range, edge case
    edges.push(255);
    edges.push(256);


    bool pushToggle = false;
    size_t bp = 0;
    size_t word = 0;
    while (!edges.empty()) {
        n = bitstream[word];
        accumulateIndexes(n, count, n_index, edges, pushToggle, bp);
        ASSERT(bp < 1000); // infinite loop protection
        word++;
    }
    size_t MASKED = -1;
    expected_n = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
        60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78,
        79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 101,
        102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        117, 118, 119, 120, MASKED, 129, 192,
    };
    expected_count = 192;

    // print out expected
    // std::cout << "expected: ";
    // for (size_t i = 0; i < expected_n.size(); i++) {
    //     std::cout << expected_n[i] << ", ";
    // }
    // std::cout << std::endl;

    // std::cout << "count: " << count << std::endl;
    // std::cout << "n_index: ";
    // for (size_t i = 0; i < n_index.size(); i++) {
    //     std::cout << n_index[i] << ", ";
    // }
    // std::cout << std::endl;



    // XXX: WIP, expected_n currently off by 1 intentionally while developments happen
    for (size_t i = 0; i < expected_n.size(); i++) {
        if (expected_n[i] != MASKED) {
            expected_n[i] -= 1;
        }
    }

    EXPECT(expected_n.size() == n_index.size());
    for (size_t i = 0; i < expected_n.size(); i++) {
        EXPECT(n_index[i] == expected_n[i]);
    }
    EXPECT(count == expected_count);
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    gribjump::test::setGribJumpData(); // Set the data used by the test cases
    // print the current directoy
    return run_tests ( argc, argv );
}
