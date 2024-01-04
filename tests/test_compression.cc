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
#include "gribjump/GribInfo.h"
#include "gribjump/GribHandleData.h"
#include <fstream>
#include <filesystem>

using namespace eckit::testing;

namespace test {

#include "data.cc"

void print_result(const Interval& interval, const std::vector<std::bitset<64>>& mask, const Values& actual, const Values& expected)
{
    auto [start, end] = interval;
    auto size = end - start;

    std::cerr << "interval: " << "[" << start << "-" << end << "]" << std::endl;

    std::cerr << "mask (" << mask.size() << "): ";
    for (size_t i = 0; i < mask.size(); i++)
        std::cerr << mask[i] << ", ";
    std::cerr << std::endl;

    std::cerr << "actual: ";
    for (size_t i = 0; i < actual.size(); i++)
        std::cerr << actual[i] << ", ";
    std::cerr << std::endl;

    std::cerr << "expected: ";
    for (size_t i = 0; i < actual.size(); i++)
        std::cerr << expected[start + i] << ", ";
    std::cerr << std::endl;
}


void test_compression() {
    for (const auto& data : testData) {
        // print current working directory using STL

        // check if file exists
        if (!std::filesystem::exists(data.gribFileName)) {
            std::cerr << "File " << data.gribFileName << " does not exist" << std::endl;
            continue;
        }
        std::cerr << "Current working directory is " << std::filesystem::current_path() << '\n';
        std::cerr << "Testing " << data.gribFileName << std::endl;
        gribjump::JumpHandle dataSource(data.gribFileName);
        eckit::PathName binName = "temp";
        gribjump::JumpInfo gribInfo = dataSource.extractInfoFromFile(binName);

        EXPECT(gribInfo.ready());
        size_t numberOfDataPoints = gribInfo.getNumberOfDataPoints();
        double epsilon = data.epsilon;

        std::cerr << "Testing " << data.gribFileName << std::endl;
        EXPECT(numberOfDataPoints == data.expectedData.size());

        std::vector<Interval>  ranges = {
            std::make_pair(0, 30),
            std::make_pair(31, 60),
            std::make_pair(60, 66),
            std::make_pair(91, 120),
            std::make_pair(200, 400),
            std::make_pair(401, 402),
            std::make_pair(403, 600),
        };

        gribjump::ExtractionResult result = gribInfo.extractRanges(dataSource, ranges);
        auto actual_all = result.values();
        auto mask_all = result.mask();

        const auto expected = data.expectedData;

        for (size_t index = 0; index < ranges.size(); index++) {
            auto [start, end] = ranges[index];
            auto size = end - start;
            auto actual = actual_all[index];
            auto mask = mask_all[index];

            //print_result(ranges[index], mask, actual, expected);

            EXPECT(actual.size() == size);
            for (size_t i = 0; i < actual.size(); i++) {
                if (std::isnan(expected[start + i])) {
                    EXPECT(std::isnan(actual[i]));
                    // FIXME(maee): test mask
                    // EXPECT(!mask[i/64][i%64]);
                    continue;
                }
                double delta = std::abs(actual[i] - expected[start + i]);
                EXPECT(delta < epsilon);
                // FIXME(maee): test mask
                // EXPECT(mask[i/64][i%64]);
            }
        }
    }
}


CASE( "test_compression" ) {
    test_compression();
}


//-----------------------------------------------------------------------------


}  // namespace test


int main(int argc, char **argv)
{
    test::setGribJumpData(); // Set the data used by the test cases
    // print the current directoy
    return run_tests ( argc, argv );
}
