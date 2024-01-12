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

using Range = std::pair<size_t, size_t>; // change this with ccsds
const size_t expectedNumberOfValues = 6599680; // O1280

std::vector<double> getComparisonValues(eckit::PathName comparisonFileName, size_t expectedNumberOfValues) {
    std::ifstream comparisonFile(comparisonFileName);
    std::string line;
    std::vector<double> comparisonValues;
    int count = 0;
    while (std::getline(comparisonFile, line)) {
        std::stringstream lineStream(line);
        std::string value;
        // expect exactly 5 values per line
        for (int i = 0; i < 5; i++) {
            std::getline(lineStream, value, ',');
            comparisonValues.push_back(std::stod(value));
            count++;
        }
    }
    EXPECT(comparisonValues.size() == expectedNumberOfValues);
    return comparisonValues;
}

void test(eckit::PathName gribname, eckit::PathName comparename){
// a comma separated list of values, 5 values per line
    // const eckit::PathName comparisonFileName="2t_O1280.values";
    // const size_t expectedNumberOfValues = 6599680;
    const std::vector<double> comparisonValues = getComparisonValues(comparename, expectedNumberOfValues);

    // check the values
    
    eckit::PathName binName = "temp";
    // const eckit::PathName gribFileName = "2t_O1280.grib";
    JumpHandle dataSource(gribname);
    std::cout << "Made JumpHandle" << std::endl;
    JumpInfo gribInfo = dataSource.extractInfoFromFile(binName);  
    std::cout << "Made JumpInfo" << std::endl;
    EXPECT(gribInfo.getNumberOfDataPoints() == expectedNumberOfValues);

    // Big ranges
    std::vector<Range> ranges = {
        Range(0, 10000),  
        Range(100000, 110000), 
        Range(200000, 210000),
        Range(300000, 310000), Range(400000, 410000), Range(500000, 510000),
        // and read towards the end
        Range(6500000, 6510000),Range(6520000, 6530000),Range(6550000, 6560000),
        Range(6560000, expectedNumberOfValues),
    };

    ExtractionResult output = gribInfo.extractRanges(dataSource, ranges);
    auto values = output.values();
    
    EXPECT(values.size() == ranges.size());

    // compare the values
    // small epsilon from grib_dump valueq
    const double epsilon = 1e-2;
    for (size_t i = 0; i < values.size(); i++) {
        size_t range0 = std::get<0>(ranges[i]);
        size_t range1 = std::get<1>(ranges[i]);
        EXPECT(values[i].size() == range1 - range0);
        for (size_t j = 0; j < values[i].size(); j++) {
            if (std::isnan(values[i][j])) {
                EXPECT(comparisonValues[range0 + j] == 9999); // comparison file has 9999 for missing values
                continue;
            }
            //std::cout << "values[" << i << "][" << j << "] = " << values[i][j] << std::endl;
            //std::cout << "comparisonValues[" << range0 + j << "] = " << comparisonValues[range0 + j] << std::endl;
            double delta = std::abs(values[i][j] - comparisonValues[range0 + j]);
            EXPECT(delta < epsilon);
        }
    }

    // Now, read 50 single points
    std::vector<Range> singlePoints;
    for (size_t i = 0; i < 50; i++) {
        size_t r0 = i*10000;
        singlePoints.push_back(Range(r0, r0+1));
    }
    // Plus the last one
    singlePoints.push_back(Range(expectedNumberOfValues-1, expectedNumberOfValues));

    output = gribInfo.extractRanges(dataSource, singlePoints);
    values = output.values();

    EXPECT(values.size() == singlePoints.size());

    // compare the values
    for (size_t i = 0; i < values.size(); i++) {
        size_t range0 = std::get<0>(singlePoints[i]);
        size_t range1 = std::get<1>(singlePoints[i]);
        EXPECT(values[i].size() == range1 - range0);
        for (size_t j = 0; j < values[i].size(); j++) {
            if (std::isnan(values[i][j])) {
                EXPECT(comparisonValues[range0 + j] == 9999); // comparison file has 9999 for missing values
                continue;
            }
            double delta = std::abs(values[i][j] - comparisonValues[range0 + j]);
            EXPECT(delta < epsilon);
        }
    }

}

CASE( "test_2t_O1280" ) {
    // this is grid_simple, grib 1, no bitmask.
    test("2t_O1280.grib", "2t_O1280.values");
    
}
CASE( "test_ceil_O1280" ) {
    // this is grid_ccsds, grib 2, with bitmask.
    test("ceil_O1280.grib", "ceil_O1280.values");    
}

}  // namespace test
}  // namespace gribjump

int main(int argc, char **argv)
{
    // print the current directoy
    return run_tests ( argc, argv );
}
