/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <vector>
#include <string>
#include <cmath>
#include <bitset>

#include "eckit/testing/Test.h"

#include "gribjump/Interval.h"
#include "gribjump/Bitmap.h"

#include "gribjump/tools/EccodesExtract.h"

namespace gribjump {
namespace test {

class InputData {
public:
    std::string gribFileName;
    std::vector<double> expectedData;
    std::string expectedString;
    // std::vector<size_t> expectedCcsdsOffsets; // todo for all ccsds files.
};

constexpr double MISSING_VAL = std::numeric_limits<double>::quiet_NaN();
std::vector<InputData> testData;
std::vector<InputData> simplePackedData;


Bitmap generate_bitmap(const std::vector<double>& data, Interval interval) {
    Bitmap bitmap;
    auto [start, end] = interval;
    for (size_t i = start; i < end; i++) {
        if (std::isnan(data[i]) || data[i] == 9999) {
            bitmap.push_back(0);
        } else {
            bitmap.push_back(1);
        }
    }
    return bitmap;
}


void print_mask(const std::vector<std::bitset<64>>& mask) {
    std::cerr << "mask (" << mask.size() << "): " << std::endl;
    for (size_t count = 0, i = 0; i < mask.size(); i++)
        std::cerr << count++ << ": " << mask[i] << std::endl;
}


void print_bitmap(const Bitmap& bitmap) {
    size_t split = 64;
    std::cerr << "bitmap: " << std::endl;
    size_t count = 0;
    for (size_t i = 0; i < bitmap.size(); i++) {
        if (i % split == 0) {
            std::cerr << count++ << ": ";
        }
        std::cerr << (int) bitmap[i];
        if ((i + 1) % split == 0) {
            std::cerr << std::endl;
        }
    }
    std::cerr << std::endl;
}


void add_simple_no_bitmask() {
    // Grib with no bitmask
    std::vector<double> ecValues = eccodesExtract("no_mask.grib");
    EXPECT(ecValues.size() == 684);
    testData.push_back(
        InputData{
            .gribFileName = "no_mask.grib",
            .expectedData = ecValues,
            .expectedString = "JumpInfo[version=4,editionNumber=1,binaryScaleFactor=-10,decimalScaleFactor=0,bitsPerValue=16,ccsdsFlags=0,ccsdsBlockSize=0,ccsdsRsi=0,referenceValue=227.887,offsetBeforeData=103,offsetAfterData=1472,numberOfDataPoints=684,numberOfValues=684,offsetBeforeBitmap=0,sphericalHarmonics=0,binaryMultiplier=0.000976562,decimalMultiplier=1,totalLength=1476,msgStartOffset=0,md5GridSection=33c7d6025995e1b4913811e77d38ec50,packingType=grid_simple]",
        });

    simplePackedData.push_back(testData.back());
}

void add_surface_level_data() {
    // Surface level grib with bitmask
    std::vector<double> ecValues = eccodesExtract("sl_mask.grib");
    EXPECT(ecValues.size() == 684);
    // ecValues[0] += 1;
    testData.push_back(
        InputData{
            .gribFileName = "sl_mask.grib",
            .expectedData = ecValues,
            .expectedString = "JumpInfo[version=4,editionNumber=1,binaryScaleFactor=-10,decimalScaleFactor=0,bitsPerValue=16,ccsdsFlags=0,ccsdsBlockSize=0,ccsdsRsi=0,referenceValue=2.32006,offsetBeforeData=195,offsetAfterData=1074,numberOfDataPoints=684,numberOfValues=439,offsetBeforeBitmap=98,sphericalHarmonics=0,binaryMultiplier=0.000976562,decimalMultiplier=1,totalLength=1078,msgStartOffset=0,md5GridSection=33c7d6025995e1b4913811e77d38ec50,packingType=grid_simple]",
        });
    simplePackedData.push_back(testData.back());
}


void add_synthetic_data_with_bitmap() {
    // Bitmap
    //10011000 11100001 11100000 11111000 00011111 10000000 11111110 00000001
    //11111110 00000000 11111111 10000000 00011111 11111000 00000000 11111111
    //11100000 00000001 11111111 11100000 00000000 11111111 11111000 00000000
    //00011111 11111111 10000000 00000000 11111111 11111110 00000000 00000001
    //11111111 11111110 00000000 00000000 11111111 11111111 10000000 00000000
    //00011111 11111111 11111000 00000000 00000000 11111111 11111111 11100000
    //00000000 00000001 11111111 11111111 11100000 00000000 00000000 11111111
    //11111111 11111000 00000000 00000000 00011111 11111111 11111111 10000000
    //00000000 00000000 11111111 11111111 11111110 00000000 00000000 00000001
    //11111111 11111111 11111110 00000000 00000000 00000000 11111111 11111111
    //11111111 10000000 00000000 00000000 00011111 111

    std::vector<double> ecValues = eccodesExtract("synth11.grib");
    EXPECT(ecValues.size() == 684);

    testData.push_back(InputData{
       .gribFileName = "synth11.grib",
       .expectedData = ecValues,
       .expectedString = "JumpInfo[version=4,editionNumber=1,binaryScaleFactor=-1,decimalScaleFactor=0,bitsPerValue=11,ccsdsFlags=0,ccsdsBlockSize=0,ccsdsRsi=0,referenceValue=0,offsetBeforeData=195,offsetAfterData=656,numberOfDataPoints=684,numberOfValues=334,offsetBeforeBitmap=98,sphericalHarmonics=0,binaryMultiplier=0.5,decimalMultiplier=1,totalLength=660,msgStartOffset=0,md5GridSection=33c7d6025995e1b4913811e77d38ec50,packingType=grid_simple]",
    });
    simplePackedData.push_back(testData.back());

    testData.push_back(InputData{
       .gribFileName = "synth11_ccsds_bitmap.grib2",
       .expectedData = ecValues,
       .expectedString = "",
    });

    testData.push_back(InputData{
        .gribFileName = "synth12.grib",
        .expectedData = ecValues,
        .expectedString = "JumpInfo[version=4,editionNumber=1,binaryScaleFactor=-2,decimalScaleFactor=0,bitsPerValue=12,ccsdsFlags=0,ccsdsBlockSize=0,ccsdsRsi=0,referenceValue=0,offsetBeforeData=195,offsetAfterData=696,numberOfDataPoints=684,numberOfValues=334,offsetBeforeBitmap=98,sphericalHarmonics=0,binaryMultiplier=0.25,decimalMultiplier=1,totalLength=700,msgStartOffset=0,md5GridSection=33c7d6025995e1b4913811e77d38ec50,packingType=grid_simple]",
    });
    simplePackedData.push_back(testData.back());

    // Grib with single value
    std::vector<double> ecValuesConst = eccodesExtract("const.grib");
    EXPECT(ecValuesConst.size() == 1);
    testData.push_back(InputData{
        .gribFileName = "const.grib",
        .expectedData = std::vector<double>(ecValues.size(), ecValuesConst[0]),
        .expectedString = "JumpInfo[version=4,editionNumber=1,binaryScaleFactor=-10,decimalScaleFactor=0,bitsPerValue=0,ccsdsFlags=0,ccsdsBlockSize=0,ccsdsRsi=0,referenceValue=1.23457,offsetBeforeData=111,offsetAfterData=112,numberOfDataPoints=684,numberOfValues=1,offsetBeforeBitmap=98,sphericalHarmonics=0,binaryMultiplier=0.000976562,decimalMultiplier=1,totalLength=116,msgStartOffset=0,md5GridSection=33c7d6025995e1b4913811e77d38ec50,packingType=grid_simple]",
    });
    simplePackedData.push_back(testData.back());
}


void add_synthetic_data_without_bitmap() {

    std::vector<double> ecValues = eccodesExtract("synth11_ccsds_no_bitmap.grib2");
    EXPECT(ecValues.size() == 334);

    testData.push_back(InputData{
       .gribFileName = "synth11_ccsds_no_bitmap.grib2",
       .expectedData = ecValues,
       // TODO(maee): Fix expected string
       .expectedString = "",
    });
}

void setGribJumpData() {
// Set the data used by the test cases
    add_simple_no_bitmask();
    add_surface_level_data();
    add_synthetic_data_with_bitmap();
    add_synthetic_data_without_bitmap();
}


} // namespace gribjump
}  // namespace test