/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "eccodes.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/testing/Test.h"
#include "gribjump/Config.h"
#include "gribjump/GribJump.h"

using namespace eckit::testing;

namespace gribjump::test {

CASE("GJ-85: CCSDS extremes agree with ecCodes for both decimal scales") {
    const eckit::PathName path = eckit::PathName("extremes-100.grib2").fullName();
    const auto closeFile       = [](FILE* file) { std::fclose(file); };
    std::unique_ptr<FILE, decltype(closeFile)> file(std::fopen(path.asString().c_str(), "rb"), closeFile);
    ASSERT(file);

    Config config;
    config.set("ignoreGridHash", true);
    config.set("cache.enabled", false);
    GribJump gribjump(config);

    const long expectedDecimalScales[] = {-9, -3};
    const long expectedBinaryScales[]  = {-39, -19};
    for (size_t field = 0; field < 2; ++field) {
        int error = CODES_SUCCESS;
        std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
            codes_handle_new_from_file(nullptr, file.get(), PRODUCT_GRIB, &error), &codes_handle_delete);
        ASSERT(error == CODES_SUCCESS);
        ASSERT(handle);

        // Guard the properties that made the original extremes fields fail.
        long decimalScale, binaryScale, bitsPerValue, edition, numberOfPoints;
        ASSERT(codes_get_long(handle.get(), "decimalScaleFactor", &decimalScale) == CODES_SUCCESS);
        ASSERT(codes_get_long(handle.get(), "binaryScaleFactor", &binaryScale) == CODES_SUCCESS);
        ASSERT(codes_get_long(handle.get(), "bitsPerValue", &bitsPerValue) == CODES_SUCCESS);
        ASSERT(codes_get_long(handle.get(), "edition", &edition) == CODES_SUCCESS);
        ASSERT(codes_get_long(handle.get(), "numberOfDataPoints", &numberOfPoints) == CODES_SUCCESS);
        EXPECT_EQUAL(decimalScale, expectedDecimalScales[field]);
        EXPECT_EQUAL(binaryScale, expectedBinaryScales[field]);
        EXPECT_EQUAL(bitsPerValue, 12);
        EXPECT_EQUAL(edition, 2);
        EXPECT_EQUAL(numberOfPoints, 100);
        char packing[32];
        size_t packingLength = sizeof(packing);
        ASSERT(codes_get_string(handle.get(), "packingType", packing, &packingLength) == CODES_SUCCESS);
        EXPECT_EQUAL(std::string(packing), "grid_ccsds");

        off_t offset;
        ASSERT(codes_get_message_offset(handle.get(), &offset) == CODES_SUCCESS);
        size_t size;
        ASSERT(codes_get_size(handle.get(), "values", &size) == CODES_SUCCESS);
        ASSERT(size == 100);
        std::vector<double> reference(size);
        ASSERT(codes_get_double_array(handle.get(), "values", reference.data(), &size) == CODES_SUCCESS);
        EXPECT(reference.front() > 280 && reference.front() < 290);

        // Check all 100 points, then isolated/disjoint ranges including the tail.
        for (const std::vector<Interval>& ranges :
             {std::vector<Interval>{{0, 100}}, std::vector<Interval>{{0, 1}, {31, 36}, {63, 66}, {99, 100}}}) {
            PathExtractionRequests requests{PathExtractionRequest(path.asString(), "file", offset, "", 0, ranges)};
            auto results = gribjump.extract(requests).dumpVector();
            ASSERT(results.size() == 1);
            const auto& values = results[0]->values();
            const auto& masks  = results[0]->mask();
            ASSERT(values.size() == ranges.size());
            ASSERT(masks.size() == ranges.size());
            for (size_t range = 0; range < ranges.size(); ++range) {
                const auto begin = ranges[range].first;
                const auto end   = ranges[range].second;
                ASSERT(values[range].size() == end - begin);
                ASSERT(masks[range].size() == (end - begin + 63) / 64);
                for (size_t i = 0; i < values[range].size(); ++i) {
                    EXPECT_EQUAL(values[range][i], reference[begin + i]);
                    EXPECT(masks[range][i / 64][i % 64]);
                }
            }
        }
    }
    int error = CODES_SUCCESS;
    std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> extra(
        codes_handle_new_from_file(nullptr, file.get(), PRODUCT_GRIB, &error), &codes_handle_delete);
    EXPECT(error == CODES_SUCCESS || error == CODES_END_OF_FILE);
    EXPECT(!extra);
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
