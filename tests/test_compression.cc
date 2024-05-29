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
#include <filesystem>

#include "eckit/testing/Test.h"

#include "gribjump/GribInfo.h"
#include "gribjump/JumpHandle.h"

using namespace eckit::testing;

#include "data.cc"

namespace gribjump
{
    namespace test
    {

        void print_result(const Interval &interval, const std::vector<std::bitset<64>> &mask, const Values &actual, const Values &expected)
        {
            auto [start, end] = interval;
            auto size = end - start;

            std::cerr << "interval: "
                      << "[" << start << "-" << end << "]" << std::endl;

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

        void test_compression()
        {
            // TODO(maee): Use expected string in these tests

            for (const auto &data : testData)
            {
                std::cerr << "Testing " << data.gribFileName << std::endl;
                JumpHandle dataSource(data.gribFileName);
                eckit::PathName binName = "temp";
                std::vector<JumpInfo *> infos = dataSource.extractInfoFromFile();

                JumpInfo gribInfo = *infos.back();
                EXPECT(gribInfo.ready());
                size_t numberOfDataPoints = gribInfo.getNumberOfDataPoints();

                if (numberOfDataPoints != data.expectedData.size())
                {
                    std::cerr << "numberOfDataPoints: " << numberOfDataPoints << std::endl;
                    std::cerr << "expectedData.size(): " << data.expectedData.size() << std::endl;
                    std::cerr << "numberOfDataPoints != data.expectedData.size()" << std::endl;
                    std::cerr << "Skipping test" << std::endl;
                    throw std::runtime_error("numberOfDataPoints != data.expectedData.size()");
                }
                EXPECT(numberOfDataPoints == data.expectedData.size());

                std::vector<Interval> all_intervals = {
                    std::make_pair(0, 30),
                    std::make_pair(31, 60),
                    std::make_pair(60, 66),
                    std::make_pair(91, 120),
                    std::make_pair(200, 400),
                    std::make_pair(401, 402),
                    std::make_pair(403, 600),
                };

                std::vector<Interval> intervals;
                std::copy_if(all_intervals.begin(), all_intervals.end(), std::back_inserter(intervals), [&](const auto &interval)
                             { return interval.second <= numberOfDataPoints && interval.first < interval.second; });

                std::unique_ptr<ExtractionResult> result(gribInfo.extractRanges(dataSource, intervals));
                auto actual_all = result->values();
                auto mask_all = result->mask();

                const auto expected = data.expectedData;

                for (size_t index = 0; index < intervals.size(); index++)
                {
                    // Compare mask if it exists
                    if (!mask_all.empty())
                    {
                        auto actual_mask = mask_all[index];
                        Bitmap expected_bitmap = generate_bitmap(expected, intervals[index]);
                        auto expected_mask = to_bitset(expected_bitmap);
                        // print_bitmap(expected_bitmap);
                        // print_mask(expected_mask);
                        // print_mask(actual_mask);
                        EXPECT(actual_mask == expected_mask);
                    }

                    // Compare values
                    auto [start, end] = intervals[index];
                    auto actual_values = actual_all[index];
                    EXPECT(actual_values.size() == end - start);
                    for (size_t i = 0; i < actual_values.size(); i++)
                    {
                        if (expected[start + i] == 9999)
                        {
                            EXPECT(std::isnan(actual_values[i]));
                            continue;
                        }

                        if (actual_values[i] != expected[start + i])
                        {
                            std::cerr << "actual: " << actual_values[i] << std::endl;
                            std::cerr << "expected: " << expected[start + i] << std::endl;
                            print_result(intervals[index], {}, actual_values, expected);
                            throw std::runtime_error("actual value does not match expected value");
                        }

                        EXPECT(actual_values[i] == expected[start + i]);
                    }
                }
            }
        }

        CASE("test_compression")
        {
            setGribJumpData(); // Set the data used by the test cases

            test_compression();
        }

        //-----------------------------------------------------------------------------

    } // namespace test
} // namespace gribjump

int main(int argc, char **argv)
{
    return run_tests(argc, argv);
}
