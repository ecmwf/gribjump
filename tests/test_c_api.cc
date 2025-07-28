/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// @file   test_c_api.cc
/// @author Chris Bradley

#include <cmath>
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/gribjump_c.h"

using namespace eckit::testing;
// Wrapper around the C function calls that will throw an exception if the function fails
// i.e. if the return value is not GRIBJUMP_SUCCESS
namespace gribjump::test {
constexpr double nan = std::numeric_limits<double>::quiet_NaN();

void test_success(gribjump_error_t err) {
    // gribjump_error_values_t err = static_cast<gribjump_error_values_t>(e);
    if (err != GRIBJUMP_SUCCESS)
        throw TestException("C-API error: " + std::string(gribjump_error_string()), Here());
}

void EXPECT_STR_EQUAL(const char* a, const char* b) {
    if (strcmp(a, b) != 0)
        throw TestException("Expected: " + std::string(a) + " == " + std::string(b), Here());
}

// Recreate the extract test from the C++ API
CASE("Extract") {

    // ---------------------------------------------------

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

    // ---------------------------------------------------
    std::vector<std::string> requests = {
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
        "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc"};

    const size_t range_arr[]           = {0, 5, 20, 30};  // 2 ranges
    const size_t range_arr_size        = 4;
    std::vector<double> expectedValues = {
        {0, nan, nan, 3, 4, nan, 21, 22, 23, 24, 25, 26, 27, nan, nan},
    };

    std::vector<unsigned long long> expectedMask = {
        0b11001,      // Mask bit 1 and 2
        0b0011111110  // Mask bit 0, 8 and 9
    };
    size_t n_total_values = 5 + 10;

    // array of requests pointers
    gribjump_extraction_request_t* requests_c[3];

    for (size_t i = 0; i < requests.size(); i++) {
        test_success(
            gribjump_new_request(&requests_c[i], requests[i].c_str(), range_arr, range_arr_size, gridHash.c_str()));
    }

    gribjump_extraction_request_t* requests_from_paths_c[3];
    size_t offset = 0;

    for (size_t i = 0; i < requests.size(); i++) {
        test_success(gribjump_new_request_from_path(&requests_from_paths_c[i], requests[i].c_str(), "file", offset,
                                                    range_arr, range_arr_size, gridHash.c_str()));
    }


    gribjump_handle_t* handle{};
    test_success(gribjump_new_handle(&handle));

    // call extract
    gribjump_extractioniterator_t* iterator{};
    test_success(gribjump_extract(handle, requests_c, requests.size(), nullptr, &iterator));

    // check iterator
    gribjump_iterator_status_t status;
    gribjump_extraction_result_t* result{};
    size_t count = 0;
    while ((status = gribjump_extractioniterator_next(iterator, &result)) == GRIBJUMP_ITERATOR_SUCCESS) {

        // Extract the values. Allocate the array
        double* values = new double[n_total_values];
        test_success(gribjump_result_values(result, &values, n_total_values));

        for (size_t i = 0; i < n_total_values; i++) {
            if (std::isnan(expectedValues[i])) {
                EXPECT(std::isnan(values[i]));
                continue;
            }
            EXPECT_EQUAL(values[i], expectedValues[i]);
        }

        // check the mask/
        size_t nmasks            = 2;  // == sum_i {range_size_i / 64}, summed over all ranges
        unsigned long long* mask = new unsigned long long[nmasks];
        test_success(gribjump_result_mask(result, &mask, nmasks));
        for (size_t i = 0; i < nmasks; i++) {
            EXPECT_EQUAL(mask[i], expectedMask[i]);
        }

        // cleanup
        test_success(gribjump_delete_result(result));
        delete[] values;
        count++;
    }

    EXPECT_EQUAL(status, GRIBJUMP_ITERATOR_COMPLETE);
    EXPECT_EQUAL(count, requests.size());

    // Cleanup
    test_success(gribjump_extractioniterator_delete(iterator));
    for (size_t i = 0; i < requests.size(); i++) {
        test_success(gribjump_delete_request(requests_c[i]));
    }

    // ---------------------------------------
    // Same request but with extract_single

    gribjump_extractioniterator_t* iterator_single{};
    std::string requeststr =
        "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/"
        "3,stream=oper,time=1200,type=fc";
    test_success(gribjump_extract_single(handle, requeststr.c_str(), range_arr, range_arr_size, gridHash.c_str(),
                                         nullptr, &iterator_single));

    // check iterator
    count = 0;
    while ((status = gribjump_extractioniterator_next(iterator_single, &result)) == GRIBJUMP_ITERATOR_SUCCESS) {

        // Extract the values. Allocate the array
        double* values = new double[n_total_values];
        test_success(gribjump_result_values(result, &values, n_total_values));

        for (size_t i = 0; i < n_total_values; i++) {
            if (std::isnan(expectedValues[i])) {
                EXPECT(std::isnan(values[i]));
                continue;
            }
            EXPECT_EQUAL(values[i], expectedValues[i]);
        }

        // check the mask/
        size_t nmasks            = 2;  // == sum_i {range_size_i / 64}, summed over all ranges
        unsigned long long* mask = new unsigned long long[nmasks];
        test_success(gribjump_result_mask(result, &mask, nmasks));
        for (size_t i = 0; i < nmasks; i++) {
            EXPECT_EQUAL(mask[i], expectedMask[i]);
        }

        // cleanup
        test_success(gribjump_delete_result(result));
        delete[] values;
        count++;
    }

    EXPECT_EQUAL(status, GRIBJUMP_ITERATOR_COMPLETE);
    EXPECT_EQUAL(count, 3);

    // Cleanup
    test_success(gribjump_extractioniterator_delete(iterator_single));

    // ------------------------------------
    test_success(gribjump_delete_handle(handle));
}

CASE("Axes") {

    // --------------------------------------------------------------------------

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
          - path: ")XX" + tmpdir +
                                 R"XX("
    )XX");

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::FDB fdb;
    eckit::PathName path = "axes.grib";
    fdb.archive(*path.fileHandle());
    fdb.flush();

    std::map<std::string, std::unordered_set<std::string>> expectedValues = {
        {"class", {"rd"}},     {"date", {"20230508", "20230509"}},
        {"domain", {"g"}},     {"expver", {"xxxx"}},
        {"levtype", {"sfc"}},  {"levelist", {""}},
        {"param", {"151130"}}, {"step", {"3", "2", "1"}},
        {"stream", {"oper"}},  {"time", {"1200"}},
        {"type", {"fc"}},
    };
    // --------------------------------------------------------------------------

    gribjump_handle_t* handle{};
    test_success(gribjump_new_handle(&handle));

    gribjump_axes_t* axes{};
    test_success(gribjump_new_axes(handle, "class=rd,expver=xxxx", 3, nullptr, &axes));

    size_t nkeys;
    test_success(gribjump_axes_keys_size(axes, &nkeys));
    EXPECT_EQUAL(nkeys, expectedValues.size());

    const char** keys = new const char*[nkeys];
    test_success(gribjump_axes_keys(axes, keys, nkeys));
    EXPECT_EQUAL(nkeys, expectedValues.size());

    for (size_t i = 0; i < nkeys; i++) {
        const char* key = keys[i];
        size_t nvalues;
        test_success(gribjump_axes_values_size(axes, key, &nvalues));
        EXPECT_EQUAL(nvalues, expectedValues[key].size());

        const char** values = new const char*[nvalues];
        test_success(gribjump_axes_values(axes, key, values, nvalues));

        for (size_t j = 0; j < nvalues; j++) {
            EXPECT(expectedValues[key].find(values[j]) != expectedValues[key].end());
        }
    }

    // cleanup
    test_success(gribjump_delete_axes(axes));
    delete[] keys;
    test_success(gribjump_delete_handle(handle));
}

}  // namespace gribjump::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
